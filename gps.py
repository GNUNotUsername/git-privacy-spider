"""
A web spider for analysing GPS data accidentally committed to github

USAGE:  sudo python gps.py <-s path> | <[-j threads] count>
"""
# TODO test the bajeesus out of multithreading
# TODO test the bajeesus out of VPN integration
# TODO fix duplicates in marked places


from csv                import reader,  writer
from json               import loads
from os                 import mkdir,   path,   sep
from random             import choice,  randint
from requests           import get
from shutil             import rmtree
from string             import ascii_letters as al
from sys                import argv
from threading          import Thread,  Semaphore

from pathlib            import Path
from subprocess         import CalledProcessError,  check_output,   DEVNULL,    run
from sqlalchemy         import Column,              create_engine,  delete,     ForeignKey, Integer, insert, MetaData, select, String, Table
from sqlalchemy.orm     import scoped_session,      sessionmaker
from sqlalchemy_utils   import create_database,     database_exists


# Argc & Argv
COUNT       = -1
FLAG        = 1
GOOD_ARGCS  = range(2, 5)
PATH_IND    = 2
SERIAL_FLAG = "-s"
THREAD_FLAG = "-j"
THREADS     = 2

# CSV
CSV_HEADER  = ["Repo", "Path"]

# Databases
BASE_NAME   = "name"
DB_ADDR     = "mariadb://root@localhost:3306/gitprivacyspider"
GOOD_TABLES = {"repo", "user", "repo_queue", "hits", "user_queue"}
HITS        = "hits"
ID          = "id"
IDEXT       = "." + ID
LINK_LEN    = 255
NAME_IND    = 1
PATH        = "path"
QUEUE_EXT   = "_queue"
REPO_ENT    = "repo"
TOP         = 1
USER_ENT    = "user"

# Error Messages
BAD_TABLES  = "Tables do not match required schema; please fix in MariaDB"

# Exif
EXIFTOOL    = "exiftool"
GPS_ATTR    = "GPS"

# Exit codes
BAD_ARGV    = 1
BAD_TABS    = 2

# Github API
API_HEAD    = "https://api.github.com/"
CLONE_TMP   = "git clone https://www.github.com/{0}.git {1}"
CONTRIBS    = (API_HEAD + "repos/{0}/contributors")
RAND_MAX    = 255   # I have no idea what this number should be
RAND_REPO   = (API_HEAD + "repositories?since={0}")
REPNAME     = "full_name"
UNAME       = "login"
URL_ATTR    = "html_url"
USER_REPOS  = (API_HEAD + "users/{0}/repos")

# IO
UTF8        = "utf-8"
WRITE       = "w"

# Logging
BAD_CLONE   = "Repo {0} could not be cloned"

# Pathing
ALL         = "*"
GIT_EXTRA   = ".git"
GH_EXTRA    = ".github"
HIDE        = "."
RAND_LEN    = 20

# URLs
MIN_DELIMS  = 2
NO_START    = 3
URL_DELIM   = "/"

# VPN
CONNECTED   = 0
FIND_IP     = ["nordvpn", "status"]
IP_TAG      = "IP: "
MOVED_IP    = "IP address moved to {0}."
NEWLINE     = "\n"
NO_CONNECT  = 1
RESET_IP    = ["nordvpn", "connect", "Australia"]   # TODO remove hardcoding
STAT_DELIM  = " "


IS_POSINT   = lambda n      : n.isnumeric() and int(n) > 0
URLSTRIP    = lambda u      : URL_DELIM.join(u.split(URL_DELIM)[NO_START:])


"""
Wrapper for shared memory between threads
"""
class Threadargs:
    def __init__(self, top, threads, engine, tables):
        self._count = 0
        self._max   = top
        self._dbe   = engine
        self._tabs  = tables
        self._paths = set()
        self._lock  = Semaphore()
        while len(self._paths) < threads:
            self._paths.add(gen_temp_path())
        self._paths = list(self._paths)

    def inc(self):
        self._lock.acquire()
        self._count += 1
        self._lock.release()

    def get_temp_path(self, thread_no):
        return self._paths[thread_no]

    def get_session(self):
        return scoped_session(sessionmaker(bind = self._dbe))

    def get_tables(self):
        return self._tabs

    def is_finished(self):
        self._lock.acquire()
        result = self._count >= self._max
        self._lock.release()

        return result

    def lock(self):
        self._lock.acquire()

    def release(self):
        self._lock.release()


"""
Push unseen contributors for a repo into the user queue

session - sqla session for this thread
tables  - collection of sqla table objects
repo    - url of the repo for which to enqueue the contributors of
"""
def add_contributors(session, tables, repo, threadargs):
    js  = request_json(CONTRIBS, repo, threadargs)
    contribs = [u[UNAME] for u in js]
    for user in contribs:
        push_entity(session, tables, USER_ENT, user)


"""
Check a repo out into its isolated temp dir and scan everything in it

session - sqla session for this thread
tables  - collection of sqla table objects
tempdir - path of hidden temporary directory to checkout into

returns - name of repo iff interrupted mid-scan; else None
"""
def analyse_repo(session, tables, tempdir, threadargs):
    requeue = None
    try:
        search = pop_repo(session, tables, threadargs)
        requeue = search
        add_contributors(session, tables, search, threadargs)
        mkdir(tempdir)
        checkout_repo(search, tempdir)
        paths = itemise_repo(tempdir)
        scan_exif(session, tables, search, paths)
        requeue = None
        rmtree(tempdir)
    except KeyboardInterrupt:
        pass

    return requeue


"""
Check out a repo into a hidden temporary dir

url     - shortened url of repo to check out
tempdir - path of hidden temporary directory to checkout into

returns - return code of git clone command
"""
def checkout_repo(url, tempdir):
    cmd = CLONE_TMP.format(url, tempdir).split()
    ret = run(cmd, stdout = DEVNULL, stderr = DEVNULL).returncode

    if ret:
        print(BAD_CLONE.format(url))

    return ret


"""
Connect to an existing mariadb db if it exists or build a new one if not

returns - the associated sqla db engine and collection of table objects
"""
def connect_db():
    engine = create_engine(DB_ADDR)
    md = MetaData()
    if not database_exists(engine.url):
        create_database(engine.url)
        # Probably a better way of doing this
        users = Table(
            USER_ENT, md,
            Column(ID,          Integer, primary_key = True),
            Column(BASE_NAME,   String(LINK_LEN))
        )
        repos = Table(
            REPO_ENT, md,
            Column(ID,          Integer, primary_key = True),
            Column(BASE_NAME,   String(LINK_LEN))
        )
        user_queue = Table(
            USER_ENT + QUEUE_EXT, md,
            Column(ID,          Integer, primary_key = True),
            Column(USER_ENT,    Integer, ForeignKey(USER_ENT + IDEXT))
        )
        repo_queue = Table(
            REPO_ENT + QUEUE_EXT, md,
            Column(ID,          Integer, primary_key = True),
            Column(REPO_ENT,    Integer, ForeignKey(REPO_ENT + IDEXT))
        )
        hits = Table(
            HITS, md,
            Column(ID,          Integer, primary_key = True),
            Column(REPO_ENT,    Integer, ForeignKey(REPO_ENT + IDEXT)),
            Column(PATH, String(LINK_LEN))
        )
        md.create_all(engine)
    else:
        md.reflect(bind = engine)
        tables = set(list(md.tables.keys()))
        if tables != GOOD_TABLES:
            print(BAD_TABLES)
            exit(BAD_TABS)
        # Else; we'll just trust each table has the right cols for now
    tables = md.tables


    return engine, tables


"""
Enqueue every (<=30) repo a user has contributed to

session - sqla session for this thread
tables  - collection of sqla table objects
user    - username of user in question
"""
def crawl_user_repos(session, tables, user, threadargs):
    # TODO this could be combined with add_contributors realistically
    js  = request_json(USER_REPOS, user, threadargs)
    repos = [r[REPNAME] for r in js]
    for repo in repos:
        push_entity(session, tables, REPO_ENT, repo)


"""
Add a random repo to the queue

session - sqla session for this thread
tables  - collection of sqla table objects
"""
def fetch_random_repo(session, tables, threadargs):
    page = randint(0, RAND_MAX)
    js = request_json(RAND_REPO, page, threadargs)
    select = choice(js)
    repo = select[URL_ATTR]
    repo = "GNUNotUsername/git-privacy-spider"
    push_entity(session, tables, REPO_ENT, repo)


"""
Randomly generate the path of a hidden directory to put repos in temporarily

returns - the name of the directory
"""
def gen_temp_path():
    tempdir = ""
    while True:
        tempdir = HIDE + "".join([choice(al) for _ in range(RAND_LEN)])
        if not path.isdir(tempdir):
            break

    return tempdir


"""
Add a random repo to the queue

session - sqla session for this thread
tables  - collection of sqla table objects
repo    - repo to look up

returns - the table ID for the selected repo
"""
def get_repo_id(session, tables, repo):
    repos = tables[REPO_ENT]
    query = select(repos).where(repos.c.name == repo)
    find  = session.execute(query).fetchone()
    repo_id = None if find is None else find.id

    return repo_id


"""
Resolve the path of every file in a repo (less some git additions)

tempdir - the temporary directory for this repo

returns - the path to every file in this repo with spaces escaped
"""
def itemise_repo(tempdir):
    try:
        rmtree(tempdir + sep + GIT_EXTRA)
    except FileNotFoundError:
        pass
    try:
        rmtree(tempdir + sep + GH_EXTRA)
    except FileNotFoundError:
        pass
    base = map(lambda p : str(p), Path(tempdir).rglob(ALL))
    files = list(filter(lambda f : not path.isdir(f), base))

    return files


def move_ip():
    # TODO something's wrong I can feel it
    retcode = NO_CONNECT
    while retcode != CONNECTED:
        retcode = run(RESET_IP, stdout = DEVNULL).returncode

    status = check_output(FIND_IP).decode(UTF8).split(NEWLINE)
    ip = list(filter(lambda l : l.startswith(IP_TAG), status))[0]
    _, _, addr = ip.partition(STAT_DELIM)
    print(MOVED_IP.format(addr))


"""
Remove the first entity from a queue table

session - sqla session for this thread
tables  - collection of sqla table objects
tabkey  - the relevant table to pop from

return  - the string data of the popped entity (repo url / username)
"""
def pop_entity(session, tables, tabkey):
    queue   = tables[tabkey + QUEUE_EXT]
    agg     = tables[tabkey]
    query   = select(queue).limit(TOP)
    out     = session.execute(query).fetchone()
    if out is not None:
        query = delete(queue).where(queue.c.id == out.id)
        session.execute(query)
        session.commit()
        query = select(agg).where(agg.c.id == out.id)
        out = session.execute(query).fetchone()[NAME_IND]
        # TODO there's an index error floatin around here...

    return (out)


"""
Pop a repo from the repo queue and repopulate if necessary

session - sqla session for this thread
tables  - collection of sqla table objects

returns - the shortened URL of the repo popped
"""
def pop_repo(session, tables, threadargs):
    repo = pop_entity(session, tables, REPO_ENT)
    print(repo)
    while repo is None:
        user = pop_entity(session, tables, USER_ENT)
        if user is None:
            fetch_random_repo(session, tables, threadargs)
        else:
            crawl_user_repos(session, tables, user, threadargs)
        repo = pop_entity(session, tables, REPO_ENT)

    return repo


"""
Push a user or repo to its respective queue

session - sqla session for this thread
tables  - collection of sqla table objects
tabkey  - the relevant table to push to
url     - the (maybe shortened) URL of the entity to push
"""
def push_entity(session, tables, tabkey, url):
    # TODO rethink -- pass the exact table now that others scrapped?
    base_ent, queue = tables[tabkey], tables[tabkey + QUEUE_EXT]
    cut = url if url.count(URL_DELIM) < MIN_DELIMS else URLSTRIP(url)
    query = select(base_ent).where(base_ent.c.name == cut)
    check = session.execute(query).fetchone()
    if check is None:
        entry = {BASE_NAME: cut}
        session.execute(insert(base_ent).values(entry))
        fkey = session.execute(query).fetchone().id
        link = {tabkey: fkey}
        session.execute(insert(queue).values(link))
        session.commit()


def request_json(fmt, param, threadargs):
    if threadargs is not None:
        threadargs.lock()
    body = fmt.format(param)
    resp = get(body)
    while not resp.ok:
        move_ip()
        resp = get(body)
    if threadargs is not None:
        threadargs.release()
    json = loads(resp.text)

    return json


"""
Requeue an already seen repo

session - sqla session for this thread
tables  - collection of sqla table objects
repo    - name of repo to requeue
"""
def requeue_repo(session, tables, repo):
    repo_id = get_repo_id(session, tables, repo)
    queue = tables[REPO_ENT + QUEUE_EXT]
    session.execute(insert(queue).values({REPO_ENT: repo_id}))
    session.commit()


"""
Scan every file in a list of paths for GPS exif data

session - sqla session for this thread
tables  - collection of sqla table objects
repo    - the repo these files belongs to
paths   - list of file paths to scan
"""
def scan_exif(session, tables, repo, paths):
    for p in paths:
        try:
            raw = check_output([EXIFTOOL, p])
        except CalledProcessError:
            continue
        exif = raw.decode(UTF8)
        if GPS_ATTR in exif:
            _, _, path = p.partition(sep)
            repo_id = get_repo_id(session, tables, repo)
            record = {REPO_ENT: repo_id, PATH: path}
            session.execute(insert(tables[HITS]).values(record))
            session.commit()
            # TODO make this not allow duplicates


"""
Append the contents of the hits table to a csv file

engine  - sqla engine
tables  - collection of sqla table objects
path    - path to the file to append to
"""
def serialise_results(engine, tables, path):
    hits = engine.execute(select(tables[HITS])).fetchall()
    if len(hits) == 0:
        return

    repos = {}
    repos_all = tables[REPO_ENT]
    f = open(path, WRITE)
    csvw = writer(f)
    csvw.writerow(CSV_HEADER)

    for _, repo_id, repo_path in hits:
        if repo_id not in repos:
            query = select(repos_all).where(repos_all.c.id == repo_id)
            repo_name = engine.execute(query).fetchone()[NAME_IND]
            repos[repo_id] = repo_name
        name = repos[repo_id]
        csvw.writerow([name, repo_path])
    f.close()


"""
Thread wrapper to analyse_repo

thread_no   - the ID of this thread
args        - shared memory wrapper
"""
def thread_scraping(thread_no, args):
    session = args.get_session()
    tables = args.get_tables()
    tempdir = args.get_temp_path(thread_no)
    while not args.is_finished():
        requeue = analyse_repo(session, tables, tempdir, args)
        if requeue is not None:
            requeue_repo(session, tables, search)
            break

    if path.isdir(tempdir):
        rmtree(tempdir)


"""
Ensure that the given arg vector is valid

argv    - the arg vector to validate

returns - true iff valid ; no. repos to examine ; no. threads to use
"""
def validate(argv):
    verdict = False
    count = 0
    threads = 0
    argc = len(argv)

    if argc in GOOD_ARGCS:
        count = argv[COUNT]
        flag = argv[FLAG]
        match argc:
            case 2:
                threads = 1
                verdict = IS_POSINT(count)
            case 3:
                verdict = (flag == SERIAL_FLAG)
                threads = count = 0
            case 4:
                threads = argv[THREADS]
                verdict = (flag == THREAD_FLAG) and IS_POSINT(count) \
                        and IS_POSINT(threads)
            case _:
                verdict = False

        if verdict:
            count = int(count)
            threads = int(threads)

    return verdict, count, threads


def main():
    print("Starting")
    # Validate argv
    verdict, count, threads = validate(argv)
    if not verdict:
        exit(BAD_ARGV)

    # Set up environment
    dbe, tables = connect_db()

    if threads == 0:
        serialise_results(dbe, tables, argv[PATH_IND])
    elif threads > 1:
        move_ip()
        running = []
        args = Threadargs(count, threads, dbe, tables)
        for t in range(threads):
            add = Thread(target = thread_scraping, args = (t, args))
            add.start()
            running.append(add)
        for t in running:
            # Probably a better way of doing this
            t.join()
    else:
        # No point messing around with threadargs
        move_ip()
        dbs = scoped_session(sessionmaker(bind = dbe))
        requeue = None
        tempdir = gen_temp_path()
        for _ in range(count):
            requeue = analyse_repo(dbs, tables, tempdir, None)
            if requeue is not None:
                requeue_repo(dbs, tables, requeue)
                break

        if path.isdir(tempdir):
            rmtree(tempdir)


if __name__ == "__main__":
    main()
