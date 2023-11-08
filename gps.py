"""
A web spider for analysing GPS data accidentally committed to github

USAGE:  sudo python gps.py count
"""
# TODO add csv write option


from json       import loads
from os         import mkdir, path
from random     import choice
from requests   import get
from shutil     import rmtree
from string     import ascii_letters as al
from sys        import argv


from selenium.common.exceptions         import ElementNotInteractableException
from selenium.webdriver                 import Chrome, ChromeOptions
from selenium.webdriver.common.by       import By
from webdriver_manager.chrome           import ChromeDriverManager as CDM
from selenium.webdriver.chrome.service  import Service
from sqlalchemy                         import Column,          create_engine, delete, ForeignKey, Integer, insert, MetaData, select, String, Table
from sqlalchemy_utils                   import create_database, database_exists


# Argc & Argv
COUNT       = 1
GOOD_ARGC   = 2

# Databases
BASE_NAME   = "name"
DB_ADDR     = "mariadb://root@localhost:3306/gitprivacyspider"
GOOD_TABLES = {"repo", "user", "repo_queue", "hits", "user_queue"}
ID          = "id"
IDEXT       = "." + ID
LINK_LEN    = 255
NAME_IND    = 1
QUEUE_EXT   = "_queue"
REPO_ENT    = "repo"
TOP         = 1
USER_ENT    = "user"

# Error Messages
BAD_TABLES  = "Tables do not match required schema; please fix in MariaDB"

# Exit codes
BAD_ARGV    = 1
BAD_TABS    = 2

# Github API
API_HEAD    = "https://api.github.com/"
CONTRIBS    = (API_HEAD + "repos/{0}/contributors")
UNAME       = "login"
USER_REPOS  = (API_HEAD + "users/{0}/repos")

# IO
WRITE       = "w"

# Pathing
HIDE        = "."
RAND_LEN    = 20

# Scraping
RANDOM_URL  = "https://gitrandom.digitalbunker.dev"
CHROME_OPS  = ["--no-sandbox", "--headless"]
LINK_ATTR   = "href"
NEXT_XPATH  = "/html/body/div[3]/div/div[1]/div[2]/form/div[2]/button"
REPO_ID     = "currentRepoURL"

# URLs
MIN_DELIMS  = 2
NO_START    = 3
URL_DELIM   = "/"


URLSTRIP    = lambda u : URL_DELIM.join(u.split(URL_DELIM)[NO_START:])


"""
Push unseen contributors for a repo into the user queue

engine  - sqla db engine
tables  - collection of sqla table objects
repo    - url of the repo for which to enqueue the contributors of
"""
def add_contributors(engine, tables, repo):
    url = CONTRIBS.format(repo)
    req = get(url).text
    js  = loads(req)
    contribs = [u[UNAME] for u in js]
    for user in contribs:
        push_entity(engine, tables, USER_ENT, user)


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
            "hits", md,
            Column(ID,          Integer, primary_key = True),
            Column(REPO_ENT,    Integer, ForeignKey(REPO_ENT + IDEXT)),
            Column("committer", Integer, ForeignKey(USER_ENT + IDEXT)),
            Column("path", String(LINK_LEN))
        )
        md.create_all(engine)
    else:
        md.reflect(bind = engine)
        tables = set(list(md.tables.keys()))
        if tables != GOOD_TABLES:
            print(BAD_TABLES)
            exit(BAD_TABS)
        # Else; we'll just trust each table has the right cols for now
        # TODO make it idiot proof later
    tables = md.tables

    return engine, tables


"""
Randomly generate a hidden directory to put repos in temporarily

returns - the name of the directory
"""
def make_temp_dir():
    tempdir = ""
    while True:
        tempdir = HIDE + "".join([choice(al) for _ in range(RAND_LEN)])
        if not path.isdir(tempdir):
            break
    mkdir(tempdir)

    return tempdir


"""
Remove the first entity from a queue table

engine  - sqla db engine
tables  - collection of sqla table objects
tabkey  - the relevant table to pop from

return  - the string data of the popped entity (repo url / username)
"""
def pop_entity(engine, tables, tabkey):
    queue = tables[tabkey + QUEUE_EXT]
    agg = tables[tabkey]
    query = select(queue).limit(TOP)
    out = engine.execute(query).fetchone()
    if out is not None:
        query = delete(queue).where(queue.c.id == out.id)
        engine.execute(query)
        query = select(agg).where(agg.c.id == out.id)
        out = engine.execute(query).fetchone()[NAME_IND]

    return (out)


"""
Pop a repo from the repo queue and repopulate if necessary

engine  - sqla db engine
tables  - collection of sqla table objects

returns - the shortened URL of the repo popped
"""
def pop_repo(engine, tables):
    repo = pop_entity(engine, tables, REPO_ENT)
    while repo is None:
        user = pop_entity(engine, tables, USER_ENT)
        if user is None:
            scrape_random_repo(engine, tables)
        else:
            crawl_user_repos(engine, tables, user)
        repo = pop_entity(engine, tables, REPO_ENT)

    return repo


"""
Push a user or repo to its respective queue

engine  - sqla db engine
tables  - collection of sqla table objects
tabkey  - the relevant table to push to
url     - the (maybe shortened) URL of the entity to push
"""
def push_entity(engine, tables, tabkey, url):
    # TODO rethink if you can just pass the exact table now that those other are scrapped
    base_ent, queue = tables[tabkey], tables[tabkey + QUEUE_EXT]
    cut = url if url.count(URL_DELIM) < MIN_DELIMS else URLSTRIP(url)
    query = select(base_ent).where(base_ent.c.name == cut)
    check = engine.execute(query).fetchone()
    if check is None:
        entry = {BASE_NAME: cut}
        engine.execute(insert(base_ent).values(entry))
        fkey = engine.execute(query).fetchone().id
        link = {tabkey: fkey}
        engine.execute(insert(queue).values(link))


"""
Add a random repo to the queue by scraping it from an online tool

engine  - sqla db engine
tables  - collection of sqla table objects
"""
def scrape_random_repo(engine, tables):
    ops = ChromeOptions()
    for a in CHROME_OPS:
        ops.add_argument(a)

    c = Chrome(service = Service(CDM().install()), options = ops)
    c.get(RANDOM_URL)
    c.find_element(By.XPATH, NEXT_XPATH).click()
    link = ""
    while len(link) == 0:
        link = c.find_element(By.ID, REPO_ID).get_attribute(LINK_ATTR)
    push_entity(engine, tables, REPO_ENT, link)
    c.quit()


"""
Ensure that the given arg vector is valid

argv    - the arg vector to validate

returns - true iff valid; else false
"""
def validate(argv):
    verdict = True
    argc = len(argv)

    verdict = argc == GOOD_ARGC
    if verdict:
        count = argv[COUNT]
        verdict = count.isnumeric() and int(count) > 0

    return verdict


def main():
    # Validate argv
    if not validate(argv):
        exit(BAD_ARGV)

    # Unpack argv and set up environment
    count = int(argv[COUNT])
    dbe, tables = connect_db()
    tempdir = make_temp_dir()

    # Spider across all of github haphazardly
    requeue = None
    for _ in range(count):
        try:
            search = pop_repo(dbe, tables)
            add_contributors(dbe, tables, search)
            input("look")
        except KeyboardInterrupt:
            requeue = search
            break

    # Clean up anything left over
    rmtree(tempdir)
    if requeue is not None:
        # Current repo is probably not fully analysed yet so re-push
        # but we can't push_entity because the repo is already seen
        engine.execute(insert(tables[REPO_ENT]).values({BASE_ENT: search}))

if __name__ == "__main__":
    main()
