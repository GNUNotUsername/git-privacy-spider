"""
A web spider for analysing GPS data accidentally committed to github

USAGE:  sudo python gps.py count [repo-url]
"""


from os     import path
from sys    import argv

from sqlalchemy         import Column,          create_engine, ForeignKey, Integer, MetaData, String, Table
from sqlalchemy_utils   import create_database, database_exists

import re


# Argc & Argv
COUNT       = 1
GOOD_ARGVS  = (2, 3)
URL         = 2
USE_URL     = 3

# Databases
DB_ADDR     = "mariadb://root@localhost:3306/gitprivacyspider"
LINK_LEN    = 255
GOOD_TABLES = {"repos", "users", "repo_seen", "user_seen", "repo_queue", "hits", "user_queue"}

# Error Messages
BAD_TABLES  = "Tables do not match required schema; please fix in MariaDB"

# Exit codes
BAD_ARGV    = 1
BAD_TABS    = 2

# IO
WRITE   = "w"

# Regex
GH_REG  = r"https://github\.com/[A-Za-z]+/([A-Za-z0-9]+(\.[A-Za-z0-9]+)+)"


def connect_db():
    engine = create_engine(DB_ADDR)
    md = MetaData()
    if not database_exists(engine.url):
        create_database(engine.url)
        # Probably a better way of doing this
        users = Table(
            "users", md,
            Column("id",    Integer, primary_key = True),
            Column("user",  String(LINK_LEN))
        )
        repos = Table(
            "repos", md,
            Column("id",    Integer, primary_key = True),
            Column("repo",  String(LINK_LEN))
        )
        user_queue = Table(
            "user_queue", md,
            Column("id",    Integer, primary_key = True),
            Column("user",  Integer, ForeignKey("users.id"))
        )
        user_seen = Table(
            "user_seen", md,
            Column("id",    Integer, primary_key = True),
            Column("user",  Integer, ForeignKey("users.id"))
        )
        repo_queue = Table(
            "repo_queue", md,
            Column("id",    Integer, primary_key = True),
            Column("user",  Integer, ForeignKey("repos.id"))
        )
        repo_seen = Table(
            "repo_seen", md,
            Column("id",    Integer, primary_key = True),
            Column("user",  Integer, ForeignKey("repos.id"))
        )
        hits = Table(
            "hits", md,
            Column("id",        Integer, primary_key = True),
            Column("repo",      Integer, ForeignKey("repos.id")),
            Column("committer", Integer, ForeignKey("users.id")),
            Column("repo-path", String(LINK_LEN))
        )
        md.create_all(engine)
    else:
        md.reflect(bind = engine)
        tables = set(list(md.tables.keys()))
        if tables != GOOD_TABLES:
            print(BAD_TABLES)
            exit(BAD_TABS)
        # Else; we'll just trust each table has the right cols for now
        # TODO make it idiot proof

    return engine, md


def validate(argv):
    verdict = True
    argc = len(argv)

    verdict = argc in GOOD_ARGVS
    if verdict:
        count = argv[COUNT]
        verdict = count.isnumeric() and int(count) > 0
    if verdict and argc == USE_URL:
        regex = re.compile(GH_REG, re.IGNORECASE)
        verdict = regex.match(argv[URL])
    if verdict:
        verdict = len(set(argv)) == argc

    return verdict


def main():
    if not validate(argv):
        exit(BAD_ARGV)

    dbe, md = connect_db()


if __name__ == "__main__":
    main()
