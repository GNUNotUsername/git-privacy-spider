"""
A web spider for analysing GPS data accidentally committed to github

USAGE:  sudo python gps.py count
"""


from os     import path
from sys    import argv

from sqlalchemy         import Column,          create_engine, ForeignKey, Integer, insert, MetaData, select, String, Table
from sqlalchemy_utils   import create_database, database_exists

import re


# Argc & Argv
COUNT       = 1
GOOD_ARGC   = 2

# Databases
DB_ADDR     = "mariadb://root@localhost:3306/gitprivacyspider"
LINK_LEN    = 255
GOOD_TABLES = {"repo", "user", "repo_seen", "user_seen", "repo_queue", "hits", "user_queue"}
REPO_ENT    = "repo"

# Error Messages
BAD_TABLES  = "Tables do not match required schema; please fix in MariaDB"

# Exit codes
BAD_ARGV    = 1
BAD_TABS    = 2

# IO
WRITE   = "w"

# URLs
IGNORE_START    = 3
URL_DELIM       = "/"


def connect_db():
    engine = create_engine(DB_ADDR)
    md = MetaData()
    if not database_exists(engine.url):
        create_database(engine.url)
        # Probably a better way of doing this
        users = Table(
            "user", md,
            Column("id",    Integer, primary_key = True),
            Column("user",  String(LINK_LEN))
        )
        repos = Table(
            "repo", md,
            Column("id",    Integer, primary_key = True),
            Column("name",  String(LINK_LEN))
        )
        user_queue = Table(
            "user_queue", md,
            Column("id",    Integer, primary_key = True),
            Column("name",  Integer, ForeignKey("user.id"))
        )
        user_seen = Table(
            "user_seen", md,
            Column("id",    Integer, primary_key = True),
            Column("user",  Integer, ForeignKey("user.id"))
        )
        repo_queue = Table(
            "repo_queue", md,
            Column("id",    Integer, primary_key = True),
            Column("repo",  Integer, ForeignKey("repo.id"))
        )
        repo_seen = Table(
            "repo_seen", md,
            Column("id",    Integer, primary_key = True),
            Column("repo",  Integer, ForeignKey("repo.id"))
        )
        hits = Table(
            "hits", md,
            Column("id",        Integer, primary_key = True),
            Column("repo",      Integer, ForeignKey("repo.id")),
            Column("committer", Integer, ForeignKey("user.id")),
            Column("repo-path", String(LINK_LEN))
        )
        md.create_all(engine)
    else:
        md.reflect(bind = engine)
        tables = set(list(md.tables.keys()))
        if tables != GOOD_TABLES:
            print(tables)
            print(BAD_TABLES)
            exit(BAD_TABS)
        # Else; we'll just trust each table has the right cols for now
        # TODO make it idiot proof
    tables = md.tables

    return engine, tables


def push_entity(engine, tables, tabkey, url):
    base_ent, queue = tables[tabkey], tables[f"{tabkey}_queue"]
    cut = URL_DELIM.join(url.split(URL_DELIM)[IGNORE_START:])
    query = select(base_ent).where(base_ent.c.name == cut)
    check = engine.execute(query).fetchone()
    if check is None:
        entry = {tabkey: cut}
        engine.execute(insert(base_ent).values(entry))
        fkey = engine.execute(query).fetchone().id
        link = {tabkey: fkey}
        engine.execute(insert(queue).values(link))


def validate(argv):
    verdict = True
    argc = len(argv)

    verdict = argc == GOOD_ARGC
    if verdict:
        count = argv[COUNT]
        verdict = count.isnumeric() and int(count) > 0

    return verdict


def main():
    if not validate(argv):
        exit(BAD_ARGV)

    count = int(argv[COUNT])
    dbe, tables = connect_db()


if __name__ == "__main__":
    main()
