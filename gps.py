"""
A web spider for analysing GPS data accidentally committed to github

USAGE:  sudo python gps.py count [repo-url]
"""


from os     import path
from sys    import argv

from sqlalchemy         import create_engine
from sqlalchemy_utils   import create_database, database_exists

import re


# Argc & Argv
COUNT       = 1
GOOD_ARGVS  = (2, 3)
URL         = 2
USE_URL     = 3

# Databases
DB_ADDR = "mariadb://root@localhost:3306/gitprivacyspider"

# Exit codes
BAD_ARGV    = 1

# IO
WRITE   = "w"

# Regex
GH_REG  = r"https://github\.com/[A-Za-z]+/([A-Za-z0-9]+(\.[A-Za-z0-9]+)+)"


def connect_db():
    engine = create_engine(DB_ADDR)
    if not database_exists(engine.url):
        create_database(engine.url)


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

    connect_db()


if __name__ == "__main__":
    main()
