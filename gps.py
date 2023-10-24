"""
A web spider for analysing GPS data accidentally committed to github

USAGE:  python gps.py checkpoint dataset count [repo-url]
"""


from os     import path
from sys    import argv

import re


# Argc & Argv
CHECKPOINT  = 1
COUNT       = 3
GOOD_ARGVS  = (4, 5)
URL         = 4
USE_URL     = 5

# Exit codes
BAD_ARGV    = 1

# IO
WRITE   = "w"

# Regex
GH_REG  = r"https://github\.com/[A-Za-z]+/([A-Za-z0-9]+(\.[A-Za-z0-9]+)+)"


def unpack(argv):
    files = tuple(argv[CHECKPOINT: COUNT])
    for f in files:
        # Touch some empty files to make rdds less annoying
        if not path.exists(f):
            fp = open(f, WRITE)
            fp.close()
    checkpoint, dataset = files
    count = int(argv[COUNT])

    return checkpoint, dataset, count


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

    checkpoint, dataset, count = unpack(argv)


if __name__ == "__main__":
    main()
