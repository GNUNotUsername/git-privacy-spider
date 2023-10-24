"""
A web spider for analysing GPS data accidentally committed to github

USAGE:  python gps.py checkpoint dataset count [repo-url]
"""


from sys import argv


import re


# Argc & Argv
COUNT       = 3
GOOD_ARGVS  = (4, 5)
URL         = 4
USE_URL     = 5

# Exit codes
BAD_ARGV    = 1

# Regex
GH_REG  = r"https://github\.com/[A-Za-z]+/([A-Za-z0-9]+(\.[A-Za-z0-9]+)+)"

def validate(argv):
    verdict = True
    argc = len(argv)

    verdict = argc in GOOD_ARGVS
    if verdict:
        verdict = argv[COUNT].isnumeric()
    if verdict and argc == USE_URL:
        regex = re.compile(GH_REG, re.IGNORECASE)
        verdict = regex.match(argv[URL])
    if verdict:
        verdict = len(set(argv)) == argc

    return verdict


def main():
    if not validate(argv):
        exit(BAD_ARGV)

if __name__ == "__main__":
    main()
