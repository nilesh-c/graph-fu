#!/usr/local/bin/python2.7

import sys
import csv
from datetime import date

def main():
    for line in sys.stdin:
        line = line.split("\t")[1]
        line = line.split(",")
        line[-2] = date.fromtimestamp(float(line[-2])).strftime("%d-%m-%Y")
        line = ",".join(line)
        sys.stdout.write(line)

if __name__ == "__main__":
    main()
