#!/usr/local/bin/python2.7

import sys
import csv
import time
import datetime
import random
from dateutil.rrule import rrule, DAILY

def main():
    reader = csv.reader(sys.stdin, delimiter=',')
    for line in reader:
        sys.stdout.write(line[0] + "\t" + line[1])

if __name__ == "__main__":
    main()
