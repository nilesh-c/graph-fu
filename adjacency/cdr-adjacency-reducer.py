#!/usr/local/bin/python2.7

import sys
import csv
from datetime import date

def main():
    prev = line = [s.strip() for s in sys.stdin.readline().split("\t")]
    edges = [line[1]]
    while True:
        prev = line
        line = [s.strip() for s in sys.stdin.readline().split("\t")]
        if not line or line == ['']:
	    sys.stdout.write(prev[0] + "," + ",".join(edges) + "\n")
	    break
        if prev[0] == line[0]:
            edges.append(line[1])
        elif len(edges) > 0:
            sys.stdout.write(prev[0] + "," + ",".join(edges) + "\n")
            edges = [prev[1]]

if __name__ == "__main__":
    main()
