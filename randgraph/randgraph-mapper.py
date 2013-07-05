#!/usr/local/bin/python2.7

import sys
import csv
import time
import datetime
import random
from dateutil.rrule import rrule, DAILY

def main():
    reader = csv.reader(sys.stdin, delimiter='|')
    for line in reader:
        node = line[0].split(',')[0]

        callCount = lambda : biasedRandom(0, 50, 2, random.randint(2,4))
        totalVolume = lambda : biasedRandom(0, 2000, 50, random.randint(1,6))
        maxmin = lambda lo, hi : random.randint(lo, hi)
        callDate = lambda : time.mktime(random.choice(list(rrule(DAILY,dtstart=datetime.date(2010,1,1),until=datetime.date(2010,3,31)))).timetuple())

        count = callCount()
        totvol = totalVolume()
        maxvol = maxmin(totvol/4+1, totvol/2+1)
        minvol = maxmin((totvol-maxvol)/5+1, (totvol-maxvol)/2+1)
        date = callDate()
        for edge in [(edge, count, totvol, maxvol, minvol, date) for edge in line[1:]]:
            sys.stdout.write(str(date) + "\t")
            sys.stdout.write(str(node) + ",")
            for x in edge:
                sys.stdout.write(str(x) + ",")
            print "\b "

def stringToInt(s):
    ord3 = lambda x : '%.3d' % ord(x)
    return int(''.join(map(ord3, s)))

def biasedRandom(lo, hi, target, steps=1):
    if lo >= hi:
        raise ValueError("lo should be less than hi")
    elif target < lo or target >= hi:
        raise ValueError("target not in range(lo, hi)")
    else:
        num = random.randint(lo, hi)
        for i in range(steps):
            num += int(random.random() * (target - num))
        return num

if __name__ == "__main__":
    main()
