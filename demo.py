import random
import os

from bigsort import BigSort, bigsort, sortFile, check, bisect
# from bigsortQ import BigSort, bigsort, sortFile, check, bisect


def test():
    a = [x for x in range(10**6)]
    bisect(a, 100, lambda x, y: x <= y)
    a = [10**6-x for x in range(10**6)]
    bisect(a, 100, lambda x, y: x >= y)


# test()

# sort in file
sortFile("cat C:/data/bookcorpus/*.txt", "sorted.txt", budget=0.1)
check(open("sorted.txt"), "<=")

# sort in pipe
# bigsort(os.popen("cat bookcorpus.txt"), open("sorted.txt", 'w', buffering=1024*1024), unique=1, sortType="d", budget=0.1)
# check(open("sorted.txt"), ">")

# sort in shell
"""
bigsort -i  readme.md -o sorted.txt  # default sort in increase 
cat readme.md |  bigsort --sortType=d --unique=1 > sorted.txt  # sort pipe, order in descend, unique
bigsort -i sorted.txt -c ">"  # check order
bigsort -i  readme.md --unique=1   | bigsort --sortType=R > sorted.txt   # unique and shufle 
seq 0  1123456789  | bigsort --sortType=d -T "./"  > sorted.txt  # just try sort 10^10 numbers
"""

# custom sort


def sortFn(array, sortType):
    if sortType == 'R':
        random.shuffle(array)
    elif sortType == "i":
        array.sort()
    elif sortType == 'd':
        array.sort(reverse=True)
    return array


def bisect(arr, val, cmp):
    l = -1
    r = len(arr)
    while r-l > 1:
        m = (l+r)//2
        if cmp(arr[m], val):
            l = m
        else:
            r = m
    return r


def splitFn(queue, sortType, pivot, nSplit):
    if sortType == 'R':
        random.shuffle(queue)
    if pivot == None:
        return queue, []
    if sortType == 'R':
        idx = len(queue)//nSplit
    elif sortType == 'i':
        idx = bisect(queue, pivot, lambda x, y: x <= y)
    elif sortType == 'd':
        idx = bisect(queue, pivot, lambda x, y: x >= y)
    lines = queue[:idx]
    queue = [] if idx == len(queue) else queue[idx:]
    return lines, queue


import sys,tempfile
def bigsort(reader, writer, sortType='i', unique=False, nHead=-1, budget=0.8, nSplit=10, nLine=10000, tmpDir=None, sortFn=sortFn, splitFn=splitFn):
    temp_dir = tempfile.TemporaryDirectory(dir=tmpDir)
    sorter = BigSort(sortType=sortType, unique=unique, nHead=nHead, budget=budget, nSplit=nSplit, nLine=nLine, tmpDir=tmpDir, sortFn=sortFn, splitFn=splitFn)
    lines = sorter.sort(reader, temp_dir.name)
    for l in lines:
        writer.write(l)
    temp_dir.cleanup()


def sortFile(src=None, tgt=None, sortType='i', unique=False, nHead=-1, budget=0.8, nSplit=10, nLine=10000, tmpDir=None, buffering=1024*1024):
    if not src:
        reader = sys.stdin
    else:
        if ' ' not in src:
            src = "cat " + src
        reader = os.popen(src)
    if not tgt:
        writer = sys.stdout
    else:
        writer = open(tgt, 'w', buffering=buffering)
    bigsort(reader, writer, sortType=sortType, unique=unique, nHead=nHead, budget=budget, nSplit=nSplit, nLine=nLine, tmpDir=tmpDir)
    writer.close()
