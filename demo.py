
import os

from bigsort import  bigsort, sortFile, check, bisect


def test():
    a = [x for x in range(10**6)]
    bisect(a, 100, lambda x, y: x <= y)
    a = [10**6-x for x in range(10**6)]
    bisect(a, 100, lambda x, y: x >= y)


# test()

# sort in file
# sortFile("cat C:/data/bookcorpus/*.txt", "sorted.txt", budget=0.1)
# check(open("sorted.txt"), "<=")

# sort in pipe
# bigsort(os.popen("cat bookcorpus.txt"), open("sorted.txt", 'w', buffering=1024*1024), unique=1, sortType="d", budget=0.1)
# check(open("sorted.txt"), ">")

# sort in shell
"""
bigsort -i  readme.md -o sorted.txt  # default sort in increase 
cat readme.md |  bigsort --sortType=d --unique=1 > sorted.txt  # sort pipe, order in descend, unique
bigsort -i sorted.txt -c ">"  # check order
bigsort -i  readme.md --unique=1   | bigsort --sortType=R > sorted.txt   # unique and shufle 
head -c 100000000  /dev/urandom  | python bigsort.py -C=1000 -m=0.999 --sortType=R -T="./" > sorted.txt
wc -l *.py | bigsort   -k 1n,2  -b 1 -t " "   # sort by key
"""

# custom sort


def keyFn(x):
    return x


reader = os.popen("cat readme.md")
writer = open("sorted.txt",'w',buffering=1024*1024)
bigsort(reader, writer, keyFn=keyFn)

src = "cat readme.md"
tgt = "sorted.txt"
sortFile(src, tgt, keyFn=keyFn)

check(open("sorted.txt"),">")
