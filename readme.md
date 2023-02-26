# bigsort
sort or shuffle big file/stream

## usage
> pip install bigsort 

### shell

```shell
bigsort -i  readme.md -o sorted.txt  # default sort in increase 
cat readme.md |  bigsort --sortType=d --unique=1 > sorted.txt  # sort pipe, order in descend, unique
bigsort -i sorted.txt -c ">"  # check order
cat readme.md | bigsort --sortType=R > sorted.txt  # shuffle

bigsort -i  readme.md --unique=1   | bigsort --sortType=R > sorted.txt   # unique and shufle 

head -c 100000 /dev/urandom | bigsort >sorted.txt  # just try
```

### python

```python
import os
from bigsort import BigSort,bigsort,sortFile,check,bisect

# sort in file
sortFile("cat readme.md","sorted.txt",budget=0.8)
check(open("sorted.txt"),"<=")

# sort in pipe
bigsort(os.popen("cat readme.md"),open("sorted.txt",'w',buffering=1024*1024),unique=1,sortType="d",budget=0.8)
check(open("sorted.txt"),">")
```

## custom sort
```python
# custom sort
import random
def sortFn(array,sortType):
    if sortType=='R':
        random.shuffle(array)
    elif sortType=="i":
        array.sort()
    elif sortType=='d':
        array.sort(reverse=True)
    return array

def splitFn(queue,sortType,pivot,nSplit):
    if pivot==None :
        return queue,[]
    if sortType=='R':
        idx=len(queue)//nSplit
    elif sortType=='i':
        idx=bisect(queue,pivot,lambda x,y:x<=y)
    elif sortType=='d':
        idx=bisect(queue,pivot,lambda x,y:x>=y)
    lines=queue[:idx]
    queue=[] if idx==len(queue) else queue[idx:]
    return lines,queue

def bigsort(reader,writer,sortType='i',unique=False,budget=0.8,nSplit=10,nLine=10000,tmpDir=None,sortFn=sortFn,splitFn=splitFn):
    sorter=BigSort(sortType=sortType,unique=unique,budget=budget,nSplit=nSplit,nLine=nLine,tmpDir=tmpDir,sortFn=sortFn,splitFn=splitFn)
    sorter.sort(reader,writer)
```