import os

from bigsort import BigSort,bigsort,sortFile,check,bisect
def test():
    a = [ x for x in range(10**6)]
    bisect(a, 100, lambda x, y: x <= y)
    a = [10**6-x for x in range(10**6)]
    bisect(a, 100, lambda x, y: x >= y)
test()

# sort in file
sortFile("cat bookcorpus.txt","sorted.txt",budget=0.1)
check(open("sorted.txt"),"<=")

# sort in pipe
bigsort(os.popen("cat bookcorpus.txt"),open("sorted.txt",'w',buffering=1024*1024),unique=1,sortType="d",budget=0.1)
check(open("sorted.txt"),">")

# sort in command
os.system("cat bookcorpus.txt | python bigsort.py --sortType=d --unique=1 > sorted.txt")
os.system("python bigsort.py -i sorted.txt -c != ")
os.system("cat bookcorpus.txt | bigsort --sortType=d --unique=1 > sorted.txt")
os.system('bigsort -i sorted.txt -c ">" ')

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


def bigsort(reader,writer,sortType='i',unique=False,budget=0.8,nSplit=10,nLine=10000,tmpDir=None):
    sorter=BigSort(sortType=sortType,unique=unique,budget=budget,nSplit=nSplit,nLine=nLine,tmpDir=tmpDir,sortFn=sortFn,splitFn=splitFn)
    sorter.sort(reader,writer)

