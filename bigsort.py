import operator
import sys
import psutil
import os,time
import argparse
import random
import tempfile
import math
from multiprocessing import Queue, Process

import logzero
from logzero import logger
# logzero.loglevel(logzero.INFO)
logzero.loglevel(logzero.ERROR)
# logger.setLevel(level=logger.INFO)
# import logging
# logger = logging.getLogger()
# logger.propagate = False
# logger.handlers.clear()
# logger.setLevel(level=logging.INFO)
# # logger.setLevel(level=logging.ERROR)
# # handler = logging.FileHandler("log.txt")
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# formatter = logging.Formatter(
#     '%(asctime)s %(filename)s-%(lineno)d-%(levelname)s - %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)


def free():
    available = psutil.virtual_memory().available
    return available


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


class Block:
    def __init__(self, path, bucket, sortType, nSplit, buffering, sortFn) -> None:
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.n_line = len(bucket)
        bucket = sortFn(bucket, sortType)
        step = math.ceil(len(bucket)/nSplit)
        Nodes = []
        for i in range(0, len(bucket), step):
            batch = bucket[i:i+step]
            name = f"{self.path}/node-{i}"
            node = Node(name, batch, buffering)
            Nodes.append(node)
        self.Nodes = Nodes


class Node:
    def __init__(self, name, batch, buffering) -> None:
        self.name = name
        self.n_line = len(batch)
        self.head = batch[0]
        self.tail = batch[-1]
        if name==None:
            self.bucket=batch
        else:
            writer = open(name, mode='w', buffering=buffering)
            writer.writelines(batch)
            writer.close()

    def catch(self):
        if self.name==None:
            return self.bucket
        bucket = open(self.name).readlines()
        os.unlink(self.name)
        return bucket


class BigSort:
    def __init__(self, sortType='i', unique=False,head=-1, budget=0.8, nSplit=10, nLine=100000, buffering=1024*1024, tmpDir=None, sortFn=sortFn, splitFn=splitFn):
        self.head = head
        self.buffering = buffering
        self.nSplit = nSplit
        self.nLine = nLine
        self.budget = budget
        self.unique = unique
        assert sortType in 'idR'
        self.sortType = sortType
        if unique and sortType not in 'id':
            exit("unique only when sortType in i/d")
        self.tmpDir = tmpDir
        self.sortFn = sortFn
        self.splitFn = splitFn
        self.MEM = free()*budget
        logger.info(f"MEM  {self.MEM//1024//1024}M")
        self.n_readed=0
        self.n_writed=0

    def map(self, reader, folder):
        Nodes = []
        bucket = []
        total = 0
        n_block = 0
        for l in reader:
            bucket.append(l)
            if bucket and len(bucket) % self.nLine == 0:
                usage = self.MEM/self.budget-free()
                if usage < self.MEM:
                    continue
                total += len(bucket)
                block_dir = f"{folder}/block-{n_block}"
                block = Block(block_dir, bucket, self.sortType, self.nSplit, self.buffering, self.sortFn)
                logger.info(f"map:{total} bucket:{len(bucket)} --> {block_dir}")
                Nodes += block.Nodes
                bucket = []
                n_block += 1

        if bucket:
            bucket = sortFn(bucket, self.sortType)
            if total == 0:
                node=Node(None,bucket,self.buffering)
                Nodes.append(node)
                total += len(bucket)
            else:
                total += len(bucket)
                block_dir = f"{folder}/block-{n_block}"
                block = Block(block_dir, bucket, self.sortType, self.nSplit, self.buffering, self.sortFn)
                logger.info(f"map:{total} bucket:{len(bucket)} --> {block_dir}")
                Nodes += block.Nodes
        logger.info(f"map done!  {vars(reader)}  maped:{total}  -->  Nodes:{len(Nodes)} ")
        return Nodes

    def reduce(self, Nodes):
        queue = []
        n_read = 0
        n_write = 0

        for i, node in enumerate(Nodes):
            bucket = node.catch()
            queue += bucket
            pivot = Nodes[i+1].head if i != len(Nodes)-1 else None
            queue = self.sortFn(queue, self.sortType)
            lines, queue = self.splitFn(queue, self.sortType, pivot, self.nSplit)
            # last = self.write(lines, writer, last)
            for l in lines:
                yield l
            r1 = len(bucket)
            w1 = len(lines)
            n_read += r1
            n_write += w1
            if i % self.nSplit == 0:
                logger.info(f"Node:{i}/{len(Nodes)} n_read:{n_read} -- n_write:{n_write} queue:{len(queue)}  r1:{r1} -> w1:{w1}   {r1-w1} ")
        logger.info(f"reduce done! n_read:{n_read} --> n_write:{n_write} ")

    def outflow(self, reciver):
        last = None
        for x in reciver:
            self.n_readed+=1
            if self.n_writed>=self.head>=0:
                return 
            if self.unique and  x == last:
                continue
            last = x
            self.n_writed+=1
            yield x

    def sort(self, reader,tmpDir):
        # import time
        # t0=time.time()
        Nodes = self.map(reader, tmpDir)
        Nodes = [(x.head, x.tail,i,x) for i,x in enumerate(Nodes)]
        Nodes = self.sortFn(Nodes, self.sortType)
        Nodes = [ x[-1] for x in Nodes]
        # t1=time.time()
        reciver=self.reduce(Nodes)
        lines=self.outflow(reciver)

        # t2=time.time()
        # logger.info(f"map {t1-t0} reduce {t2-t1}")
        logger.info(f" n_readed:{self.n_readed} n_writed:{self.n_writed}")
        return lines


def bigsort(reader, writer, sortType='i', unique=False,head=-1, budget=0.8, nSplit=10, nLine=10000, tmpDir=None, sortFn=sortFn, splitFn=splitFn):
    temp_dir = tempfile.TemporaryDirectory(dir=tmpDir)
    sorter = BigSort(sortType=sortType, unique=unique,head=head, budget=budget, nSplit=nSplit, nLine=nLine, tmpDir=tmpDir, sortFn=sortFn, splitFn=splitFn)
    lines=sorter.sort(reader, temp_dir.name)
    for l in lines:
        writer.write(l)
    writer.flush()
    temp_dir.cleanup()    


def sortFile(src=None, tgt=None, sortType='i', unique=False,head=-1, budget=0.8, nSplit=10, nLine=10000, tmpDir=None, buffering=1024*1024):
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
    bigsort(reader, writer, sortType=sortType, unique=unique,head=head, budget=budget, nSplit=nSplit, nLine=nLine, tmpDir=tmpDir)
    # writer.close()


# https://docs.python.org/3/library/operator.html
OrderingFn = {
    '<': operator.le,
    '<=': operator.lt,
    '==': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt,

}


def check(reader, Ordering='<='):
    cmp = OrderingFn[Ordering]
    last = None
    i = -1
    for i, l in enumerate(reader):
        if last == None:
            last = l
            continue
        # assert last<l
        assert cmp(last, l), f"{last} {Ordering} {l}"
    print(f'check {i+1} lines {Ordering} , ok!')
    return True


def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("src") # sys.stdin
    # parser.add_argument("tgt") # sys.stdout
    parser.add_argument("-i", "--input", default=None)
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument("--buffering", type=int, default=1024*1024)
    parser.add_argument("--nSplit", type=int, default=10)  # bigger if skew or shuffle
    parser.add_argument("--nLine", type=int, default=100000)
    parser.add_argument("-M","--budget", type=float, default=0.5)  # 0.8 memary budget in ratio
    parser.add_argument("-u", "--unique", default=False)  # only when sort
    parser.add_argument("-s", "--sortType", default="i")  # one of  'i/d/R': increase descend random
    parser.add_argument("-n","--number", type=int,default=-1)  # number from head
    parser.add_argument("-T", "--tmpDir", default=None)  # None "_tmp_"
    parser.add_argument("-c", "--checkOrdering")  # check file order
    args = parser.parse_args()
    logger.info(args)
    if args.checkOrdering:
        src = args.input
        if not src:
            reader = sys.stdin
        else:
            if ' ' not in src:
                src = "cat " + src
            reader = os.popen(src)
        check(reader, args.checkOrdering)
    else:
        sortFile(args.input, args.output, sortType=args.sortType, unique=args.unique,head=args.number, budget=args.budget, tmpDir=args.tmpDir, nSplit=args.nSplit, nLine=args.nLine, buffering=args.buffering)
    # sortFile("cat bookcorpus.txt","sorted.txt")
    # check(open("sorted.txt"),"<=")


if __name__ == "__main__":
    main()
