import operator
import sys
import psutil
import os
import argparse
import random
import tempfile
import math
from logzero import logger

# logger.info()
import logging
logger = logging.getLogger()
logger.propagate = False
logger.handlers.clear()
logger.setLevel(level=logging.INFO)
# logger.setLevel(level=logging.ERROR)
# handler = logging.FileHandler("log.txt")
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s %(filename)s-%(lineno)d-%(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


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
        writer = open(name, mode='w', buffering=buffering)
        writer.writelines(batch)
        writer.close()

    def catch(self):
        bucket = open(self.name).readlines()
        os.unlink(self.name)
        return bucket


class BigSort:
    def __init__(self, sortType='i', unique=False, budget=0.8, nSplit=10, nLine=100000, buffering=1024*1024, tmpDir=None, sortFn=sortFn, splitFn=splitFn):
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

    def map(self, reader, folder):
        Nodes = []
        block = None
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
                if not block:
                    block_dir = f"{folder}/block-{n_block}"
                    block = Block(block_dir, bucket, self.sortType, self.nSplit, self.buffering, self.sortFn)
                    logger.info(f"{total} {len(bucket)} --> {block_dir}")
                    Nodes += block.Nodes
                bucket = []
                n_block += 1
                block = None

        if bucket:
            bucket = sortFn(bucket, self.sortType)
            if total == 0:
                return Nodes, bucket
            total += len(bucket)
            if not block:
                block_dir = f"{folder}/block-{n_block}"
                block = Block(block_dir, bucket, self.sortType, self.nSplit, self.buffering, self.sortFn)
                logger.info(f"{total} {len(bucket)} --> {block_dir}")
                Nodes += block.Nodes
        logger.info(f"map done!  {vars(reader)}  {total}  -->  {len(Nodes)} ")
        return Nodes, bucket

    def write(self, doc, writer, last=None):
        if not self.unique:
            writer.writelines(doc)
            return
        for x in doc:
            if x == last:
                continue
            writer.write(x)
            last = x
        return last

    def reduce(self, Nodes, writer, bucket):
        queue = []
        n_read = 0
        n_write = 0
        last = None

        if not Nodes:
            n_read=len(bucket)
            last = self.write(bucket, writer, last)
            logger.info(f"reduce done! {n_read} --> {n_write} {vars(writer)}")
            return

        Nodes = [(x.head, x) for x in Nodes]
        Nodes = self.sortFn(Nodes, self.sortType)
        for i, x in enumerate(Nodes):
            head, node = x
            bucket = node.catch()
            queue += bucket
            pivot = Nodes[i+1][1].head if i != len(Nodes)-1 else None
            queue = self.sortFn(queue, self.sortType)
            lines, queue = self.splitFn(queue, self.sortType, pivot, self.nSplit)
            last = self.write(lines, writer, last)
            r1 = len(bucket)
            w1 = len(lines)
            n_read += r1
            n_write += w1
            if i % self.nSplit == 0:
                logger.info(f"{i}/{len(Nodes)} {n_read} --{n_write} queue:{len(queue)}  {r1} -> {w1}   {r1-w1} ")

        if queue:
            pivot = None
            queue = self.sortFn(queue, self.sortType)
            lines, queue = self.splitFn(queue, self.sortType, pivot, self.nSplit)
            last = self.write(lines, writer, last)
            r1 = len(bucket)
            w1 = len(lines)
            n_read += r1
            n_write += w1
            logger.info(f"{i}/{len(Nodes)} {n_read} --{n_write}  {r1} -> {w1}   {r1-w1} ")

        logger.info(f"reduce done! {n_read} --> {n_write} {vars(writer)}")

    def sort(self, reader, writer):
        # import time
        # t0=time.time()
        temp_dir = tempfile.TemporaryDirectory(dir=self.tmpDir)
        Nodes, bucket = self.map(reader, temp_dir.name)
        # t1=time.time()
        self.reduce(Nodes, writer, bucket)
        # temp_dir.cleanup()
        # t2=time.time()
        # logger.info(f"map {t1-t0} reduce {t2-t1}")


def bigsort(reader, writer, sortType='i', unique=False, budget=0.8, nSplit=10, nLine=10000, tmpDir=None, sortFn=sortFn, splitFn=splitFn):
    sorter = BigSort(sortType=sortType, unique=unique, budget=budget, nSplit=nSplit, nLine=nLine, tmpDir=tmpDir, sortFn=sortFn, splitFn=splitFn)
    sorter.sort(reader, writer)
    writer.flush()


def sortFile(src=None, tgt=None, sortType='i', unique=False, budget=0.8, nSplit=10, nLine=10000, tmpDir=None, buffering=1024*1024):
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
    bigsort(reader, writer, sortType=sortType, unique=unique, budget=budget, nSplit=nSplit, nLine=nLine, tmpDir=tmpDir)
    writer.close()


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
    parser.add_argument("--budget", type=float, default=0.5)  # 0.8 memary budget in ratio
    parser.add_argument("-u", "--unique", default=False)  # only when sort
    parser.add_argument("-s", "--sortType", default="i")  # one of  'i/d/R': increase descend random
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
        sortFile(args.input, args.output, sortType=args.sortType, unique=args.unique, budget=args.budget, tmpDir=args.tmpDir, nSplit=args.nSplit, nLine=args.nLine, buffering=args.buffering)
    # sortFile("cat bookcorpus.txt","sorted.txt")
    # check(open("sorted.txt"),"<=")


if __name__ == "__main__":
    main()
