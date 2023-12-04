import argparse
import gc
import math
import operator
import os
import random
import sys
import tempfile
from multiprocessing import Process, Queue

import logzero
import psutil
from logzero import logger

# logzero.loglevel(logzero.INFO)


def free():
    return psutil.virtual_memory().available // 1024**2


def _keyFn(x):
    return x


def sortArray(array, sortType, keyFn=_keyFn):
    if sortType == "R":
        random.shuffle(array)
    elif sortType == "i":
        array.sort(key=lambda x: keyFn(x))
    elif sortType == "d":
        array.sort(key=lambda x: keyFn(x), reverse=True)
    return array


def bisect(arr, pivot, cmp, keyFn):
    val = keyFn(pivot)
    l = -1
    r = len(arr)
    while r - l > 1:
        m = (l + r) // 2
        if cmp(keyFn(arr[m]), val):
            l = m
        else:
            r = m
    return r


class Block:
    def __init__(self, path, bucket, sortType, step, keyFn) -> None:
        self.path = path
        if not os.path.exists(path):
            os.makedirs(path)
        self.n_line = len(bucket)
        bucket = sortArray(bucket, sortType, keyFn)
        Nodes = []
        for i in range(0, len(bucket), step):
            batch = bucket[i : i + step]
            name = f"{self.path}/node-{i}"
            node = Node(name, batch)
            Nodes.append(node)
        self.Nodes = Nodes


class Node:
    def __init__(self, name, batch) -> None:
        self.name = name
        self.n_line = len(batch)
        self.head = batch[0]
        self.tail = batch[-1]
        if name == None:
            self.bucket = batch
        else:
            writer = open(name, mode="w", errors="ignore")
            writer.writelines(batch)
            writer.close()

    def catch(self):
        if self.name == None:
            return self.bucket
        bucket = open(self.name).readlines()
        os.unlink(self.name)
        return bucket


class BigSort:
    def __init__(self, sortType="i", unique=False, keyFn=_keyFn, nHead=-1, tmpDir=None, memory=0.5, chunk=1000000, part=10):
        self.nHead = nHead
        self.chunk = chunk
        self.part = part
        self.memory = memory
        self.unique = unique
        assert sortType in "idR"
        self.sortType = sortType
        if unique and sortType not in "id":
            exit("unique only when sortType in i/d")
        self.keyFn = keyFn
        self.tmpDir = tmpDir
        self.MEM = free()
        logger.info(f"free:{self.MEM}M  remain:{self.MEM*self.memory:.2f}M")
        self.n_readed = 0
        self.n_writed = 0

    def map(self, reader, folder):
        Nodes = []
        bucket = []
        total = 0
        n_block = 0
        for l in reader:
            bucket.append(l)
            if bucket and len(bucket) % self.chunk == 0:
                # logger.debug((usage, self.MEM * self.memory))
                if free() > self.MEM * self.memory:
                    continue
                total += len(bucket)
                block_dir = f"{folder}/block-{n_block}"
                step = math.ceil(max(len(bucket), self.chunk) / self.part)
                logger.info(f"map:{total} free:{free():.2f}M n_block:{n_block} bucket:{len(bucket)} --> {block_dir} step:{step} nodes:{len(bucket)/step:.2f}")
                block = Block(block_dir, bucket, self.sortType, step, self.keyFn)
                Nodes += block.Nodes
                bucket = []
                n_block += 1
                gc.collect()

        if bucket:
            bucket = sortArray(bucket, self.sortType)
            if total == 0:
                node = Node(None, bucket)
                Nodes.append(node)
                total += len(bucket)
            else:
                total += len(bucket)
                block_dir = f"{folder}/block-{n_block}"
                step = math.ceil(max(len(bucket), self.chunk) / self.part)
                logger.info(f"map:{total} free:{free():.2f}M n_block:{n_block} bucket:{len(bucket)} --> {block_dir} step:{step} nodes:{len(bucket)/step:.2f}")
                block = Block(block_dir, bucket, self.sortType, step, self.keyFn)
                Nodes += block.Nodes
        logger.info(f"map done!  {reader}  maped:{total}  -->  Nodes:{len(Nodes)} ")
        return Nodes

    def reduce(self, Nodes):
        queue = []
        n_read = 0
        n_write = 0
        if self.sortType == "R" and len(Nodes) > 1:
            random.shuffle(Nodes)
        for i, node in enumerate(Nodes):
            bucket = node.catch()
            queue += bucket
            pivot = Nodes[i + 1].head if i != len(Nodes) - 1 else None
            queue = sortArray(queue, self.sortType, self.keyFn)
            if pivot == None:
                idx = len(queue)
            elif self.sortType == "R":
                idx = len(queue) // self.part
            elif self.sortType == "i":
                idx = bisect(queue, pivot, cmp=lambda x, y: x <= y, keyFn=self.keyFn)
            elif self.sortType == "d":
                idx = bisect(queue, pivot, cmp=lambda x, y: x >= y, keyFn=self.keyFn)

            r1 = len(bucket)
            w1 = idx
            n_read += r1
            n_write += w1
            logger.info(f"Node:{i}/{len(Nodes)} n_read:{n_read} -- n_write:{n_write} queue:{len(queue)}  r1:{r1} -> w1:{w1}   {r1-w1} ")
            for j in range(idx):
                yield queue[j]
            queue = queue[idx:]
            gc.collect()
        logger.info(f"reduce done! n_read:{n_read} --> n_write:{n_write} ")

    def sort(self, reader, tmpDir):
        Nodes = self.map(reader, tmpDir)

        Nodes = [(self.keyFn(x.head), self.keyFn(x.tail), i, x) for i, x in enumerate(Nodes)]
        Nodes = sortArray(Nodes, self.sortType, _keyFn)
        Nodes = [x[-1] for x in Nodes]
        reciver = self.reduce(Nodes)

        last = None
        for x in reciver:
            self.n_readed += 1
            if self.n_writed >= self.nHead >= 0:
                break
            if self.unique and x == last:
                continue
            yield x
            last = x
            self.n_writed += 1
        logger.info(f" n_readed:{self.n_readed} n_writed:{self.n_writed}")


def bigsort(reader, writer, sortType="i", unique=False, keyFn=_keyFn, nHead=-1, tmpDir=None, memory=0.5, chunk=100000, part=10):
    temp_dir = tempfile.TemporaryDirectory(dir=tmpDir)
    sorter = BigSort(sortType=sortType, unique=unique, keyFn=keyFn, nHead=nHead, tmpDir=tmpDir, memory=memory, chunk=chunk, part=part)
    sorted = sorter.sort(reader, temp_dir.name)
    for x in sorted:
        writer.write(x)
    temp_dir.cleanup()


def sortFile(src=None, tgt=None, sortType="i", unique=False, keyFn=_keyFn, nHead=-1, tmpDir=None, memory=0.5, chunk=100000, part=10):
    if not src:
        reader = sys.stdin
    else:
        if " " not in src:
            src = "cat " + src
        reader = os.popen(src)
    if not tgt:
        writer = sys.stdout
    else:
        writer = open(tgt, "w")

    bigsort(reader, writer, sortType=sortType, unique=unique, keyFn=keyFn, nHead=nHead, tmpDir=tmpDir, memory=memory, chunk=chunk, part=part)
    if tgt:
        writer.close()


# https://docs.python.org/3/library/operator.html
OrderingFn = {
    "<": operator.le,
    "<=": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    ">": operator.gt,
}


def check(reader, Ordering="<=", keyFn=_keyFn):
    cmp = OrderingFn[Ordering]
    last = None
    i = -1
    for i, l in enumerate(reader):
        if last == None:
            continue
        if not cmp(last, keyFn(l)):
            return last, l, Ordering
            # return f"{last} {Ordering} {l}"
        last = keyFn(l)
    print(f"check {i+1} lines {Ordering} , ok!")
    return True


def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("src") # sys.stdin
    # parser.add_argument("tgt") # sys.stdout
    # parser.add_argument("-i", "--input", default="readme.md")
    parser.add_argument("-i", "--input", default=None)
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument("-m", "--memory", type=float, default=0.5, help="memory remain ratio")
    parser.add_argument("-p", "--part", type=int, default=10, help="number of part")
    parser.add_argument("-C", "--ChunkSize", type=int, default=10000, help="ChunkSize")
    parser.add_argument("-u", "--unique", default=False, help="remove repeat neighbor,work after sorted")
    parser.add_argument("-s", "--sortType", default="i", help=" one of  'i/d/R': increase descend random")
    parser.add_argument("-b", "--blanks", default=None, help="ignore-leading-blanks")
    parser.add_argument("-t", "--sep", default=None, help="seperator in a line")
    parser.add_argument("-k", "--key", default=None, help="sort by key; '3n,5'")
    parser.add_argument("-n", "--number", type=int, default=0, help="key as number instead of string")
    parser.add_argument("-g", "--get", type=int, default=-1, help="get first number lines , like head -n")
    parser.add_argument("-T", "--tmpDir", default=None, help="None _tmp_")
    parser.add_argument("-c", "--checkOrdering", help=" check file order; < > <= !=...")
    args = parser.parse_args()
    logger.info(args)

    def keyFn(l):
        if args.blanks:
            l = l.lstrip()
        if args.key == None:
            return l

        if args.sep == None:
            t = [l] + l.split()
        elif args.sep == "":
            t = [l] + list(l)
        else:
            t = [l] + l.split(args.sep)

        keys = []
        ks = args.key.split(",")
        for k in ks:
            i = 0
            idx = k.rstrip("n")
            if idx:
                i = int(idx)
            key = t[i]
            if k[-1] == "n":
                key = int(key)
            keys.append(key)

        return keys

    if args.checkOrdering:
        src = args.input
        if not src:
            reader = sys.stdin
        else:
            if " " not in src:
                src = "cat " + src
            reader = os.popen(src)
        check(reader, args.checkOrdering, keyFn=keyFn)
    else:
        sortFile(args.input, args.output, sortType=args.sortType, unique=args.unique, keyFn=keyFn, nHead=args.get, tmpDir=args.tmpDir, memory=args.memory, chunk=args.ChunkSize, part=args.part)
    # sortFile("cat bookcorpus.txt","sorted.txt")
    # check(open("sorted.txt"),"<=")


if __name__ == "__main__":
    main()
