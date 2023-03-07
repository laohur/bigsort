# bigsort
sort or shuffle big file/stream

## usage
> pip install bigsort 

### shell

```shell
bigsort -i  readme.md -o sorted.txt  # default sort in increase 
cat readme.md |  bigsort --sortType=d --unique=1 > sorted.txt  # sort pipe, order in descend, unique
bigsort -i sorted.txt -c ">"  # check order
bigsort -i  readme.md --unique=1   | bigsort --sortType=R > sorted.txt   # unique and shufle 
seq 0  1123456789  | bigsort --sortType=d -T "./"  > sorted.txt  # just try sort 10^10 numbers
wc -l *.py | bigsort   -k 1n,2  -b 1 -t " "   # sort by key
```

### python

```python
import os
from bigsort import  bigsort, sortFile, check, bisect

# sort in file
sortFile("cat readme.md","sorted.txt")
check(open("sorted.txt"),"<=")

# sort in pipe
bigsort(os.popen("cat readme.md"),open("sorted.txt",'w'),unique=1,sortType="d")
check(open("sorted.txt"),">")
```

## [custom sort](demo.py)
