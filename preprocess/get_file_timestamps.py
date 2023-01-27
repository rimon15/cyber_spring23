import multiprocessing as mp
import os
import sys
import numpy as np
import json
sys.path.append('../')
from constants import *

TIMESTAMP_FILE = '../data/timestamps.txt'

def init(l):
    global lock
    lock = l

def times_parallel(fname):
    max_time = 0
    min_time = sys.maxsize
    with open(fname, 'r') as f:
        for line in f:
            line = json.loads(line)
            if 'com.bbn.tc.schema.avro.cdm18.Event' in line['datum']:
                ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
                if ts > max_time:
                    max_time = ts
                if ts < min_time:
                    min_time = ts
    lock.acquire()
    with open(TIMESTAMP_FILE, 'a') as f:
        f.write(fname + '\t' + str(min_time) + '\t' + str(max_time) + '\n')
    lock.release()
    print('Wrote for file: ' + fname)
    #print(fname, '\t', min_time, '\t', max_time)
    return

all_trace_files = [os.path.join(TRACE_DATA_DIR, f) for f in os.listdir(TRACE_DATA_DIR)]
l = mp.Lock()
pool = mp.Pool(20, initializer=init, initargs=(l,))
# let's the the max and min timestamps FOR EACH TRACE file

print('Getting file timestamps...')
pool.map(times_parallel, all_trace_files)
pool.close()
pool.join()
print('Done getting file timestamps.')