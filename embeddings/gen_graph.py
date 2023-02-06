import sys
sys.path.append('../')

import os
import networkx as nx
import json
from constants import *
import multiprocessing as mp
from argparse import ArgumentParser
import tqdm
import pickle

class TraceFileGraph:
    '''
    Represents the graph for a single TRACE json file
    '''
    def __init__(self):
        self.hosts = set()
        self.entities = {}
        self.relations = {}
        self.num_entities = 0
        self.num_relations = 0

    def add_entity(self, entity):
        self.entities[entity.uuid] = entity
        self.hosts.add(entity.hostId)
        self.num_entities += 1

    def add_relation(self, relation):
        self.relations[relation.uuid] = relation
        self.hosts.add(relation.hostId)
        self.num_relations += 1


def init(l):
    global lock
    lock = l

def parse_event(event: dict) -> dict:
    """ 
    TRACE event schema:
        root
    |-- hostId: string (nullable = true)
    |-- location: struct (nullable = true)
    |    |-- long: long (nullable = true)
    |-- name: string (nullable = true)
    |-- parameters: string (nullable = true)
    |-- predicateObject: struct (nullable = true)
    |    |-- com.bbn.tc.schema.avro.cdm18.UUID: string (nullable = true)    
    |-- predicateObject2: struct (nullable = true)
    |    |-- com.bbn.tc.schema.avro.cdm18.UUID: string (nullable = true)
    |-- predicateObject2Path: struct (nullable = true)
    |    |-- string: string (nullable = true)
    |-- predicateObjectPath: struct (nullable = true)
    |    |-- string: string (nullable = true)
    |-- programPoint: string (nullable = true)
    |-- properties: struct (nullable = true)
    |    |-- map: struct (nullable = true)
    |    |    |-- flags: string (nullable = true)
    |    |    |-- mode: string (nullable = true)
    |    |    |-- operation: string (nullable = true)
    |    |    |-- opm: string (nullable = true)
    |    |    |-- protection: string (nullable = true)
    |-- sequence: struct (nullable = true)
    |    |-- long: long (nullable = true)
    |-- size: struct (nullable = true)
    |    |-- long: long (nullable = true)
    |-- subject: struct (nullable = true)
    |    |-- com.bbn.tc.schema.avro.cdm18.UUID: string (nullable = true)
    |-- threadId: struct (nullable = true)
    |    |-- int: long (nullable = true)
    |-- timestampNanos: long (nullable = true)
    |-- type: string (nullable = true)
    |-- uuid: string (nullable = true)
    """
    
    pass

# parse subject
def parse_process(process: dict) -> dict:
    '''
    TRACE subject schema:
    root
    |-- cid: long (nullable = true)
    |-- cmdLine: struct (nullable = true)
    |    |-- string: string (nullable = true)
    |-- count: struct (nullable = true)
    |    |-- int: long (nullable = true)
    |-- exportedLibraries: string (nullable = true)
    |-- hostId: string (nullable = true)
    |-- importedLibraries: string (nullable = true)
    |-- iteration: struct (nullable = true)
    |    |-- int: long (nullable = true)
    |-- localPrincipal: string (nullable = true)
    |-- parentSubject: struct (nullable = true)
    |    |-- com.bbn.tc.schema.avro.cdm18.UUID: string (nullable = true)
    |-- privilegeLevel: string (nullable = true)
    |-- properties: struct (nullable = true)
    |    |-- map: struct (nullable = true)
    |    |    |-- cwd: string (nullable = true)
    |    |    |-- name: string (nullable = true)
    |    |    |-- ppid: string (nullable = true)
    |    |    |-- seen time: string (nullable = true)
    |-- startTimestampNanos: long (nullable = true)
    |-- type: string (nullable = true)
    |-- unitId: struct (nullable = true)
    |    |-- int: long (nullable = true)
    |-- uuid: string (nullable = true)
    '''

    pass

def parse_file(file: dict) -> dict:
    '''
    TRACE file schema:
    root
    |-- baseObject: struct (nullable = true)
    |    |-- epoch: struct (nullable = true)
    |    |    |-- int: long (nullable = true)
    |    |-- hostId: string (nullable = true)
    |    |-- permission: struct (nullable = true)
    |    |    |-- com.bbn.tc.schema.avro.cdm18.SHORT: string (nullable = true)
    |    |-- properties: struct (nullable = true)
    |    |    |-- map: struct (nullable = true)
    |    |    |    |-- path: string (nullable = true)
    |-- fileDescriptor: string (nullable = true)
    |-- hashes: string (nullable = true)
    |-- localPrincipal: string (nullable = true)
    |-- peInfo: string (nullable = true)
    |-- size: string (nullable = true)
    |-- type: string (nullable = true)
    |-- uuid: string (nullable = true)
    '''

    pass

def parse_netflow(netflow: dict) -> dict:
    '''
    TRACE socket schema:
    root
    |-- baseObject: struct (nullable = true)
    |    |-- epoch: struct (nullable = true)
    |    |    |-- int: long (nullable = true)
    |    |-- hostId: string (nullable = true)
    |    |-- permission: string (nullable = true)
    |-- fileDescriptor: string (nullable = true)
    |-- ipProtocol: struct (nullable = true)
    |    |-- int: long (nullable = true)
    |-- localAddress: string (nullable = true)
    |-- localPort: long (nullable = true)
    |-- remoteAddress: string (nullable = true)
    |-- remotePort: long (nullable = true)
    |-- uuid: string (nullable = true)
    '''
    
    pass

def parse_json(fpath) -> None:
    '''
    Parse a file into a list of nodes and edges
    '''
    file_graph = TraceFileGraph()

    with open(fpath) as f:
        for line in f:
            is_relation = False
            line = json.loads(line)
            res = {}
            if 'com.bbn.tc.schema.avro.cdm18.Event' in line['datum']:
                res = parse_event(line['datum']['com.bbn.tc.schema.avro.cdm18.Event'])
                is_relation = True
            elif 'com.bbn.tc.schema.avro.cdm18.Subject' in line['datum']:
                res = parse_process (line['datum']['com.bbn.tc.schema.avro.cdm18.Subject'])
            elif 'com.bbn.tc.schema.avro.cdm18.FileObject' in line['datum']:
                res = parse_file(line['datum']['com.bbn.tc.schema.avro.cdm18.FileObject'])
            elif 'com.bbn.tc.schema.avro.cdm18.NetFlowObject' in line ['datum']:
                res = parse_netflow(line['datum']['com.bbn.tc.schema.avro.cdm18.NetFlowObject'])  
            
            # check for nodes and edges existing and stuff
            if is_relation:
                file_graph.add_relation(res)
            else:
                file_graph.add_entity(res)
    pickle.dump(file_graph, open('../data/file_graphs/' + os.path.basename(fpath) + '.pkl', 'wb'))
    

def gen_graph(pool: mp.Pool, trace_dir: str) -> None:
    all_files = [os.path.join(trace_dir, f) for f in os.listdir(trace_dir)]
    nodes_edges_lists = list(tqdm.tqdm(pool.imap(parse_json, all_files), total=len(all_files)))

if __name__ == '__main__':
    l = mp.Lock()
    pool = mp.Pool(10, initializer=init, initargs=(l,))
    parser = ArgumentParser()
    parser.add_argument('--files_dir', type=str)
    parser.add_argument('--start_time', type=int, default=1523318400)
    parser.add_argument('--end_time', type=int, default=1523404800)


# trace_red_labels = []
# with open('../groundtruth_threaTrace/trace.txt', 'r') as f:
#     trace_red_labels = f.read().splitlines()

# # Figure out the days of the labels for the trace data
# all_trace_files = [os.path.join(TRACE_DATA_DIR, f) for f in os.listdir(TRACE_DATA_DIR)]
# all_red_events = []

# for f in all_trace_files:
#     with open(f, 'r') as f:
#         for line in f:
#             line = json.loads(line)
#             print(line)
#             uuid = ''
#             ts = 0
#             if 'com.bbn.tc.schema.avro.cdm18.Event' in line['datum']:
#                 uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['uuid']
#                 ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
#             # elif 'com.bbn.tc.schema.avro.cdm18.Subject' in line['datum']:
#             #     uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.Subject']['uuid']
#             #     ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
#             # elif 'com.bbn.tc.schema.avro.cdm18.FileObject' in line['datum']:
#             #     uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.FileObject']['uuid']
#             #     ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
#             # elif 'com.bbn.tc.schema.avro.cdm18.NetFlowObject' in line ['datum']:
#             #     uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.NetFlowObject']['uuid']
#             #     print(line['datum']['com.bbn.tc.schema.avro.cdm18.NetFlowObject']['baseObject']['epoch'])
#             #     ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
            
#             if uuid in trace_red_labels:
#                 all_red_events.append(ts)

# all_red_events = sorted(all_red_events)
# print(len(all_red_events))
# print(all_red_events[0], all_red_events[-1])