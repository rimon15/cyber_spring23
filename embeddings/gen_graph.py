import sys
sys.path.append('../')

import os
import pandas as pd
import json
import multiprocessing as mp
from argparse import ArgumentParser
import tqdm
import pickle
from enum import Enum

class RelationTypes(Enum):
    EVENT_UNIT = 0
    EVENT_LINK = 1
    EVENT_MMAP = 2
    EVENT_WRITE = 3
    EVENT_CLOSE = 4
    EVENT_OPEN = 5
    EVENT_EXIT = 6
    EVENT_READ = 7
    EVENT_ACCEPT = 8
    EVENT_SENDMSG = 9
    EVENT_FORK = 10
    EVENT_UNLINK = 11
    EVENT_EXECUTE = 12
    EVENT_RECVMSG = 13
    EVENT_MODIFY_FILE = 14
    EVENT_CREATE_OBJECT = 16
    EVENT_RENAME = 17
    EVENT_CONNECT = 18
    EVENT_LOADLIBRARY = 19
    EVENT_TRUNCATE = 20
    EVENT_CLONE = 21
    EVENT_MPROTECT = 22
    EVENT_OTHER = 23
    EVENT_CHANGE_PRINCIPAL = 24
    EVENT_UPDATE = 25
    EVENT_UNKNOWN = 26

def init(p_l: mp.Lock, f_l: mp.Lock, s_l: mp.Lock, r_l: mp.Lock, u_l: mp.Lock, uuids: set):
    global proc_l
    global file_l
    global sock_l
    global rel_l
    global uuid_l
    global all_uuids
    proc_l = p_l
    file_l = f_l
    sock_l = s_l
    rel_l = r_l
    uuid_l = u_l
    all_uuids = uuids

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
        if entity != {}:
            self.entities[entity['uuid']] = entity
            self.hosts.add(entity['hostId'])
            self.num_entities += 1

    def add_relation(self, relation):
        if relation != {}:
            self.relations[relation['uuid']] = relation
            self.hosts.add(relation['hostId'])
            self.num_relations += 1

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
    if event['predicateObject'] and event['predicateObject']['com.bbn.tc.schema.avro.cdm18.UUID'] \
        and event['subject'] and event['subject']['com.bbn.tc.schema.avro.cdm18.UUID'] \
        and event['type']:

        ret = {}
        ret['hostId'] = event['hostId']
        ret['timestamp'] = event['timestampNanos'] // 1000000000
        ret['subject'] = event['subject']['com.bbn.tc.schema.avro.cdm18.UUID']
        ret['object'] = event['predicateObject']['com.bbn.tc.schema.avro.cdm18.UUID']
        ret['uuid'] = event['uuid']
        ret['type'] = event['type']
        return ret

    return None

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
    if process['cmdLine'] and process['cmdLine']['string']:
        ret = {}
        ret['hostId'] = process['hostId']
        ret['user'] = process['localPrincipal']
        ret['startTimestamp'] = process['startTimestampNanos'] // 1000000000
        if process['parentSubject'] and process['parentSubject']['com.bbn.tc.schema.avro.cdm18.UUID']:
            ret['parentProcess'] = process['parentSubject']['com.bbn.tc.schema.avro.cdm18.UUID']
        ret['cmdLine'] = process['cmdLine']['string']
        ret['ppid'] = process['properties']['map']['ppid']
        ret['uuid'] = process['uuid']

        return ret

    return None


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
    if file['baseObject'] and file['baseObject']['properties'] and file['baseObject']['properties']['map'] and file['baseObject']['properties']['map']['path']:
        ret = {}
        ret['hostId'] = file['baseObject']['hostId']
        ret['user'] = file['localPrincipal']
        ret['path'] = file['baseObject']['properties']['map']['path']
        ret['uuid'] = file['uuid']
        return ret

    return None

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
    if netflow['remoteAddress'] and netflow['remotePort']:
        ret = {}
        ret['hostId'] = netflow['baseObject']['hostId']
        ret['remoteAddress'] = netflow['remoteAddress']
        ret['remotePort'] = netflow['remotePort']
        ret['uuid'] = netflow['uuid']
        return ret

    return None

def parse_json(args: tuple) -> None:
    '''
    Parse a file into a list of nodes and edges
    '''
    file_graph = TraceFileGraph()
    fpath, output_dir = args

    with open(fpath) as f:
        for line in f:
            is_relation = False
            line = json.loads(line)
            res = {}
            # if 'com.bbn.tc.schema.avro.cdm18.Event' in line['datum']:
            #     res = parse_event(line['datum']['com.bbn.tc.schema.avro.cdm18.Event'])
            #     is_relation = True
            if 'com.bbn.tc.schema.avro.cdm18.Subject' in line['datum']:
                res = parse_process(line['datum']['com.bbn.tc.schema.avro.cdm18.Subject'])
            # elif 'com.bbn.tc.schema.avro.cdm18.FileObject' in line['datum']:
            #     res = parse_file(line['datum']['com.bbn.tc.schema.avro.cdm18.FileObject'])
            # elif 'com.bbn.tc.schema.avro.cdm18.NetFlowObject' in line ['datum']:
            #     res = parse_netflow(line['datum']['com.bbn.tc.schema.avro.cdm18.NetFlowObject'])  
            
            if res:
                # check for nodes and edges existing and stuff
                if is_relation:
                    file_graph.add_relation(res)
                else:
                    file_graph.add_entity(res)
    output_dir = os.path.join(output_dir, os.path.basename(fpath) + '_trace_file_graph.pkl')
    pickle.dump(file_graph, open(output_dir, 'wb'))

def gen_full(args: tuple) -> None:
    in_path, out_path = args
    global proc_l
    global file_l
    global sock_l
    global rel_l
    global uuid_l
    global all_uuids
    all_files_path = os.path.join(out_path, 'all_files.txt')
    all_sockets_path = os.path.join(out_path, 'all_sockets.txt')
    all_processes_path = os.path.join(out_path, 'all_processes.txt')
    all_relations_path = os.path.join(out_path, 'all_relations.txt')

    file_graph = pickle.load(open(in_path, 'rb'))
    for e in file_graph.entities.keys():
        with uuid_l:
            if e not in all_uuids:
                all_uuids.add(e)
                e = file_graph.entities[e]
                if 'remoteAddress' in e:
                    with sock_l:
                        with open(all_sockets_path, 'a+') as f:
                            f.write(str(e['uuid']) + '\t' + str(e['hostId']) + '\t' + str(e['remoteAddress']) + '\t' + str(e['remotePort']) + '\n')
                elif 'path' in e:
                    with file_l:
                        with open(all_files_path, 'a+') as f:
                            f.write(str(e['uuid']) + '\t' + str(e['hostId']) + '\t' + str(e['user']) + '\t' + str(e['path']) + '\n')
                else:
                    with proc_l:
                        with open(all_processes_path, 'a+') as f:
                            if 'parentProcess' not in e:
                                e['parentProcess'] = 'None'
                            f.write(str(e['uuid']) + '\t' + str(e['hostId']) + '\t' + str(e['user']) + '\t' + str(e['startTimestamp']) + '\t' + str(e['cmdLine']) + '\t' + str(e['parentProcess']) + '\n')

    for r in file_graph.relations.keys():
        with rel_l:
            with open(all_relations_path, 'a+') as f:
                r = file_graph.relations[r]
                f.write(str(r['uuid']) + '\t' + str(r['subject']) + '\t' + str(r['object']) + '\t' + str(r['type']) + '\t' + str(r['timestamp']) + '\t' + str(r['hostId']) + '\n')

'''
Stats:

Total events on the 10th: 56138733

'''
def gen_relations_timeslice(start_time, end_time, fname):
    in_base_dir = './full_trace_graphs'

    entity2id = {}
    with open(os.path.join(in_base_dir, 'entity2id.txt'), 'r') as f:
        for line in f:
            line = line.strip('\n')
            line = line.split('\t')
            entity2id[line[1]] = line[0]

    print('Done extracting entity2id')
    
    tot_correct = 0
    tot_keyerror = 0
    with open(os.path.join(in_base_dir, 'all_relations.txt')) as f:
        with open(os.path.join(in_base_dir, fname), 'w') as out_f:
            for line in f:
                line = line.split('\t')
                ts = line[4]
                if int(ts) >= start_time and int(ts) <= end_time:
                    rel_type = 26
                    for e in RelationTypes:
                        if e.name in line[3]:
                            rel_type = e.value
                    try:
                        out_f.write(str(entity2id[line[1]]) + '\t' + str(rel_type) + '\t' + str(entity2id[line[2]]) + '\t' + str(ts) + '\n')
                        tot_correct += 1
                    except KeyError as e:
                        #print(e)
                        # grep -rnw ./full_trace_graphs/ -e 'FC640DED-82C9-5B2C-7133-E2D3960051D8'
                        tot_keyerror += 1

    print('Done writing relations for timeslice: ', start_time, end_time)
    print('Total relations', tot_correct + tot_keyerror)
    print('Total correct: ', tot_correct)
    print('Total keyerror: ', tot_keyerror)

    # print(len(all_relations))
    # with open(os.path.join(in_base_dir, 'all_relations_10th.pkl'), 'wb') as f:
    #     pickle.dump(all_relations_10th, f)
    #df = pd.read_csv(os.path.join(in_base_dir, 'all_relations'))

def _write_entity2id(dir, fname, cur_idx) -> int:
    with open(os.path.join(dir, 'entity2id.txt'), 'a+') as f:
        with open(os.path.join(dir, fname), 'r') as cur_in_file:
            for line in cur_in_file:
                f.write(str(cur_idx) + '\t' + str(line).split('\t')[0] + '\n')
                cur_idx += 1

    return cur_idx

def dir_to_kg(dir='./full_trace_graphs'):
    cur_entity_count = 0

    with open(os.path.join(dir, 'relation2id.txt'), 'w') as f:
        for e in RelationTypes:
            f.write(str(e.value) + '\t' + e.name + '\n')

    for fname in ['all_files.txt', 'all_sockets.txt', 'all_processes.txt']:
        cur_entity_count = _write_entity2id(dir, fname, cur_entity_count)
    print('Done writing entity2id.txt and relation2id.txt')
    # with open(os.path.join(dir, 'entity2id.txt')) as f:
    #     cur_idx = 0
    #     # first processes, then files, then sockets
    #     with open(os.path.join(dir, 'all_processes.txt'), 'rb') as procs_file:
    #         for line in procs_file:

    #         #f.write()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--mode', type=str)
    parser.add_argument('--files_dir', type=str, default='/home/ead/datasets/trace/')
    parser.add_argument('--trace_file_graphs_dir', type=str, default='/home/ead/rmelamed/spring23_trace_processing/trace_file_graphs_processes')
    parser.add_argument('--out_base_dir', type=str, default='/home/ead/rmelamed/spring23_trace_processing')
    parser.add_argument('--start_time', type=int, default=1523318400)
    parser.add_argument('--end_time', type=int, default=1523404800)
    parser.add_argument('--slice_fname', type=str, default='kg_10th.txt')
    args = parser.parse_args()

    if args.mode == 'parse_json':
        pool = mp.Pool(40)
        all_files = [os.path.join(args.files_dir, f) for f in os.listdir(args.files_dir)]

        out_dir = os.path.join(args.out_base_dir, 'trace_file_graphs_processes')
        args_tuple = [(f, out_dir) for f in all_files]
        list(tqdm.tqdm(pool.imap(parse_json, args_tuple), total=len(all_files)))
        print('Done generating TraceFileGraph for all json files!')
    elif args.mode == 'gen_full': # generate the full graph for all days/files
        p_l = mp.Lock()
        s_l = mp.Lock()
        f_l = mp.Lock()
        r_l = mp.Lock()
        u_l = mp.Lock()
        uuids = set()
        pool = mp.Pool(40, initializer=init, initargs=(p_l, f_l, s_l, r_l, u_l, uuids))
        out_dir = os.path.join(args.out_base_dir, 'full_trace_graphs') 
        all_files = [os.path.join(args.trace_file_graphs_dir, f) for f in os.listdir(args.trace_file_graphs_dir)]

        args_tuple = [(f, out_dir) for f in all_files]
        list(tqdm.tqdm(pool.imap(gen_full, args_tuple), total=len(all_files)))
        #gen_full(all_files, out_dir)
        print('Done generating full graph for all json files for all days!')
        #args_tuple = [(f, out_dir) for f in all_files]

        #pickle.dump(G, open(os.path.join(out_dir, 'full_graph.pkl'), 'wb'))

    elif args.mode == 'gen_timeslice':
        gen_relations_timeslice(args.start_time, args.end_time, args.slice_fname)
    
    elif args.mode == 'to_kg':
        dir_to_kg()


    #parser.add_argument('--start_time', type=int, default=1523318400)
    #parser.add_argument('--end_time', type=int, default=1523404800)


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