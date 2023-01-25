# use 24th data as eval, day 2 was the best
# for now use same red team labels as nethawk
import os
import networkx as nx
#import gzip
import json
from datetime import datetime
import dateutil.parser as parser
from constants import *

'''
TRACE layout: 
'''

def _parse_file_event(event: dict) -> dict:
    print(event)

def parse_event(event: dict) -> dict:
    """
    Parse a single optc event
    @param event: json event
    @return: dict of the sanitized important values for the record
    """
    obj_type = event['object'] + '_' + event['action']
    if 'FILE' in obj_type:
        return _parse_file_event(event)

# def gen_graph(eval_files):
#     for f in eval_files:
#         with gzip.open(f, 'rb') as f:
#             for line in f:
#                 line = json.loads(line)
#                 # if not 'NT AUTHORITY' in line['principal'] and line['principal'] != '':
#                 #     print(line)
#                 #unix_ts = int(datetime.fromisoformat(parser.parse(line['timestamp']).strftime('%Y-%m-%d %H:%M:%S')).timestamp())


#[os.path.join(EVAL_DAY1_PATH, f) for f in os.listdir(EVAL_DAY2_PATH)]
# gen_graph(eval_files)






trace_red_labels = []
with open('./groundtruth_threaTrace/trace.txt', 'r') as f:
    trace_red_labels = f.read().splitlines()

# Figure out the days of the labels for the trace data
all_trace_files = [os.path.join(TRACE_DATA_DIR, f) for f in os.listdir(TRACE_DATA_DIR)]
all_red_events = []

for f in all_trace_files:
    with open(f, 'r') as f:
        for line in f:
            line = json.loads(line)
            uuid = ''
            ts = 0
            if 'com.bbn.tc.schema.avro.cdm18.Event' in line['datum']:
                uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['uuid']
                ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
            # elif 'com.bbn.tc.schema.avro.cdm18.Subject' in line['datum']:
            #     uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.Subject']['uuid']
            #     ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
            # elif 'com.bbn.tc.schema.avro.cdm18.FileObject' in line['datum']:
            #     uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.FileObject']['uuid']
            #     ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
            # elif 'com.bbn.tc.schema.avro.cdm18.NetFlowObject' in line ['datum']:
            #     uuid = line['datum']['com.bbn.tc.schema.avro.cdm18.NetFlowObject']['uuid']
            #     print(line['datum']['com.bbn.tc.schema.avro.cdm18.NetFlowObject']['baseObject']['epoch'])
            #     ts = line['datum']['com.bbn.tc.schema.avro.cdm18.Event']['timestampNanos']
            
            if uuid in trace_red_labels:
                all_red_events.append(ts)

all_red_events = sorted(all_red_events)
print(len(all_red_events))
print(all_red_events[0], all_red_events[-1])