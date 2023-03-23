'''
The graph representation and DFS is modeled from SHADEWATCHER and WATSON:
https://github.com/jun-zeng/ShadeWatcher
https://jun-zeng.github.io/file/shadewatcher_paper.pdf
https://jun-zeng.github.io/file/watson_paper.pdf
'''
import argparse
import pickle
import json
import tqdm
import glob
import networkx as nx
import walker  # pip install graph-walker: https://github.com/kerighan/graph-walker
import os

VALID_EVENTS = {
    'event_execute': 0,
    'event_clone': 1,
    'event_fork': 2,
    'event_open': 3,
    'event_close': 4,
    'event_connect': 5,
    'event_unlink': 6,
    'event_read': 7,
    'event_write': 8,
    'event_recvmsg': 9,
    'event_sendmsg': 10,
    'event_rename': 11,
    'event_loadlibrary': 12,
    'event_create_object': 13,
    'event_update': 14,
}


def parse_edge(line_json: dict) -> tuple[dict, str]:
  '''
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
  '''
  if line_json['predicateObject'] and line_json['predicateObject']['com.bbn.tc.schema.avro.cdm18.UUID'] \
          and line_json['subject'] and line_json['subject']['com.bbn.tc.schema.avro.cdm18.UUID'] \
          and line_json['type']:
    ret = {}
    line_json['type'] = line_json['type'].lower()
    if line_json['type'] not in VALID_EVENTS:
      return None, None

    ret['timestamp'] = line_json['timestampNanos'] // 1000000000
    ret['subject'] = line_json['subject']['com.bbn.tc.schema.avro.cdm18.UUID']
    ret['object'] = line_json['predicateObject']['com.bbn.tc.schema.avro.cdm18.UUID']
    ret['type'] = line_json['type']
    ret['sequence'] = line_json['sequence']['long']
    uuid = line_json['uuid']
    return ret, uuid

  return None, None


def parse_process(line_json: dict) -> tuple[dict, str]:
  '''
  TRACE process schema:
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
  if line_json['cmdLine'] and line_json['cmdLine']['string']:
    ret = {}
    ret['cmdLine'] = line_json['cmdLine']['string']
    ret['pid'] = line_json['cid']
    if line_json['properties'] and line_json['properties']['map'] and line_json['properties']['map']['ppid']:
      ret['ppid'] = line_json['properties']['map']['ppid']
    else:
      ret['ppid'] = -1
    uuid = line_json['uuid']
    ret['type'] = 'process'

    return ret, uuid

  return None, None


def parse_file(line_json: dict) -> tuple[dict, str]:
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
  if line_json['baseObject'] and line_json['baseObject']['properties'] and \
          line_json['baseObject']['properties']['map'] and line_json['baseObject']['properties']['map']['path']:
    ret = {}
    ret['path'] = line_json['baseObject']['properties']['map']['path']
    uuid = line_json['uuid']
    ret['type'] = 'file'

    return ret, uuid

  return None, None


def parse_socket(line_json: dict) -> tuple[dict, str]:
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
  if line_json['remoteAddress'] and line_json['remotePort']:
    ret = {}
    ret['remoteAddress'] = line_json['remoteAddress']
    ret['remotePort'] = line_json['remotePort']
    uuid = line_json['uuid']
    ret['type'] = 'socket'

    return ret, uuid

  return None, None


def parse_trace_entities(fpath: str, all_entities: dict, red_labels: dict) -> None:
  benign_file = True
  if 'EVAL' in fpath:
    benign_file = False
  with open(fpath, "r") as f:
    print(f'Processing entities for file: {fpath}...')
    lines = f.readlines()
    for line in tqdm.tqdm(lines, total=len(lines)):
      line = json.loads(line)
      res = {}

      # Parse process
      if 'com.bbn.tc.schema.avro.cdm18.Subject' in line['datum']:
        res, uuid = parse_process(line['datum']['com.bbn.tc.schema.avro.cdm18.Subject'])
        if res:
          if benign_file:
            res['label'] = 0
          else:
            for p in red_labels['processes']:
              if p in res['cmdLine']:
                res['label'] = 1
                break

      # Parse file
      elif 'com.bbn.tc.schema.avro.cdm18.FileObject' in line['datum']:
        res, uuid = parse_file(line['datum']['com.bbn.tc.schema.avro.cdm18.FileObject'])
        if res:
          if benign_file:
            res['label'] = 0
          else:
            for f in red_labels['files']:
              if f in res['path']:
                res['label'] = 1
                break

      # Parse socket
      elif 'com.bbn.tc.schema.avro.cdm18.NetFlowObject' in line['datum']:
        res, uuid = parse_socket(line['datum']['com.bbn.tc.schema.avro.cdm18.NetFlowObject'])
        if res:
          if benign_file:
            res['label'] = 0
          else:
            for s in red_labels['remote_ips']:
              if s in res['remoteAddress']:
                res['label'] = 1
                break

      if res and not uuid in all_entities:
        all_entities[uuid] = res


def parse_trace_edges(fpath: str, all_entities: dict, all_edges: dict) -> None:
  with open(fpath, "r") as f:
    print(f'Processing edges for file: {fpath}...')
    lines = f.readlines()
    for line in tqdm.tqdm(lines, total=len(lines)):
      line = json.loads(line)

      if 'com.bbn.tc.schema.avro.cdm18.Event' in line['datum']:
        res, uuid = parse_edge(line['datum']['com.bbn.tc.schema.avro.cdm18.Event'])

        if res and res['object'] in all_entities and res['subject'] in all_entities:
          all_edges[uuid] = res


def reduce_noise_temp_files(entities: dict, edges: dict) -> None:
  print("Removing temporary files...")
  files_to_edges = {}
  files_to_edge_uuids = {}
  for uuid in edges:
    src_node = edges[uuid]['subject']
    dest_node = edges[uuid]['object']
    if entities[src_node]['type'] == 'file':
      if not src_node in files_to_edges:
        files_to_edges[src_node] = []
        files_to_edge_uuids[src_node] = []
      files_to_edges[src_node].append(edges[uuid])
      files_to_edge_uuids[src_node].append(uuid)
    if entities[dest_node]['type'] == 'file':
      if not dest_node in files_to_edges:
        files_to_edges[dest_node] = []
        files_to_edge_uuids[dest_node] = []
      files_to_edges[dest_node].append(edges[uuid])
      files_to_edge_uuids[dest_node].append(uuid)

  temp_files = []
  for uuid in tqdm.tqdm(files_to_edges, total=len(files_to_edges)):
    pids = set()
    sorted_edges = sorted(files_to_edges[uuid], key=lambda x: x['sequence'])
    if sorted_edges[0]['type'] == 'event_create_object' and sorted_edges[-1]['type'] == 'event_unlink':
      for edge in sorted_edges:
        src_node = edge['subject']
        dest_node = edge['object']
        if entities[src_node]['type'] == 'process':
          pids.add(entities[src_node]['pid'])
        if entities[dest_node]['type'] == 'process':
          pids.add(entities[dest_node]['pid'])
      if len(pids) == 1:
        temp_files.append(uuid)

  for uuid in temp_files:
    del entities[uuid]
    for edge_uuid in files_to_edge_uuids[uuid]:
      del edges[edge_uuid]

  print(f'Removed {len(temp_files)} temporary files')


def reduce_noise_cpr(entities: dict, edges: dict) -> None:
  pass


def gen_nx_graph(edges: dict) -> nx.DiGraph:
  g = nx.DiGraph()
  for e in edges:
    g.add_edge(edges[e]['subject'], edges[e]['object'], type=edges[e]['type'], sequence=edges[e]['sequence'])
  return g


def gen_dfs_walks(entities: dict, edges: dict) -> list[list[str]]:
  pass


def gen_random_walks(entities: dict, edges: dict) -> list[list[str]]:
  pass


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Parse JSON TRACE files")
  parser.add_argument('--trace_dir', type=str, help='The directory/file of the original TRACE json data')
  parser.add_argument('--output_dir', type=str, help='The output directory')
  parser.add_argument('--labels_file', type=str, help='The JSON file with the red labels for the scenario')
  parser.add_argument('--reduce_noise', type=str, help='noise reduction applied to graphs (NONE, TEMP_FILES, \
                      CPR, ALL)')
  args = parser.parse_args()

  # The labeling strategy is to get the red labels for the entities, then for each sequence, we can call
  # the sequence malicious if it contains an entity that is labeled as malicious
  red_labels = {}
  with open(args.labels_file, 'r') as f:
    red_labels = json.load(f)

  # First, load all entities from the files
  all_entities = {}
  files = glob.glob(args.trace_dir + '/*')
  for f in tqdm.tqdm(files, total=len(files)):
    parse_trace_entities(f, all_entities, red_labels)

  # Next, collect the edges from the graphs and generate the graphs
  all_benign_edges = {}
  all_eval_edges = {}
  for f in tqdm.tqdm(files, total=len(files)):
    if 'EVAL' in f:
      parse_trace_edges(f, all_entities, all_eval_edges)
    else:
      parse_trace_edges(f, all_entities, all_benign_edges)

  print("Total number of entities: ", len(all_entities))
  print("Total number of benign unreduced edges: ", len(all_benign_edges))
  print("Total number of eval unreduced edges: ", len(all_eval_edges))

  # Perform noise reduction on the graphs if specified
  if args.reduce_noise == 'ALL':
    reduce_noise_temp_files(all_entities, all_benign_edges)
    reduce_noise_temp_files(all_entities, all_eval_edges)
    reduce_noise_cpr(all_entities, all_benign_edges)
    reduce_noise_cpr(all_entities, all_eval_edges)
  elif args.reduce_noise == 'TEMP_FILES':
    reduce_noise_temp_files(all_entities, all_benign_edges)
    reduce_noise_temp_files(all_entities, all_eval_edges)
  elif args.reduce_noise == 'CPR':
    reduce_noise_cpr(all_entities, all_benign_edges)
    reduce_noise_cpr(all_entities, all_eval_edges)

  # Save the graphs
  print("Saving entities & graphs...")
  with open(os.path.join(args.output_dir, 'entities.pkl'), 'wb') as f:
    pickle.dump(all_entities, f)
  with open(os.path.join(args.output_dir, 'benign_graph.pkl'), 'wb') as f:
    pickle.dump(all_benign_edges, f)
  with open(os.path.join(args.output_dir, 'eval_graph.pkl'), 'wb') as f:
    pickle.dump(all_eval_edges, f)

  # Finally, run random walks or the modified DFS according to WATSON and get the generated sequences
  print("Generating sequences...")
  benign_graph = gen_nx_graph(all_benign_edges)
  eval_graph = gen_nx_graph(all_eval_edges)
  # get the isolated nodes from the graphs
