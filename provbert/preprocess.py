import pickle
import re

all_walks = []
ent_dict = {}
# ip_with_port_regex = r'\b(?:\d{1,3}\.){3}\d{1,3}:\d{1,5}\b'
ip_regex = r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
num_regex = r'\d+'
ip_replace = 'IP'
num_replace = 'NUM'

BENIGN_WALK_PATH = 'data/trace_phishing_base/benign_walks.txt'
EVAL_WALK_PATH = 'data/trace_phishing_base/eval_walks.txt'
BENIGN_OUT_PATH = 'data/preprocessed/benign.txt'
EVAL_OUT_PATH = 'data/preprocessed/eval.txt'

with open(EVAL_WALK_PATH, 'r') as f:
    for line in f:
        all_walks.append(line.strip())

with open('data/trace_phishing_base/entity_dict.pkl', 'rb') as f:
    ent_dict = pickle.load(f)

seqs = []
for w in all_walks:
    cur = ''
    for e in w.split('\t')[:-1]:
        if 'event' in e:
            cur += f' [{e}] '
        else:
            e_type = ent_dict[e]['type']
            if e_type == 'process':
                cur += ent_dict[e]['cmdLine'].split('/')[-1]
            elif e_type == 'file':
                cur += ent_dict[e]['path']
            else:
                cur += ent_dict[e]['remoteAddress']
    seqs.append(cur)

with open(EVAL_OUT_PATH, 'w') as f:
    for s in seqs:
        s = re.sub(ip_regex, ip_replace, s)
        s = re.sub(num_regex, num_replace, s)
        f.write(s + '\n')
