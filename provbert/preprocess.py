import pickle
import re

all_walks = []
ent_dict = {}
ip_with_port_regex = r'\b(?:\d{1,3}\.){3}\d{1,3}:\d{1,5}\b'
num_regex = r'\d+'
ip_replace = 'IP'
num_replace = 'NUM'


with open('data/trace_phishing_base/benign_walks.txt', 'r') as f:
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

with open('data/preprocessed/benign.txt', 'w') as f:
    for s in seqs:
        s = re.sub(ip_with_port_regex, ip_replace, s)
        s = re.sub(num_regex, num_replace, s)
        f.write(s + '\n')
