import torch
from transformers import BertTokenizerFast, BertForMaskedLM
from torch import nn
import heapq
from tqdm import tqdm
from sklearn.metrics import recall_score, precision_score, f1_score
import pickle

K = 20

tokenizer = BertTokenizerFast.from_pretrained('wordpiece_tokenizer_benign')
model = BertForMaskedLM.from_pretrained('results/checkpoint-24000').to('cuda')

labels = []
eval_seqs = []

with open('data/preprocessed/eval.txt', 'r') as f:
    for line in f:
        eval_seqs.append(line.strip())
with open('data/trace_phishing_base/eval_walks.txt', 'r') as f:
    for line in f:
        labels.append(line.strip().split('\t')[-1])

loss_func = nn.CrossEntropyLoss()
results = []
for i, eval_seq in tqdm(enumerate(eval_seqs), total=len(eval_seqs)):
    cur_res = {}
    cur_probs = []
    tokenized_text = tokenizer.tokenize(eval_seq)
    for masked_idx in range(len(tokenized_text)):
        orig_token = tokenized_text[masked_idx]
        tokenized_text[masked_idx] = '[MASK]'
        indexed_tokens = tokenizer.convert_tokens_to_ids(tokenized_text)
        indexed_tokens = tokenizer.build_inputs_with_special_tokens(
            indexed_tokens)
        segments_ids = [0] * len(indexed_tokens)
        segments_tensors = torch.tensor([segments_ids]).to('cuda')
        tokens_tensor = torch.tensor([indexed_tokens]).to('cuda')

        model.eval()
        predictions = model(tokens_tensor, segments_tensors)[0]

        # calculate the error b/w the predicted token and the actual token
        # lanobert picks the top-6 error and avg it i think?
        # predicted_tensor = predictions[0, masked_idx + 1].unsqueeze(0)
        # orig_tensor = torch.tensor(
        #     [tokenizer.convert_tokens_to_ids(orig_token)])
        # print('loss', loss)

        # try the other metric: get predictive prob. of top-k
        # masked_idx + 1 because of the added [CLS] token at the start
        cur_prob = predictions[0, masked_idx + 1].softmax(dim=0)
        top_prob = torch.max(cur_prob).item()
        # top_prob_token = torch.argmax(cur_prob).item()
        cur_probs.append(top_prob)
    cur_res['label'] = labels[i]
    cur_res['probs'] = heapq.nlargest(K, cur_probs)
    cur_res['score'] = sum(cur_res['probs']) / K
    cur_res['seq'] = eval_seq
    results.append(cur_res)

y_true = []
y_pred = []
for res in results:
    y_true.append(int(res['label']))
    if res['score'] > 0.8:
        y_pred.append(0)
    else:
        y_pred.append(1)
print('recall', recall_score(y_true, y_pred))
print('precision', precision_score(y_true, y_pred))
print('f1', f1_score(y_true, y_pred))

with open('results/results_dict.pkl', 'wb') as f:
    pickle.dump(results, f)

# # top-6 number of log keys used in lanobert
# text = 'thunderbird [event_execute] thunderbird [event_clone] thunderbird [event_fork] thunderbird [event_close] /proc/NUM/fd'
# tokenized_text = tokenizer.tokenize(text)
# masked_idx = 2
# tokenized_text[masked_idx] = '[MASK]'

# indexed_tokens = tokenizer.convert_tokens_to_ids(tokenized_text)
# indexed_tokens = tokenizer.build_inputs_with_special_tokens(indexed_tokens)
# tokens_tensor = torch.tensor([indexed_tokens])

# model.eval()
# predictions = model(tokens_tensor)[0]

# predicted_index = torch.argmax(predictions[0, masked_idx]).item()
# predicted_token = tokenizer.convert_ids_to_tokens([predicted_index])
# print('predicted_token', predicted_token)
