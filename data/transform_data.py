import os
import re
import pickle
import torch
from torch.utils.data import Dataset
from transformers import PreTrainedTokenizerFast


class NodeSequenceDataset(Dataset):
  def __init__(self, hparams, tokenizer):
    super().__init__()
    self.hparams = hparams
    self.tokenizer = tokenizer
    self.inputs = []
    self.sep_category = {"process": 0, "file": 1, "socket": 2}

    with open(os.path.join(hparams.root_dir + hparams.data_dir, "entity_dict.pkl"), "rb") as pkl_handler:
      '''
      e.g., key: id, value: (info, type)
      6328899306454701882: ('/usr/share/doc/libfile-fcntllock-perl', 'file')
      '''
      self.entity_dict = pickle.load(pkl_handler)

    with open(os.path.join(hparams.root_dir + hparams.data_dir, "walk_benign.txt"), "r") as data_handler:
      '''
      e.g., id, relation, ... relation, id
      -3832575887008287552	EVENT_CREATE_OBJECT	-4185163237277691623	EVENT_FORK	-3442430275422800032	EVENT_EXECUTE	108003145666665265	EVENT_FORK	-2067566085360019418
      '''
      for line in data_handler:
        self.inputs.append(line)

  def __len__(self):
    return len(self.inputs)

  def __getitem__(self, index):
    example = self.inputs[index]
    feature = dict()
    input_ids, sep_pos, attention_mask, sep_labels = self.prepare_features(
        example)
    feature["input_ids"] = torch.tensor(input_ids).long()
    feature["sep_pos"] = torch.tensor(sep_pos).long()
    feature["attention_mask"] = torch.tensor(attention_mask).long()
    feature["sep_labels"] = torch.tensor(sep_labels).long()  # num of [sep]
    return feature

  def prepare_features(self, example):
    sep_labels = list()
    seqs = "[CLS] "
    elements = example.strip().split("\t")

    for e in elements:
      if e.isdigit() or e.lstrip('-').isdigit():
        seqs = seqs + self.entity_dict[int(e)][0] + " [SEP] "
        sep_labels.append(self.sep_category[self.entity_dict[int(e)][1]])
      else:
        seqs = seqs + "[" + e + "] "

    tokenized_seqs = self.tokenizer.tokenize(seqs)
    sep_pos = list()
    for _, tok in enumerate(tokenized_seqs):
      if tok == "[SEP]":
        sep_pos.append(1)
      else:
        sep_pos.append(0)

    attention_mask = [1] * len(tokenized_seqs)

    while len(tokenized_seqs) < self.hparams.max_sequence_len:
      tokenized_seqs.append("[PAD]")
      attention_mask.append(0)
      sep_pos.append(0)

    input_ids = self.tokenizer.convert_tokens_to_ids(tokenized_seqs)

    assert len(input_ids) == len(attention_mask)
    assert sep_pos.count(1) == len(sep_labels)
    assert len(input_ids) <= self.hparams.max_sequence_len

    return input_ids, sep_pos, attention_mask, sep_labels
