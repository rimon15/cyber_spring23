from data.transform_data import NodeSequenceDataset
import numpy as np
import logging
from datetime import datetime
from tqdm import tqdm
import torch
from torch import nn, optim
from torch.utils.data import DataLoader, random_split
from model.kbert import KBERT
from transformers import DataCollatorForLanguageModeling, AutoTokenizer
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
import os


class NodeSequenceTest(object):
  def __init__(self, hparams):
    self.hparams = hparams
    self.tokenizer = AutoTokenizer.from_pretrained(self.hparams.tokenizer_dir)
    self._logger = logging.getLogger(__name__)

    np.random.seed(hparams.random_seed)
    torch.manual_seed(hparams.random_seed)
    torch.cuda.manual_seed_all(hparams.random_seed)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True

  def build_dataloader(self):
    self.dataset = NodeSequenceDataset(self.hparams, self.tokenizer)
    self.test_dataloader = DataLoader(
        self.dataset,
        batch_size=1,
        shuffle=False,
    )

  def load_model(self):
    self.model = KBERT(self.hparams)
    chkpt = torch.load(self.hparams.model_path)
    self.model.load_state_dict(chkpt['model_state_dict'])
    self.model.to(self.device)
    self.model.eval()

  def test(self):
    self.device = torch.device(
        "cuda:0" if torch.cuda.is_available() else "cpu")
    self.load_model()
    self.build_dataloader()
    tqdm_batch_iterator = tqdm(self.test_dataloader)
    for idx, test_batch in enumerate(tqdm_batch_iterator):
      seq_len = torch.count_nonzero(test_batch['attention_mask'])
      act_tokens = self.tokenizer.convert_ids_to_tokens(
          test_batch['input_ids'].view(self.hparams.max_sequence_len).tolist()[:seq_len])
      seq_prob = 1

      for masked_idx in range(1, seq_len - 1):
        masked_input = act_tokens[masked_idx]
        indexed_tokens = self.tokenizer.convert_tokens_to_ids(act_tokens)
        segments_ids = [0 for _ in range(seq_len)]
        tokens_tensor = torch.tensor([indexed_tokens])
        segments_tensor = torch.tensor([segments_ids])

        with torch.inference_mode():
          cur = {}
          cur['tokens_tensor'] = tokens_tensor
          cur['segments_tensor'] = segments_tensor
          _, preds = self.model(cur)
          idx = self.tokenizer.convert_tokens_to_ids(masked_input)
          cur_prob = preds[0, masked_idx][idx]
          seq_prob *= cur_prob

      total_seq_prob = np.power(seq_prob.cpu(), 1 / (seq_len - 3))
      if test_batch['red_label'] == '1':
        print('total_seq_prob', total_seq_prob)
        print("sequence tokens: ", act_tokens)
        print("label: ", test_batch['red_label'])
      # seq_len = torch.count_nonzero(test_batch['attention_mask'])
      # repeat_input = test_batch['input_ids'].repeat(seq_len - 2, 1)
      # repeat_attn = test_batch['attention_mask'].repeat(seq_len - 2, 1)
      # mask = torch.ones(test_batch['input_ids'].size(-1) - 1).diag(1)[:seq_len - 2]
      # masked_input = repeat_input.masked_fill(
      #     mask == 1, self.tokenizer.mask_token_id)
      # labels = repeat_input.masked_fill(
      #     masked_input != self.tokenizer.mask_token_id, -100)
      # with torch.inference_mode():
      #   test_batch['labels'] = labels
      #   test_batch['input_ids'] = masked_input
      #   test_batch['attention_mask'] = repeat_attn
      #   mlm_loss, _ = self.model(test_batch)

      # _, token_ids = self.model(test_batch)
      # pred_tokens = self.tokenizer.convert_ids_to_tokens(token_ids.tolist())
      # act_tokens = self.tokenizer.convert_ids_to_tokens(
      #     test_batch['input_ids'].view(self.hparams.max_sequence_len).tolist())
