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


class NodeSequenceTrain(object):
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
    train_size = int(0.95 * len(self.dataset))
    dev_size = len(self.dataset) - train_size
    self.train_dataset, self.dev_dataset = random_split(
        self.dataset, [train_size, dev_size])
    collater = DataCollatorForLanguageModeling(
        tokenizer=self.tokenizer, mlm=True, mlm_probability=self.hparams.mlm_prob)
    self.train_dataloader = DataLoader(
        self.train_dataset,
        batch_size=self.hparams.train_batch_size,
        shuffle=True,
        collate_fn=collater
    )
    self.dev_dataloader = DataLoader(
        self.dev_dataset,
        batch_size=1,
        shuffle=False,
    )

  def build_model(self):
    self.model = KBERT(self.hparams)
    self.model = self.model.to(self.device)

    if -1 not in self.hparams.gpu_ids and len(self.hparams.gpu_ids) > 1:
      self.model = nn.DataParallel(self.model, self.hparams.gpu_ids)

    if self.hparams.optimizer_type == "Adam":
      self.optimizer = optim.Adam(
          self.model.parameters(), lr=self.hparams.learning_rate)

  def train(self):
    self.device = torch.device(
        "cuda:0" if torch.cuda.is_available() else "cpu")

    self.build_dataloader()
    self.build_model()

    start_time = datetime.now().strftime('%H:%M:%S')
    self._logger.info("Model training starts at %s" % start_time)

    total_loss = 0
    n_examples = 0
    prev_acc, prev_p, prev_r, prev_f, prev_perplexity = 0.0, 0.0, 0.0, 0.0, float("inf")
    for epoch in range(self.hparams.num_epochs):
      self.model.train()
      tqdm_batch_iterator = tqdm(self.train_dataloader)
      for idx, batch in enumerate(tqdm_batch_iterator):
        n_examples += 1
        # mlm_loss, cls_loss, _ = self.model(batch.to(self.device))
        mlm_loss, _ = self.model(batch.to(self.device))
        loss = mlm_loss  # + cls_loss
        total_loss += loss
        loss.backward()
        self.optimizer.step()
        #break
      print(
          " ** | Epoch {:03d} | Loss {:.4f} |".format(epoch + 1, total_loss / n_examples))
      if (epoch + 1) % self.hparams.save_every == 0:
        # acc, p, r, f, perplexity = self.validation()
        perplexity = self.validation()
        if perplexity < prev_perplexity:
          self._logger.info("Saving model...")
          torch.save({
              'epoch': epoch + 1,
              'model_state_dict': self.model.state_dict(),
              'optimizer_state_dict': self.optimizer.state_dict(),
              'loss': loss,
          }, os.path.join(self.hparams.root_dir + self.hparams.save_dirpath, "model_{}.pt".format(epoch)))
        # prev_acc, prev_p, prev_r, prev_f, prev_perplexity = acc, p, r, f, perplexity

  def validation(self):
    self.model.eval()
    pred = list()
    y = list()
    perplexity = 0
    n_batch = 0
    tqdm_batch_iterator = tqdm(self.dev_dataloader)
    for idx, dev_batch in enumerate(tqdm_batch_iterator):
      n_batch += 1
      seq_len = torch.count_nonzero(dev_batch['attention_mask'])
      repeat_input = dev_batch['input_ids'].repeat(seq_len - 2, 1)
      repeat_attn = dev_batch['attention_mask'].repeat(seq_len - 2, 1)
      mask = torch.ones(dev_batch['input_ids'].size(-1) - 1).diag(1)[:seq_len - 2]
      masked_input = repeat_input.masked_fill(
          mask == 1, self.tokenizer.mask_token_id)
      labels = repeat_input.masked_fill(
          masked_input != self.tokenizer.mask_token_id, -100)
      with torch.inference_mode():
        dev_batch['labels'] = labels
        dev_batch['input_ids'] = masked_input
        dev_batch['attention_mask'] = repeat_attn
        mlm_loss, _ = self.model(dev_batch)
        # mlm_loss, cls_loss, sep_logits = self.model(dev_batch)
        # pred += torch.squeeze(torch.argmax(
        #     torch.nn.functional.softmax(sep_logits, dim=1), dim=1)).tolist()
        # y += torch.squeeze(dev_batch['sep_labels'])[:sep_logits.size(0)].tolist()
        # assert len(pred) == len(y)
        perplexity += mlm_loss.item()
        # break
    perplexity = np.exp(mlm_loss.item())
    # acc = accuracy_score(y, pred)
    # p, r, f, _ = precision_recall_fscore_support(y, pred)
    # print('Acc: {}, Pre: {}, Rec: {}, F1: {}, Perpl: {}'
    #       .format(acc, p, r, f, perplexity))
    print(f'Loss: {mlm_loss.item()}, Perplexity: {perplexity}')
    return perplexity  # acc, p, r, f, perplexity
