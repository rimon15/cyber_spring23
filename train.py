from data.transform_data import NodeSequenceDataset
import numpy as np
import logging
from datetime import datetime
from tqdm import tqdm
import torch
from torch import nn, optim
from torch.utils.data import DataLoader
from model.kbert import KBERT
from transformers import DataCollatorForLanguageModeling, AutoTokenizer

class NodeSequenceTrain(object):
  def __init__(self, hparams):
    self.hparams = hparams
    self._logger = logging.getLogger(__name__)

    np.random.seed(hparams.random_seed)
    torch.manual_seed(hparams.random_seed)
    torch.cuda.manual_seed_all(hparams.random_seed)
    torch.backends.cudnn.benchmark = False
    torch.backends.cudnn.deterministic = True

  def build_dataloader(self):
    self.tokenizer = AutoTokenizer.from_pretrained(self.hparams.tokenizer_dir)
    self.train_dataset = NodeSequenceDataset(self.hparams, self.tokenizer)
    collater = DataCollatorForLanguageModeling(
      tokenizer=self.tokenizer, mlm=True, mlm_probability=self.hparams.mlm_prob)

    self.train_dataloader = DataLoader(
      self.train_dataset,
      batch_size=self.hparams.train_batch_size,
      shuffle=True,
      collate_fn=collater
    )

  def build_model(self):
    self.model = KBERT(self.hparams)
    self.model = self.model.to(self.device)

    if -1 not in self.hparams.gpu_ids and len(self.hparams.gpu_ids) > 1:
      self.model = nn.DataParallel(self.model, self.hparams.gpu_ids)

    if self.hparams.optimizer_type == "Adam":
      self.optimizer = optim.Adam(self.model.parameters(), lr=self.hparams.learning_rate)


  def train(self):
    self.device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

    self.build_dataloader()
    self.build_model()

    start_time = datetime.now().strftime('%H:%M:%S')
    self._logger.info("Model training starts at %s" % start_time)

    total_loss = 0
    n_examples = 0
    for epoch in range(self.hparams.num_epochs):
      self.model.train()
      tqdm_batch_iterator = tqdm(self.train_dataloader)
      for idx, batch in enumerate(tqdm_batch_iterator):
        n_examples += 1
        loss = self.model(batch)
        total_loss += loss
        loss.backward()
        self.optimizer.step()
      print(" ** | Epoch {:03d} | Loss {:.4f} |".format(epoch + 1, total_loss / n_examples))
      