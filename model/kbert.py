import torch
import torch.nn as nn
from transformers import BertConfig, BertForMaskedLM


class KBERT(nn.Module):
  def __init__(self, hparams):
    super(KBERT, self).__init__()
    self.hparams = hparams
    config = BertConfig(
        vocab_size=self.hparams.vocab_size,
        hidden_size=self.hparams.bert_hidden_dim,
        num_hidden_layers=self.hparams.num_hidden_layers,
        num_attention_heads=self.hparams.num_attention_heads,
        max_position_embeddings=self.hparams.max_sequence_len
    )

    self.model = BertForMaskedLM(config)

    self.classification = nn.Sequential(
        nn.Dropout(p=1 - self.hparams.dropout_keep_prob),
        nn.Linear(self.hparams.bert_hidden_dim,
                  self.hparams.num_categories)
    )

    self.loss_fct = nn.CrossEntropyLoss()

  def forward(self, batch):
    outputs = self.model(
        input_ids=batch["input_ids"],
        attention_mask=batch["attention_mask"],
        output_hidden_states=True,
    )
    mlm_loss = self.loss_fct(outputs.logits.view(-1, self.hparams.vocab_size), batch["labels"].view(-1))
    bert_outputs = outputs.hidden_states[-1]

    cls_losses = []
    for idx, sep_pos in enumerate(batch["sep_pos"]):

      sep_pos_nonzero = sep_pos.nonzero().view(-1)

      sep_out = bert_outputs[idx, sep_pos_nonzero, :]
      sep_logits = self.classification(sep_out)
      sep_logits = sep_logits.squeeze(-1)

      target_id = batch["sep_labels"][idx]

      cls_loss = self.loss_fct(sep_logits, target_id)

      cls_losses.append(cls_loss)

    if len(cls_losses) == 0:
      cls_loss = torch.tensor(0).float().to(torch.cuda.current_device())
    else:
      cls_loss = torch.mean(torch.stack(cls_losses, dim=0), dim=-1)

    return mlm_loss + cls_loss
