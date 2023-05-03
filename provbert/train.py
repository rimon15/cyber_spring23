from torch.utils.data import Dataset
from transformers import BertTokenizerFast, BertConfig, BertForMaskedLM, \
    DataCollatorForLanguageModeling, Trainer, TrainingArguments


class ProvSeqDataset(Dataset):
    def __init__(self, in_file, tokenizer):
        super().__init__()
        self.data = []
        self.tokenizer = tokenizer

        with open(in_file, 'r') as f:
            for line in f:
                self.data.append(line.strip())

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.tokenizer(self.data[idx], padding='max_length',
                              max_length=512)


tokenizer = BertTokenizerFast.from_pretrained('wordpiece_tokenizer_benign')
model_config = BertConfig(vocab_size=len(tokenizer))
model = BertForMaskedLM(config=model_config)
train_dataset = ProvSeqDataset('data/preprocessed/benign.txt', tokenizer)

data_collator = DataCollatorForLanguageModeling(
    tokenizer=tokenizer, mlm=True, mlm_probability=0.2)
training_args = TrainingArguments(
    output_dir='./results', num_train_epochs=50, per_device_train_batch_size=32,
    logging_steps=500,
    save_steps=2000)
trainer = Trainer(model=model, data_collator=data_collator,
                  train_dataset=train_dataset, args=training_args)
trainer.train()
