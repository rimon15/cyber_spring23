from datasets import load_dataset
from transformers import BertTokenizerFast
from tokenizers import Tokenizer, models, trainers, pre_tokenizers, decoders, normalizers, processors

dataset = load_dataset('text', data_files='data/preprocessed/benign.txt')


def batch_iterator():
    for i in range(0, len(dataset), 1000):
        yield dataset['train'][i: i + 1000]["text"]


tokenizer = Tokenizer(models.WordPiece(unk_token='[UNK]'))
tokenizer.normalizer = normalizers.BertNormalizer()
tokenizer.pre_tokenizer = pre_tokenizers.BertPreTokenizer()

special_tokens = ["[UNK]", "[PAD]", "[CLS]", "[SEP]", "[MASK]",
                  "[event_fork]", "[event_mprotect]", "[event_update]", "[event_mmap]", "[event_loadlibrary]",
                  "[event_read]", "[event_modify_file_attributes]", "[event_exit]", "[event_clone]", "[event_truncate]",
                  "[event_execute]", "[event_sendmsg]", "[event_link]", "[event_unlink]", "[event_unit]",
                  "[event_recvmsg]", "[event_change_principal]", "[event_accept]", "[event_connect]", "[event_write]",
                  "[event_create_object]", "[event_open]", "[event_rename]", "[event_close]"]

tokenizer.add_special_tokens(special_tokens)
trainer = trainers.WordPieceTrainer(
    vocab_size=25000, special_tokens=special_tokens)

tokenizer.train_from_iterator(batch_iterator(), trainer=trainer)
tokenizer.decoder = decoders.WordPiece(prefix="##")
tokenizer.post_processor = processors.BertProcessing(
    ("[SEP]", tokenizer.token_to_id("[SEP]")),
    ("[CLS]", tokenizer.token_to_id("[CLS]")),
)

bert_tok = BertTokenizerFast(tokenizer_object=tokenizer)
bert_tok.save_pretrained('wordpiece_tokenizer_benign')
