from tokenizers import decoders, models, normalizers, pre_tokenizers, processors, trainers, Tokenizer, BertWordPieceTokenizer
from datasets import load_dataset
from transformers import AutoTokenizer, BertTokenizerFast
from argparse import ArgumentParser
import os

'''
Load/Test tokenizer
'''
# loaded_tokenizer = AutoTokenizer.from_pretrained("/home/ykim/workspace/cyber_spring23/embeddings/tokenizer/")
# print(loaded_tokenizer.convert_tokens_to_ids(loaded_tokenizer.tokenize("[CLS] [SEP] firefox [EVENT_LINK] [SEP] rm -r [EVENT_MMAP] [SEP]")))
# print(loaded_tokenizer.tokenize("[CLS] [SEP] firefox [EVENT_LINK] [SEP] rm -r [EVENT_MMAP] [SEP]"))


'''
Train BERT-like tokeinzer for TC dataset
'''
parser = ArgumentParser()
parser.add_argument("--root_dir", type=str, help='Path to cyber_spring23/ dir', required=True)
parser.add_argument("--raw_dir", type=str, help='Path to raw_text/ dir', required=True)
args = parser.parse_args()

batch_size = 1000

# "/home/ykim/workspace/cyber_spring23/preprocess/raw_text/"
data_path = args.raw_dir
# right now it is using the local address? We can try to use remote and see what to do...
dataset = load_dataset('text', data_files={'text': [
                       data_path + "file_raw_text.txt", data_path + "process_raw_text.txt", data_path + "socket_raw_text.txt"]})


def batch_iterator():
  for i in range(0, len(dataset), batch_size):
    yield dataset['text'][i: i + batch_size]["text"]


tokenizer = Tokenizer(models.WordPiece(unk_token="[UNK]"))  # typo unl_token -> unk_token?
tokenizer.normalizer = normalizers.BertNormalizer(lowercase=True)

tokenizer.pre_tokenizer = pre_tokenizers.BertPreTokenizer()
special_tokens = ["[UNK]", "[PAD]", "[CLS]", "[SEP]", "[MASK]"]

trainer = trainers.WordPieceTrainer(vocab_size=25000, special_tokens=special_tokens)
tokenizer.train_from_iterator(batch_iterator(), trainer=trainer)

new_tokens = ["[event_fork]", "[event_mprotect]", "[event_update]", "[event_mmap]", "[event_loadlibrary]",
              "[event_read]", "[event_modify_file_attributes]", "[event_exit]", "[event_clone]", "[event_truncate]",
              "[event_execute]", "[event_sendmsg]", "[event_link]", "[event_unlink]", "[event_unit]",
              "[event_recvmsg]", "[event_change_principal]", "[event_accept]", "[event_connect]", "[event_write]",
              "[event_create_object]", "[event_open]", "[event_rename]", "[event_close]"]

tokenizer.add_tokens(new_tokens)
tokenizer.decoder = decoders.WordPiece(prefix="##")


'''
Save tokenizer
'''
new_tokenizer = BertTokenizerFast(tokenizer_object=tokenizer)
# "/home/ykim/workspace/cyber_spring23/embeddings/tokenizer/")
new_tokenizer.save_pretrained(os.path.join(args.root_dir, 'embeddings/tokenizer'))


'''
OOV check
'''
cnt = 0
for i in range(0, len(dataset['text'])):
  encoding = new_tokenizer.tokenize(dataset['text'][i]["text"])
  if '[UNK]' in encoding:
    # print(encoding)
    # print(len(encoding))
    cnt += 1
  # print(encoding)
  # print(len(encoding))
print(cnt)


'''
Train BERT tokeinzer (not using this now)
'''
# import tokenizers

# tokenizer = tokenizers.BertWordPieceTokenizer(vocab=None)

# tokenizer.train(
#     files=[data_path + "new_process_raw.txt", data_path + "new_file_raw.txt", data_path + "new_socket_raw.txt"],
#     vocab_size=50000,
#     min_frequency=2,
#     limit_alphabet=100
# )

# tokenizer.save('/home/ykim/workspace/cyber_spring23/embeddings/tokenizer/', 'cmd-bert-uncased')
