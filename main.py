import os
import argparse
import collections
from collections import defaultdict
from train import NodeSequenceTrain
from eval import NodeSequenceTest
import random
from transformers import BertTokenizerFast  # AutoTokenizer
from tokenizers import Tokenizer

MODEL_PARAMS = defaultdict(
    gpu_ids=[0],  # [0,1,2,3]

    train_batch_size=32,  # 16
    # vocab_size = 4339,
    learning_rate=3e-05,  # 2e-05

    dropout_keep_prob=0.8,
    num_epochs=1000,
    max_gradient_norm=5,
    adam_epsilon=1e-8,
    weight_decay=0.0,
    warmup_steps=0,
    optimizer_type="Adam",

    pad_idx=0,
    max_position_embeddings=512,
    num_hidden_layers=4,  # try 12 (bert base), or 24 (bert large)
    num_attention_heads=12,  # try 16 (bert large)
    bert_hidden_dim=768,  # try 1024 for bert large
    attention_probs_dropout_prob=0.1,
    layer_norm_eps=1e-12,
    mlm_prob=0.15,

    num_categories=3,

    max_sequence_len=512,
    save_dirpath='results/',
    save_every=5,

    random_seed=random.sample(range(1000, 10000), 1)[0],
    model_path='results/model_14_best.pt'
)

PARAMS_MAP = {
    "bert_base_4_layers": MODEL_PARAMS
}


def train_model(args, hparams):
  hparams = collections.namedtuple("HParams", sorted(hparams.keys()))(**hparams)
  model = NodeSequenceTrain(hparams)
  model.train()


def test_model(args, hparams):
  hparams = collections.namedtuple("HParams", sorted(hparams.keys()))(**hparams)
  model = NodeSequenceTest(hparams)
  model.test()


if __name__ == '__main__':
  arg_parser = argparse.ArgumentParser()
  arg_parser.add_argument("--model", dest="model", type=str, default="bert_base_4_layers", help="Model Name")
  arg_parser.add_argument("--root_dir", dest="root_dir", type=str,
                          default="/home/ykim/workspace/cyber_spring23/")
  arg_parser.add_argument("--data_dir", dest="data_dir", type=str,
                          default="data/",  # walk_benign.txt, entity_dict.pkl
                          help="training data")
  arg_parser.add_argument("--tokenizer_dir", dest="tokenizer_dir", type=str,
                          default="/home/ykim/workspace/cyber_spring23/embeddings/tokenizer/",
                          help="tokenizer path")
  arg_parser.add_argument("--kbert_checkpoint_path", dest="kbert_checkpoint_path", type=str,
                          default="kbert-base-uncased-pytorch_model.bin",
                          help="model save path")  # bert-base-uncased, bert-post-uncased
  arg_parser.add_argument("--gpu_ids", dest="gpu_ids", type=str,
                          help="gpu_ids", default="")
  arg_parser.add_argument("--mode", dest="mode", type=str, default="train", help="train or test")

  args = arg_parser.parse_args()
  os.environ["CUDA_VISIBLE_DEVICES"] = args.gpu_ids

  # vocab_size = len(AutoTokenizer.from_pretrained(args.tokenizer_dir))
  vocab_size = len(BertTokenizerFast.from_pretrained(args.tokenizer_dir))
  hparams = PARAMS_MAP[args.model]
  hparams["gpu_ids"] = list(range(len(args.gpu_ids.split(","))))
  hparams["root_dir"] = args.root_dir
  hparams["data_dir"] = args.data_dir
  hparams["kbert_checkpoint_path"] = args.kbert_checkpoint_path
  hparams["tokenizer_dir"] = args.tokenizer_dir
  hparams['vocab_size'] = vocab_size

  if args.mode == "train":
    train_model(args, hparams)
  elif args.mode == "test":
    test_model(args, hparams)
