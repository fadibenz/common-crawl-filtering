import argparse
import logging

import fasttext
import os
from pathlib import Path
from data_filtering.utils import setup_logging

def parse_args():

    parser = argparse.ArgumentParser(description="Train a FastText text classifier.")
    parser.add_argument("--train_file", type=str, required=True,
                        help="Path to training data file.")
    parser.add_argument("--output", type=str, default="model.ftz",
                        help="Path to save trained model.")
    parser.add_argument("--lr", type=float, default=1.0,
                        help="Learning rate.")
    parser.add_argument("--epoch", type=int, default=25,
                        help="Number of training epochs.")
    parser.add_argument("--wordNgrams", type=int, default=1,
                        help="Max length of word n-grams.")
    parser.add_argument("--dim", type=int, default=100,
                        help="Size of word vectors.")
    parser.add_argument("--loss", type=str, choices=["softmax", "ova", "hs"],
                        default="softmax", help="Loss function.")
    parser.add_argument("--verbose", type=int, default=2,
                        help="Verbosity level.")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_logging()

    if not os.path.exists(args.train_file):
        logging.error(f"Training file not found: {args.train_file}")
        raise FileNotFoundError

    logging.info(f"Training FastText classifier on: {args.train_file}")
    model = fasttext.train_supervised(
        input=args.train_file,
        lr=args.lr,
        epoch=args.epoch,
        wordNgrams=args.wordNgrams,
        dim=args.dim,
        loss=args.loss,
        verbose=args.verbose
    )

    Path(args.output).mkdir(parents=True, exist_ok=True)
    model.save_model(args.output)
    logging.info(f"Model saved to: {args.output}")
