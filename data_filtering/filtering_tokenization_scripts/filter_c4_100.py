import argparse
from pathlib import Path
import gzip, json
import re
import logging
from data_filtering.utils import setup_logging

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str)
    parser.add_argument("--output_dir", type=str)
    return parser.parse_args()

if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    output_file = Path(args.output_dir) / "pre_processed_positive.txt.gz"
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    Path()
    kept_records = 0
    _whitespace_re = re.compile(r"\s+")

    with gzip.open(output_file, "wt", encoding="utf-8", errors="ignore") as f_out, \
         open(args.input_file, "r", encoding="utf-8", errors="ignore") as f_in:
         try:
            for doc in f_in:
                text = json.loads(doc.strip())["text"]
                text = _whitespace_re.sub(" ", text).strip()
                f_out.write("__label__good " + text + "\n")
                kept_records += 1
         except Exception as e:
             logging.warning(f"Failed processing document {doc}: {e}!")
    logging.info(f"Kept {kept_records} records")