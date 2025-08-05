import os
import numpy as np
from tqdm import tqdm
from data_filtering.utils import setup_logging
from data_filtering.data_pipeline.stage_3.config import parse_args
from pathlib import Path
from transformers import AutoTokenizer
from concurrent.futures import ProcessPoolExecutor
import gzip

def _init():
    global tok
    tok = AutoTokenizer.from_pretrained("gpt2")

def tokenize_line_and_add_eos(line):
    return tok.encode(line) + [tok.eos_token_id]

if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    out_file = Path(args.STAGE3_DIR) / "tokenized_training.bin"
    Path(args.STAGE3_DIR).mkdir(parents=True, exist_ok=True)
    num_workers = args.num_workers or os.cpu_count() or 1

    with open(out_file, "wb") as f_out, \
        gzip.open(args.input_file, "rt", encoding="utf-8") as f_in, \
        ProcessPoolExecutor(max_workers=num_workers, initializer=_init) as exe:
            for token_list in tqdm(exe.map(tokenize_line_and_add_eos,
                                           f_in,
                                           chunksize=args.num_docs),
                                   desc="Tokenizing"):

                f_out.write(np.array(token_list, dtype=np.uint16).tobytes())