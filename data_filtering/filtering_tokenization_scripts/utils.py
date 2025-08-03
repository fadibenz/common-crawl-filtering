from typing import List
import requests
import gzip
import random
from io import BytesIO
import argparse

BASE = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-18"

def parse_args():
    parser = argparse.ArgumentParser(description="Extract text from .warc.gz HTML responses.")

    parser.add_argument("--input_file", required=True, type=str, help="Path to the WARC file")
    parser.add_argument("--output_file", required=True, type=str, help="Path to ouput file")

    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")

    parser.add_argument("--lang_model", type=str, default="classifier_models/fasttext_language_ID.bin",
                        help="Path to language model classifier")
    parser.add_argument("--nsfw_model", type=str, default="classifier_models/jigsaw_fasttext_bigrams_nsfw_final.bin",
                        help="Path to NSFW model classifier")
    parser.add_argument("--hatespeech_model", type=str,
                        default="classifier_models/jigsaw_fasttext_bigrams_hatespeech_final.bin",
                        help="Path to hatespeech model classifier")

    parser.add_argument("--filter_lang", action="store_true", help="Filter records based on language")
    parser.add_argument("--lang", default="en", type=str, help="Language to filter for (e.g., 'en').")
    parser.add_argument("--confidence", default=0.60, type=float, help="Minimum confidence score to keep a document.")
    parser.add_argument("--good_quality", action="store_true", help="Good quality documents")
    parser.add_argument("--max_len", type=int, default=10000, help="Max5 length for each document")

    return parser.parse_args()


def list_file_paths(max_files: int = 5000,
                    is_wet: bool = False, ) -> List[str]:
    paths_url = "wet.paths.gz" if is_wet else "warc.paths.gz"
    idx_url = f"{BASE}/{paths_url}"
    response = requests.get(idx_url)

    with gzip.open(BytesIO(response.content), "rt", encoding="utf-8") as f:
        all_paths = f.read().splitlines()
    random.shuffle(all_paths)
    sampled_paths = all_paths[:max_files]

    return [f"https://data.commoncrawl.org/{p}"
            for p in sampled_paths]
