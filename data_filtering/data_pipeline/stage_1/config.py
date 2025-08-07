import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Pre-processing of CC files using Asyncio")

    parser.add_argument("--num_urls", type=int, default=5,
                        help="Number of CC samples")
    parser.add_argument("--STAGE1_DIR", required=True, type=str,
                        help="Stage 1 output directory")
    parser.add_argument("--use_wet", action="store_true",
                        help="Whether to use WET files.")
    parser.add_argument("--concurrency", type=int, default=16,
                        help="Concurrency for downloads and processing.")
    parser.add_argument("--num_workers", type=int, default=16,
                        help="Num of workers for process loop")
    parser.add_argument("--CHUNK_MB", type=int, default=1024,
                        help="Chunk size for downloading files in MB.")
    parser.add_argument("--max_concurrent_downloads", type=int, default=16,
                        help="Max simultaneous downloads (and temp files) at any time")

    parser.add_argument("--lang_model", type=str,
                        default="classifier_models/fasttext_language_ID.bin")
    parser.add_argument("--nsfw_model", type=str,
                        default="classifier_models/jigsaw_fasttext_bigrams_nsfw_final.bin")
    parser.add_argument("--hatespeech_model", type=str,
                        default="classifier_models/jigsaw_fasttext_bigrams_hatespeech_final.bin")
    parser.add_argument("--quality_model", type=str,
                        default="classifier_models/quality_fasttext.ftz",
                        help= "Path to fasttext quality classifier")

    parser.add_argument("--lang", default="en", type=str,
                        help="Language to filter for.")
    parser.add_argument("--confidence", default=0.90, type=float,
                        help="Min language confidence score.")
    parser.add_argument("--nsfw_threshold", default=0.95, type=float,
                        help="Min non-nsfw score.")
    parser.add_argument("--hate_threshold", default=0.95, type=float,
                        help="Min non-hate score.")
    parser.add_argument("--quality_threshold", default=0.8, type=float,
                        help="Min quality score.")

    parser.add_argument("--min_words", type=int, default=6,
                        help="Minimum number of words to keep a line")

    parser.add_argument("--seed", default=2025, type=int,
                        help="Seed for reproducibility.")

    return parser.parse_args()