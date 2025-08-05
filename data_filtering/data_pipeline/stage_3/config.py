import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Pre-processing of CC files using Asyncio")

    parser.add_argument("--input_file",
                        required=True,
                        type=str,
                        help="Path to final pre-processed file")

    parser.add_argument("--STAGE3_DIR",
                        required=True,
                        type=str,
                        help="Stage 3 output directory")

    parser.add_argument("--num_docs",
                        type=int,
                        default=100,
                        help="Num of documents for each process at a time")

    parser.add_argument("--num_workers",
                        type=int,
                        default=8,
                        help="Num of workers for process loop")

    parser.add_argument("--seed",
                        default=2025,
                        type=int,
                        help="Seed for reproducibility.")

    return parser.parse_args()
