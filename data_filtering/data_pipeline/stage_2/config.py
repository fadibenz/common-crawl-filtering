import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Pre-processing of CC files using Asyncio")

    parser.add_argument("--STAGE1_DIR",
                        required=True,
                        type=str,
                        help="Path to list of paths directory")

    parser.add_argument("--STAGE2_DIR",
                        required=True,
                        type=str,
                        help="Stage 2 output directory")


    parser.add_argument("--num_workers",
                        type=int,
                        default=8,
                        help="Num of workers for process loop")

    parser.add_argument("--num_hashes",
                        type=int,
                        default=128,
                        help="Number of hashes to use in MinHash")

    parser.add_argument("--num_bands",
                        type=int,
                        default=16,
                        help="Number of bands to use in LSH")

    parser.add_argument("--num_grams",
                        type=int,
                        default=3,
                        help="Number of grams to use")

    parser.add_argument("--jaccard_threshold",
                        type=int,
                        default=0.8,
                        help="Threshold of jaccard threshold")

    parser.add_argument("--seed",
                        default=2025,
                        type=int,
                        help="Seed for reproducibility.")

    return parser.parse_args()