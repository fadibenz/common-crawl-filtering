import random, glob, logging
from tempfile import TemporaryDirectory
from pathlib import Path

from data_filtering.utils import setup_logging
from data_filtering.deduplication.exact_line_deduplication_parallel import exact_line_dedup_parallel
from data_filtering.deduplication.minhash_deduplication_parallel import minhash_deduplication_parallel
from data_filtering.data_pipeline.stage_2.config import parse_args


if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    random.seed(args.seed)

    input_list_path_exact = []
    manifests = glob.glob(f"{args.STAGE1_DIR}/*.manifest")

    for m in manifests:
        input_list_path_exact.extend(Path(m).read_text().splitlines())

    with TemporaryDirectory(prefix="dedup_") as tmp_root:
        tmp_exact_output = Path(tmp_root) / "exact_line"

        logging.info("Starting deduplication pipeline...")
        logging.info(f"Processing {len(input_list_path_exact)} files")
        logging.info(f"Args: {vars(args)}")

        logging.info(f"Started exact line deduplication")

        exact_line_dedup_parallel(
            input_list_path_exact,
            tmp_exact_output,
            args.num_workers
        )

        input_list_path_fuzzy = glob.glob(f"{tmp_exact_output}/*.txt")

        logging.info(f"Successfully finished exact line deduplication, "
                     f"retained {len(input_list_path_fuzzy)} files")

        logging.info("Starting fuzzy deduplication...")

        minhash_deduplication_parallel(
            input_list_path_fuzzy,
            args.num_hashes,
            args.num_bands,
            args.num_grams,
            args.jaccard_threshold,
            args.STAGE2_DIR,
            args.num_workers
        )