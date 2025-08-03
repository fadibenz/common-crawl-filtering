import concurrent.futures
import os
import aiohttp, asyncio, aiofiles
import argparse
import random
import gzip
import logging, uuid
from argparse import Namespace

import fasttext
from pathlib import Path
from fastwarc.warc import ArchiveIterator, WarcRecordType

from data_filtering.utils import  setup_logging
from data_filtering.filtering_tokenization_scripts.utils import list_file_paths
from data_filtering.filtering_utilities.harmful_content import classify_harmful_content
from data_filtering.filtering_utilities.mask_pii import mask_pii
from data_filtering.filtering_utilities.extract_text import extract_text
from data_filtering.filtering_utilities.language_identification import language_identification
from data_filtering.filtering_utilities.normalizing_text import normalize_whitespace
from data_filtering.filtering_utilities.gopher_quality_filters import gopher_quality_filters

lang_m: fasttext.FastText  = None
nsfw_m: fasttext.FastText  = None
hate_m: fasttext.FastText  = None

def init_models(lang_path, nsfw_path, hate_path):
    global lang_m, nsfw_m, hate_m
    lang_m  = fasttext.load_model(lang_path)
    nsfw_m  = fasttext.load_model(nsfw_path)
    hate_m  = fasttext.load_model(hate_path)

def filter_one_file(compressed_file_path: str,
                    args: Namespace,
                    output_dir: Path):
    manifest = []
    total_records = 0
    kept_records = 0
    with gzip.open(compressed_file_path, "rt", encoding="utf-8", errors="replace") as stream:
        for record in ArchiveIterator(stream, record_types=WarcRecordType.response):
            total_records += 1

            if record.http_content_type and "text/html" not in record.http_content_type:
                continue

            try:
                record_bytes = record.reader.read()
                extracted_text = extract_text(record_bytes)

                extracted_text = normalize_whitespace(extracted_text)

                if not extracted_text.strip():
                    continue

                # Language check
                lang, confidence = language_identification(extracted_text, lang_m)
                if lang != args.lang or confidence < args.confidence:
                    continue

                if not gopher_quality_filters(extracted_text):
                    continue

                # Harmful content
                label_nsfw, score_nsfw = classify_harmful_content(extracted_text, nsfw_m)
                if not (label_nsfw == "non-nsfw" and score_nsfw >= args.nsfw_threshold):
                    continue

                label_hate, score_hate = classify_harmful_content(extracted_text, hate_m)
                if not (label_hate == "non-toxic" and score_hate >= args.hate_threshold):
                    continue
                kept_records += 1

                # PII masking
                masked_extracted_text, _ = mask_pii(extracted_text)

                uid = uuid.uuid4().hex
                doc_path = output_dir / f"{uid}.txt"

                doc_path.write_text(masked_extracted_text, encoding="utf-8")
                manifest.append(str(doc_path))

            except Exception as e:
                logging.warning(f"Failed to process record #{total_records}: {e}")

    logging.info(f"Finished one file. Processed: {total_records}, Kept: {kept_records} ({kept_records / total_records:.2%})")

async def process_one_file_async(session: aiohttp.ClientSession,
                                 url: str,
                                 args: Namespace,
                                 loop,
                                 process_pool) -> Path:
    output_dir = args.STAGE1_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    async with aiofiles.tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp_file:
        tmp_path = tmp_file.name
        async with session.get(url, stream=True) as resp:
            while True:
                chunk = await resp.content.read(args.CHUNK_MB)
                if not chunk:
                    break
                await tmp_file.write(chunk)
    try:
        manifest = await loop.run_in_executor(
            process_pool,
            filter_one_file,
            tmp_path,
            args,
            output_dir,
        )
    finally:
        os.unlink(tmp_path)

    manifest_path = output_dir / (Path(url).stem + ".manifest")
    await aiofiles.open(manifest_path, "w").write("\n".join(manifest))

    return manifest_path


async def main(urls, args):
    conn = aiohttp.TCPConnector(limit=args.concurrency, limit_per_host=args.concurrency)
    process_pool = concurrent.futures.ProcessPoolExecutor(
        max_workers=args.concurrency,
        initializer=init_models,
        initargs=(args.lang_model, args.nsfw_model, args.hatespeech_model)
    )
    loop = asyncio.get_running_loop()
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [process_one_file_async(session,
                                        u,
                                        args,
                                        loop,
                                        process_pool)
                 for u in urls]
        manifests = await asyncio.gather(*tasks)
        await loop.run_in_executor(None, process_pool.shutdown, True)
    logging.info(f"All files processed. Total manifests: {len(manifests)}")


def parse_args():
    parser = argparse.ArgumentParser(description="Pre-processing of CC files using Asyncio")

    parser.add_argument("--num_urls",
                        type=int,
                        default=5,
                        help="Number of CC samples")

    parser.add_argument("--STAGE1_DIR",
                        required=True,
                        type=str,
                        help="Stage 1 output directory")

    parser.add_argument("--use_wet",
                        action="store_true",
                        help="Whether to use WET files and skip manual extraction or not")

    parser.add_argument("--concurrency",
                        type=int,
                        default=16,
                        help="Concurrency number to use")

    parser.add_argument("--lang_model",
                        type=str,
                        default="classifier_models/fasttext_language_ID.bin",
                        help="Path to language model classifier")

    parser.add_argument("--nsfw_model",
                        type=str,
                        default="classifier_models/jigsaw_fasttext_bigrams_nsfw_final.bin",
                        help="Path to NSFW model classifier")

    parser.add_argument("--hatespeech_model",
                        type=str,
                        default="classifier_models/jigsaw_fasttext_bigrams_hatespeech_final.bin",
                        help="Path to hatespeech model classifier")

    parser.add_argument("--lang",
                        default="en",
                        type=str,
                        help="Language to filter for (e.g., 'en').")

    parser.add_argument("--confidence",
                        default=0.60,
                        type=float,
                        help="Minimum language confidence score to keep a document.")
    parser.add_argument("--nsfw_threshold",
                        default=0.95,
                        type=float,
                        help="Minimum non-nsfw score to keep a document.")

    parser.add_argument("--hate_threshold",
                        default=0.95,
                        type=float,
                        help="Minimum non-hate score to keep a document.")

    parser.add_argument("--seed",
                        default=2025,
                        type=int,
                        help="Seed to use for reproducibility")

    return parser.parse_args()

if __name__ == "__main__":

    setup_logging()
    args = parse_args()
    random.seed(args.seed)

    logging.info(f"Started Stage 1 pre-processing \n"
                 f"     Num files: {args.num_urls} {'WET' if args.use_wet else 'WARC'} files"
                 f"     Language: {args.lang}, confidence score: {args.confidence}"
                 f"     Output directory: {args.STAGE1_DIR}"
                 f"     Concurrency: {args.concurrency}")

    logging.info(f"Loading fastText models")

    urls = list_file_paths(args.num_urls, args.use_wet)
    asyncio.run(main(urls, args))