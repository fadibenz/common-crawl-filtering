from pathlib import Path
import argparse
import logging
import json

import fasttext

from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import GZipStream, FileStream

from data_filtering.filtering_utilities.mask_pii import mask_pii
from data_filtering.utils import setup_logging
from data_filtering.filtering_utilities.extract_text import extract_text
from data_filtering.filtering_utilities.language_identification import language_identification
from data_filtering.filtering_utilities.normalizing_text import normalize_whitespace

def parse_args():
    parser = argparse.ArgumentParser(description="Extract text from .warc.gz HTML responses.")

    parser.add_argument("--input_file", required=True, type=str, help="Path to the WARC file")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")

    parser.add_argument("--langid_model", required=True, type=str, help="Path to fastText model")
    parser.add_argument("--filter_lang", action="store_true", help="Filter records based on language")
    parser.add_argument("--lang", default="en", type=str, help="Language to filter for (e.g., 'en').")
    parser.add_argument("--confidence", default=0.90, type=float, help="Minimum confidence score to keep a document.")


    return parser.parse_args()

if __name__ == "__main__":
    setup_logging()
    args = parse_args()

    logging.info(f"Loading fastText model from: {args.langid_model}")
    try:
        lang_model = fasttext.load_model(args.langid_model)
    except ValueError as e:
        logging.error(f"Failed to load fastText model. Make sure the path is correct. Error: {e}")
        exit(1)

    input_file_path = Path(args.input_file)
    if input_file_path.suffix == ".gz" and input_file_path.with_suffix("").suffix == ".warc":
        base_name = input_file_path.with_suffix("").with_suffix("").stem
    else:
        logging.error("Unexpected file extension format")
        exit(1)

    output_file = input_file_path.parent / f"{base_name}.jsonl"
    mode = "w" if args.overwrite else "a"

    if args.overwrite:
        logging.info(f"Overwriting output file: {output_file}")
    else:
        logging.info(f"Appending to output file: {output_file}")

    total_records = 0
    kept_records = 0
    logging.info(f"Processing WARC file: {input_file_path}")

    stream = GZipStream(FileStream(str(input_file_path), "rb"))
    with open(output_file, mode, encoding="utf-8", errors="replace") as f:
        for record in ArchiveIterator(stream, record_types=WarcRecordType.response):
            total_records +=1

            if record.http_content_type and "text/html" not in record.http_content_type:
                continue

            try:
                record_bytes = record.reader.read()
                extracted_text = extract_text(record_bytes)
                extracted_text = normalize_whitespace(extracted_text)
                if extracted_text.strip():
                    lang, confidence = language_identification(extracted_text, lang_model)
                    masked_extracted_text, counts = mask_pii(extracted_text)
                    output_data = {
                        "text": masked_extracted_text,
                        "lang": lang,
                        "confidence": round(confidence, 4),
                        "url": record.headers.get('WARC-Target-URI'),
                        "pii_counts": counts
                    }
                    if args.filter_lang:
                        if lang == args.lang and confidence >= args.confidence:
                            f.write(json.dumps(output_data, ensure_ascii=False) + '\n')
                            kept_records += 1
                    else:
                        f.write(json.dumps(output_data, ensure_ascii=False) + '\n')
                        kept_records += 1

            except Exception as e:
                logging.warning(f"Failed to process record #{total_records}: {e}")

    logging.info(f"Finished. Processed: {total_records}, Kept: {kept_records} ({kept_records/total_records:.2%})")
    logging.info(f"Output written to: {output_file}")