from pathlib import Path
import argparse
import logging
import gzip
import fasttext

from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import GZipStream, FileStream

from data_filtering.filtering_utilities.harmful_content import classify_harmful_content
from data_filtering.filtering_utilities.mask_pii import mask_pii
from data_filtering.utils import setup_logging
from data_filtering.filtering_utilities.extract_text import extract_text
from data_filtering.filtering_utilities.language_identification import language_identification
from data_filtering.filtering_utilities.normalizing_text import normalize_whitespace
from data_filtering.filtering_utilities.gopher_quality_filters import gopher_quality_filters

def parse_args():
    parser = argparse.ArgumentParser(description="Extract text from .warc.gz HTML responses.")

    parser.add_argument("--input_file", required=True, type=str, help="Path to the WARC file")
    parser.add_argument("--output_file", required=True, type=str, help="Path to ouput file")

    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")

    parser.add_argument("--lang_model", type=str, default="classifier_models/fasttext_language_ID.bin",  help= "Path to language model classifier")
    parser.add_argument("--nsfw_model", type=str, default="classifier_models/jigsaw_fasttext_bigrams_nsfw_final.bin",help= "Path to NSFW model classifier")
    parser.add_argument("--hatespeech_model", type=str, default="classifier_models/jigsaw_fasttext_bigrams_hatespeech_final.bin",help= "Path to hatespeech model classifier")

    parser.add_argument("--filter_lang", action="store_true", help="Filter records based on language")
    parser.add_argument("--lang", default="en", type=str, help="Language to filter for (e.g., 'en').")
    parser.add_argument("--confidence", default=0.60, type=float, help="Minimum confidence score to keep a document.")
    parser.add_argument("--good_quality", action="store_true", help="Good quality documents")
    parser.add_argument("--max_len", type=int, default=50000, help="Max5 length for each document")

    return parser.parse_args()

if __name__ == "__main__":
    setup_logging()
    args = parse_args()

    logging.info(f"Loading fastText models")

    lang_model = fasttext.load_model(args.lang_model)
    nsfw_model = fasttext.load_model(args.nsfw_model)
    hatespeech_model = fasttext.load_model(args.hatespeech_model)

    input_file_path = Path(args.input_file)
    output_file = Path(args.output_file)
    if not output_file.suffix == ".gz":
        output_file = Path(str(output_file) + ".gz")

    mode = "w" if args.overwrite else "a"

    if args.overwrite:
        logging.info(f"Overwriting output file: {output_file}")
    else:
        logging.info(f"Appending to output file: {output_file}")

    total_records = 0
    kept_records = 0
    logging.info(f"Processing WARC file: {input_file_path}")
    if input_file_path.suffix == ".gz":
        stream = GZipStream(FileStream(str(input_file_path), "rb"))
    else:
        stream = FileStream(str(input_file_path), "rb")

    with gzip.open(output_file, mode + "t", encoding="utf-8", errors="replace") as f:
        for record in ArchiveIterator(stream, record_types=WarcRecordType.response):
            total_records +=1

            if record.http_content_type and "text/html" not in record.http_content_type:
                continue

            try:
                record_bytes = record.reader.read()
                extracted_text = extract_text(record_bytes)
                extracted_text = normalize_whitespace(extracted_text)

                if not extracted_text.strip():
                    continue

                # Language check
                lang, confidence = language_identification(extracted_text, lang_model)
                if args.filter_lang and (lang != args.lang or confidence < args.confidence):
                    continue

                if args.good_quality:
                    # Quality check
                    if not gopher_quality_filters(extracted_text):
                        continue

                    # Harmful content
                    label_nsfw, score_nsfw = classify_harmful_content(extracted_text, nsfw_model)
                    if not (label_nsfw == "non-nsfw" and score_nsfw >= 0.95):
                        continue

                    label_hate, score_hate = classify_harmful_content(extracted_text, hatespeech_model)
                    if not (label_hate == "non-toxic" and score_hate >= 0.95):
                        continue

                # PII masking
                masked_extracted_text, counts = mask_pii(extracted_text)
                normalized_text = masked_extracted_text.replace("\n", " ")
                normalized_text = normalized_text[:args.max_len]
                label = "__label__bad" if not args.good_quality else "__label__good"

                f.write(f"{label} {normalized_text}\n")
                kept_records += 1

            except Exception as e:
                logging.warning(f"Failed to process record #{total_records}: {e}")

    logging.info(f"Finished. Processed: {total_records}, Kept: {kept_records} ({kept_records/total_records:.2%})")
    logging.info(f"Output written to: {output_file}")