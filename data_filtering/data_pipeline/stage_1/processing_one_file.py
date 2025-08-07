import logging
import uuid
from pathlib import Path

import fasttext
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc import GZipStream, FileStream
from argparse import Namespace
from typing import List

from resiliparse.parse.encoding import bytes_to_str

from data_filtering.filtering_utilities.filter_lines import filter_lines
from data_filtering.filtering_utilities.harmful_content import classify_harmful_content
from data_filtering.filtering_utilities.mask_pii import mask_pii
from data_filtering.filtering_utilities.extract_text import extract_text
from data_filtering.filtering_utilities.language_identification import language_identification
from data_filtering.filtering_utilities.normalizing_text import normalize_whitespace
from data_filtering.filtering_utilities.super_quality_filter import super_quality_filter

lang_m: fasttext.FastText = None
nsfw_m: fasttext.FastText = None
hate_m: fasttext.FastText = None
quality_m: fasttext.FastText = None


def init_models(lang_path: str | Path,
                nsfw_path: str | Path,
                hate_path:str | Path,
                quality_path:str | Path):
    global lang_m, nsfw_m, hate_m, quality_m

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(process)d] %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()]
    )

    logging.info(f"Loading models...")

    lang_m = fasttext.load_model(lang_path)
    nsfw_m = fasttext.load_model(nsfw_path)
    # hate_m = fasttext.load_model(hate_path)
    quality_m = fasttext.load_model(quality_path)

def filter_one_file(compressed_file_path: str,
                    args: Namespace,
                    output_dir: Path) -> List[str]:
    manifest = []
    total_records = 0
    kept_records = 0
    stream = GZipStream(FileStream(str(compressed_file_path), "rb"))
    record_type = WarcRecordType.response if not args.use_wet else WarcRecordType.conversion

    for record in ArchiveIterator(stream, record_types=record_type):
            total_records += 1

            if not args.use_wet:
                if record.http_content_type and "text/html" not in record.http_content_type:
                    continue

            try:
                record_bytes = record.reader.read()

                if not args.use_wet:
                    extracted_text = extract_text(record_bytes)
                else:
                    extracted_text= bytes_to_str(record_bytes)

                if not extracted_text.strip():
                    continue

                # Language check
                lang, confidence = language_identification(extracted_text, lang_m)
                if lang != args.lang or confidence < args.confidence:
                    continue

                if not super_quality_filter(extracted_text):
                    continue

                # Harmful content
                label_nsfw, score_nsfw = classify_harmful_content(extracted_text, nsfw_m)
                if not (label_nsfw == "non-nsfw" and score_nsfw >= args.nsfw_threshold):
                    continue

                # label_hate, score_hate = classify_harmful_content(extracted_text, hate_m)
                # if not (label_hate == "non-toxic" and score_hate >= args.hate_threshold):
                #     continue

                filtered_text = filter_lines(extracted_text, args.min_words)
                label_quality, score_quality = classify_harmful_content(filtered_text, quality_m)

                if not (label_quality == "good" and score_quality >= args.quality_threshold):
                    continue


                # PII masking
                masked_extracted_text, _ = mask_pii(filtered_text)
                normalized_text = normalize_whitespace(masked_extracted_text)
                kept_records += 1

                if not normalized_text:
                    continue

                uid = uuid.uuid4().hex
                doc_path = output_dir / f"{uid}.txt"

                doc_path.write_text(normalized_text, encoding="utf-8")
                manifest.append(str(f"{doc_path}"))

            except Exception as e:
                logging.warning(f"Failed to process record #{total_records}: {e}")

    logging.info(f"Finished one file. Processed: {total_records}, Kept: {kept_records} ({kept_records / total_records if total_records != 0 else 1:.2%})")
    return manifest
