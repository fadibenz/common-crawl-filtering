import gzip
import logging
import uuid
from pathlib import Path

import fasttext
from fastwarc.warc import ArchiveIterator, WarcRecordType
from argparse import Namespace
from typing import List

from data_filtering.filtering_utilities.harmful_content import classify_harmful_content
from data_filtering.filtering_utilities.mask_pii import mask_pii
from data_filtering.filtering_utilities.extract_text import extract_text
from data_filtering.filtering_utilities.language_identification import language_identification
from data_filtering.filtering_utilities.normalizing_text import normalize_whitespace
from data_filtering.filtering_utilities.gopher_quality_filters import gopher_quality_filters


lang_m: fasttext.FastText = None
nsfw_m: fasttext.FastText = None
hate_m: fasttext.FastText = None

def init_models(lang_path: str | Path,
                nsfw_path: str | Path,
                hate_path:str | Path ):
    global lang_m, nsfw_m, hate_m
    logging.info(f"Loading models in process {uuid.uuid4().hex[:6]}...")
    lang_m = fasttext.load_model(lang_path)
    nsfw_m = fasttext.load_model(nsfw_path)
    hate_m = fasttext.load_model(hate_path)

def filter_one_file(compressed_file_path: str,
                    args: Namespace,
                    output_dir: Path) -> List[str]:
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
    return manifest
