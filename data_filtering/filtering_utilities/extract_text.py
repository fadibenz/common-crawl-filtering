from pathlib import Path
import argparse
import logging

from resiliparse.parse.encoding import detect_encoding, bytes_to_str
from resiliparse.extract.html2text import extract_plain_text

from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import GZipStream, FileStream

from data_filtering.utils import setup_logging

def extract_text(html_bytes: bytes) -> str:
    try:
        decoded = bytes_to_str(html_bytes, detect_encoding(html_bytes))
        text = extract_plain_text(decoded, form_fields=True)
        return text
    except Exception as e:
        logging.warning(f"Text extraction failed: {e}")
        return ""

def parse_args():
    parser = argparse.ArgumentParser(description="Extract text from .warc.gz HTML responses.")
    parser.add_argument("--input_file", required=True, type=str, help="Path to the WARC file")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")
    return parser.parse_args()

if __name__ == "__main__":
    setup_logging()
    args = parse_args()

    input_file_path = Path(args.input_file)

    if input_file_path.suffix == ".gz" and input_file_path.with_suffix("").suffix == ".warc":
        base_name = input_file_path.with_suffix("").with_suffix("").stem
    else:
        raise ValueError("Unexpected file extension format")

    output_file = input_file_path.parent / f"{base_name}.txt"
    mode = "w" if args.overwrite else "a"
    if args.overwrite:
        logging.info(f"Overwriting output file: {output_file}")
    else:
        logging.info(f"Appending to output file: {output_file}")

    total_records = 0
    failed_records = 0
    logging.info(f"Processing WARC file: {input_file_path}")

    stream = GZipStream(FileStream(str(input_file_path), "rb"))
    with open(output_file, "a", encoding="utf-8", errors="replace") as f:
        for record in ArchiveIterator(stream, record_types=WarcRecordType.response):
            total_records +=1
            try:
                record_bytes = record.reader.read()
                extracted_text = extract_text(record_bytes)
                if extracted_text.strip():
                    f.write(extracted_text +  "\n\n<|endofdoc|>\n\n")
            except Exception as e:
                failed_records += 1
                logging.warning(f"Failed to process record #{total_records}: {e}")

    logging.info(f"Finished. Processed: {total_records}, Failed: {failed_records}")
    logging.info(f"Output written to: {output_file}")