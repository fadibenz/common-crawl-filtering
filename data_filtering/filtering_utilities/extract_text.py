import logging

from resiliparse.parse.encoding import detect_encoding, bytes_to_str
from resiliparse.extract.html2text import extract_plain_text

def extract_text(html_bytes: bytes) -> str:
    try:
        decoded = bytes_to_str(html_bytes, detect_encoding(html_bytes))
        text = extract_plain_text(decoded, form_fields=True, main_content=True)
        return text
    except Exception as e:
        logging.warning(f"Text extraction failed: {e}")
        return ""

