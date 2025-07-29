from typing import Tuple
import re

EMAIL_RE = re.compile(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b')

PHONE_RE = re.compile(
    r'(?:\+?\d{1,3}[\s\-]?)?'            # optional country code
    r'(?:\(?\d{2,4}\)?[\s\-]?)?'         # optional area code, with or without ()
    r'\d{3,4}[\s\-]?\d{3,4}'             # local number
)

IPV4_RE = re.compile(
    r'\b(?:'                                     # word boundary for clean match
    r'(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.'     # 0-255.
    r'(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.'     # 0-255.
    r'(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.'     # 0-255.
    r'(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d))'       # 0-255
)

def mask_emails(text: str)-> Tuple[str, int]:
    text, count = re.subn(EMAIL_RE, "|||EMAIL_ADDRESS|||", text)
    return text, count

def mask_phone_numbers(text: str)-> Tuple[str, int]:
    text, count = re.subn(PHONE_RE, "|||PHONE_NUMBER|||", text)
    return text, count

def mask_ip_address(text: str)-> Tuple[str, int]:
    text, count = re.subn(IPV4_RE, "|||IP_ADDRESS|||", text)
    return text, count

def mask_pii(text: str) -> Tuple[str, dict]:
    counts = {}
    text, count = mask_emails(text)
    counts["email"] = count

    text, count = mask_phone_numbers(text)
    counts["phone_numbers"] = count

    text, count = mask_ip_address(text)
    counts["ip_address"] = count

    return text, counts