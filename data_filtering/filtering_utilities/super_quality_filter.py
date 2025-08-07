import re
from nltk import word_tokenize
from data_filtering.filtering_utilities.gopher_quality_filters import gopher_quality_filters
from data_filtering.filtering_utilities.filter_lists import DOMAIN_KEYWORDS

HTML_TAG_RE = re.compile(r'<\/?[a-z][^>]*>', re.IGNORECASE)
URL_RE      = re.compile(r'https?://\S+')

def lexical_diversity_ok(words, min_diversity=0.3):
    return len(set(words)) / len(words) >= min_diversity

def no_html_noise(text):
    tokens = text.split()
    bad = sum(bool(HTML_TAG_RE.search(tok) or URL_RE.search(tok)) for tok in tokens)
    return bad / len(tokens) < 0.005

def punctuation_ratio_ok(text, max_ratio=0.2):
    total = len(text)
    if total == 0:
        return False
    punct = sum(1 for c in text if not c.isalnum() and not c.isspace())
    return (punct / total) <= max_ratio

def numeric_ratio_ok(words, max_ratio=0.1):
    nums = sum(1 for w in words if any(ch.isdigit() for ch in w))
    return nums / len(words) <= max_ratio

def domain_coherence_ok(text, min_hits=2):
    text_l = text.lower()
    hits = sum(1 for kw in DOMAIN_KEYWORDS if kw in text_l)
    return hits >= min_hits

def super_quality_filter(text):
    words = word_tokenize(text)
    if not (gopher_quality_filters(text) and
            lexical_diversity_ok(words) and
            no_html_noise(text) and
            punctuation_ratio_ok(text) and
            numeric_ratio_ok(words) and
            domain_coherence_ok(text)):
        return False
    return True