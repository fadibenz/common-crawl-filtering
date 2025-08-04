from __future__ import annotations

import os
from typing import Any
import fasttext

from data_filtering.deduplication.exact_line_deduplication import exact_line_deduplication
from data_filtering.deduplication.minhash_deduplication_parallel import minhash_deduplication_parallel
from data_filtering.filtering_utilities.extract_text import extract_text
from data_filtering.filtering_utilities.gopher_quality_filters import gopher_quality_filters
from data_filtering.filtering_utilities.harmful_content import classify_harmful_content
from data_filtering.filtering_utilities.language_identification import language_identification
from data_filtering.filtering_utilities.mask_pii import mask_emails, mask_phone_numbers, mask_ip_address
from data_filtering.deduplication.minhash_deduplication import minhash_deduplication
from data_filtering.deduplication.exact_line_deduplication_parallel import exact_line_dedup_parallel

def run_extract_text_from_html_bytes(html_bytes: bytes) -> str | None:
    return extract_text(html_bytes)


def run_identify_language(text: str) -> tuple[Any, float]:
    model = fasttext.load_model("classifier_models/fasttext_language_ID.bin")
    return language_identification(text, model)

def run_mask_emails(text: str) -> tuple[str, int]:
    return mask_emails(text)

def run_mask_phone_numbers(text: str) -> tuple[str, int]:
    return mask_phone_numbers(text)

def run_mask_ips(text: str) -> tuple[str, int]:
    return mask_ip_address(text)

def run_classify_nsfw(text: str) -> tuple[Any, float]:
    model = fasttext.load_model("classifier_models/jigsaw_fasttext_bigrams_nsfw_final.bin")
    return classify_harmful_content(text, model)

def run_classify_toxic_speech(text: str) -> tuple[Any, float]:
    model = fasttext.load_model("classifier_models/jigsaw_fasttext_bigrams_hatespeech_final.bin")
    return classify_harmful_content(text, model)

def run_classify_quality(text: str) -> tuple[Any, float]:
    raise NotImplementedError


def run_gopher_quality_filter(text: str) -> bool:
    return gopher_quality_filters(text)


def run_exact_line_deduplication(
    input_files: list[os.PathLike], output_directory: os.PathLike
):
    return exact_line_dedup_parallel(input_files, output_directory)


def run_minhash_deduplication(
    input_files: list[os.PathLike],
    num_hashes: int,
    num_bands: int,
    ngrams: int,
    jaccard_threshold: float,
    output_directory: os.PathLike,
):
    return minhash_deduplication_parallel(input_files, num_hashes, num_bands, ngrams, jaccard_threshold, output_directory)
