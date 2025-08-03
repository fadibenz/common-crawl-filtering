import gzip
import logging, uuid
from argparse import Namespace
from io import BytesIO

import fasttext
from envs.NLP.Lib.typing import TextIO
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import GZipStream, FileStream
from pathlib import Path
from typing import List

from data_filtering.filtering_utilities.harmful_content import classify_harmful_content
from data_filtering.filtering_utilities.mask_pii import mask_pii
from data_filtering.filtering_utilities.extract_text import extract_text
from data_filtering.filtering_utilities.language_identification import language_identification
from data_filtering.filtering_utilities.normalizing_text import normalize_whitespace
from data_filtering.filtering_utilities.gopher_quality_filters import gopher_quality_filters

