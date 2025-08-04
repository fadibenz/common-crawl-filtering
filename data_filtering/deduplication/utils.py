import string
import unicodedata
import re
import sqlite3
import os

def normalize(text:str) -> str:
    text = text.lower()

    text = unicodedata.normalize("NFD", text)
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = text.translate(str.maketrans("", "", string.punctuation))
    text = re.sub(r"\s+", " ", text).strip()

    return text

def setup_db_connection(db_path: str | os.PathLike, read_only: bool = False):
    if read_only:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro",
                               uri=True,
                               check_same_thread=False)
    else:
        conn = sqlite3.connect(db_path, isolation_level=None)

        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")

    return conn
