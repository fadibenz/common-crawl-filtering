import string
import unicodedata
import re
import sqlite3
import os
from typing import Set, List, Tuple, Dict
import mmh3
from collections import defaultdict
import random


def normalize(text:str) -> str:
    text = text.lower()

    text = unicodedata.normalize("NFD", text)
    text = "".join(char for char in text if unicodedata.category(char) != "Mn")
    text = "".join(ch for ch in text if not unicodedata.category(ch).startswith("C"))
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


def get_ngrams(text: str,
               n: int) -> Set[str]:
    normalized_text = normalize(text)
    word_list = normalized_text.split(" ")
    ngram_set = (set
        (" ".join(word for word in word_list[i: i + n])
         for i in range(len(word_list) - n + 1)
    ))

    return ngram_set

def compute_minhash_signature(ngrams_set: Set[str], num_hashes: int) -> List[int]:
    signature = []
    for seed in range(num_hashes):
        min_hash = min([mmh3.hash(ngram, seed) & 0xffffffff for ngram in ngrams_set])
        signature.append(min_hash)
    return signature

def compute_jaccard(pair: Tuple[str, str],
                    num_grams:int) -> float:
    path_1, path_2 = pair

    with open(path_1, "r", encoding="utf-8") as f:
        doc = f.read()
    ngram_set_1 = get_ngrams(doc, num_grams)

    with open(path_2, "r", encoding="utf-8") as f:
        doc = f.read()
    ngram_set_2 = get_ngrams(doc, num_grams)

    jaccard_index = len(ngram_set_1 & ngram_set_2) / len(ngram_set_1 | ngram_set_2)

    return jaccard_index


def build_clusters(confirmed_pairs: Set[Tuple[str, str]]
                   )-> List[str]:
    graph: Dict[str, Set[str]] = defaultdict(set)

    for pair in confirmed_pairs:
        graph[pair[0]].add(pair[1])
        graph[pair[1]].add(pair[0])

    visited_nodes = set()
    duplicate_list_random = []

    def dfs(node: str, visited: Set[str], local_cluster: List[str]):
        if node in visited:
            return
        visited.add(node)
        local_cluster.append(node)
        for neighbor in graph[node]:
            dfs(neighbor, visited, local_cluster)

    for node in graph.keys():
        if node not in visited_nodes:
            local_cluster = []
            dfs(node, visited_nodes, local_cluster)
            duplicate_list_random.append(random.choice(local_cluster))

    return duplicate_list_random

