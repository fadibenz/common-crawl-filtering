import itertools
import random
from collections import defaultdict
from typing import Set, List, Tuple, Dict
import mmh3
import os
from pathlib import Path
from data_filtering.deduplication.utils import normalize

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


def lsh_candidates(signature_dict: Dict[str, List[int]],
                   num_bands:int
                   )-> Set[Tuple[str, str]]:

    potential_pairs: Dict[Tuple[int, ...], List[str]] = defaultdict(list)
    for doc, signature in signature_dict.items():
        band_size = len(signature) // num_bands

        for i in range(0, len(signature), band_size):
            band = tuple(signature[i: i + band_size])
            potential_pairs[band].append(doc)

    candidate_pairs_set = {
        combo
        for doc_list in potential_pairs.values()
        for combo in itertools.combinations(doc_list, 2)
    }

    return candidate_pairs_set


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



def minhash_deduplication(list_paths: List[str] | list[os.PathLike],
                          num_hashes: int,
                          num_bands: int,
                          num_grams: int,
                          jaccard_threshold: float,
                          output_directory: str | os.PathLike ):

    signature_dict: Dict[str, List[int]] = {}

    for path in list_paths:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        ngram_set = get_ngrams(text, num_grams)
        signature = compute_minhash_signature(ngram_set, num_hashes)
        signature_dict[str(path)] = signature

    candidate_pairs_set = lsh_candidates(signature_dict, num_bands)


    confirmed_pairs = {
        pair
        for pair in  candidate_pairs_set
        if compute_jaccard(pair, num_grams) >= jaccard_threshold
    }

    duplicate_random = build_clusters(confirmed_pairs)

    all_paths = set(str(p) for p in list_paths)
    clustered_paths = set().union(*confirmed_pairs)
    non_duplicates = all_paths - clustered_paths

    paths_to_write = list(non_duplicates) + duplicate_random

    output_dir = Path(output_directory)
    output_dir.mkdir(parents=True, exist_ok=True)

    for path in paths_to_write:
        path_name = Path(path).name
        output_path = output_dir / path_name
        with open(output_path, "w", encoding="utf-8") as f_output:
            with open(path, "r", encoding="utf-8") as f_input:
                text = f_input.read()
            f_output.write(text)