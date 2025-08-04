import itertools
from collections import defaultdict
from typing import Set, List, Tuple, Dict
import os
from pathlib import Path
from data_filtering.deduplication.utils import get_ngrams, compute_minhash_signature, build_clusters, compute_jaccard


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