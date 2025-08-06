from typing import Set


def filter_lines(text: str,
                 min_chars:int,
                 blacklist_words: Set[str]
                 ) -> str:
    kept_lines = []

    for line in text.splitlines():
        if len(line) < min_chars:
            continue
        words = set(line.lower().split())
        if words & blacklist_words:
            continue
        kept_lines.append(line)

    return "\n".join(kept_lines)