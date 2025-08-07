from data_filtering.filtering_utilities.filter_lists import BLACKLIST

def filter_lines(text: str,
                 min_words: int = 3,
                 max_chars_per_line: int = 500,
                 ) -> str:
    kept = []

    for line in text.splitlines():
        lw = line.strip().lower()
        words = lw.split()
        if len(words) < min_words:
            continue
        if len(line) > max_chars_per_line:
            continue

        if any(blk in lw for blk in BLACKLIST):
            continue

        if '<' in line or 'http://' in lw or 'https://' in lw:
            continue

        non_alpha = sum(1 for c in line if not c.isalnum() and not c.isspace())
        if non_alpha / max(1, len(words)) > 0.5:
            continue

        digits = sum(1 for c in line if c.isdigit())
        if digits / max(1, len(line)) > 0.2:
            continue

        non_ascii = sum(1 for c in line if ord(c) > 127)
        if non_ascii / max(1, len(line)) > 0.2:
            continue

        kept.append(line)
    return "\n".join(kept)
