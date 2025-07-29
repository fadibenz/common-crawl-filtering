def normalize_whitespace(text: str) -> str:
    lines = (line.strip() for line in text.splitlines())
    return '\n'.join(line for line in lines if line)