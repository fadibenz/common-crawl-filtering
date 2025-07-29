import nltk
from nltk import word_tokenize

nltk.download('punkt_tab')

def gopher_quality_filters(text: str)-> bool:
    words = word_tokenize(text)

    if not words:
        return False

    length_words = len(words)

    # Contain less than 50 or more than 100,000 words.
    if length_words < 50 or length_words > 100_000:
        return False

    # Have a mean word length outside the range of 3 to 10 characters
    mean_word_length = sum(map(len, words)) / length_words

    if not (3<= mean_word_length <= 10):
        return False

    # Contain less than 80% of words with at least one alphabetic character.
    percentage_alpha = sum(any(c.isalpha() for c in word) for word in words) / length_words

    if percentage_alpha < 0.8:
        return False

    # Have more than 30% of lines ending with an ellipsis (“...”).
    lines = text.splitlines()
    if not lines:
        return False

    percentage_ellipsis = sum(line.strip().endswith("...") for line in lines) / len(lines)
    if percentage_ellipsis > 0.3:
        return False

    return True