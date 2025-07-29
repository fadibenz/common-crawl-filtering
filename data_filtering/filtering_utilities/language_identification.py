import fasttext
from typing import Tuple

def language_identification(text: str,
                            model) -> Tuple[str, float]:
    cleaned_text = text.replace('\n', ' ')
    label, proba = model.predict(cleaned_text)
    return label[0].replace("__label__", ""), float(proba[0])