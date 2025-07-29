from typing import Tuple

def classify_harmful_content(text: str,
                  model) -> Tuple[str, float]:
    cleaned_text = text.replace('\n', ' ')
    label, proba = model.predict(cleaned_text)
    return label[0].replace("__label__", ""), float(proba[0])