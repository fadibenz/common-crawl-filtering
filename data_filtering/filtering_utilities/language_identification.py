import fasttext
from typing import Tuple

def language_identification(text: str) -> Tuple[str, float]:
    model_path = "classifier_models/fasttext_language_ID.bin"
    cleaned_text = text.replace('\n', ' ')
    model = fasttext.load_model(model_path)
    label, proba = model.predict(cleaned_text)
    return label[0][9:], float(proba[0])