from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch


class DistillBertModel:
    def __init__(self, model_name="distilbert-base-uncased", num_labels=2):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=num_labels)

    def predict(self, text):
        # Токенізація тексту
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, padding=True)

        # Прогноз через модель
        outputs = self.model(**inputs)
        logits = outputs.logits
        probabilities = torch.nn.functional.softmax(logits, dim=1)
        predicted_class = torch.argmax(probabilities, dim=1).item()

        return {
            "class": predicted_class,
            "probabilities": probabilities.tolist()
        }