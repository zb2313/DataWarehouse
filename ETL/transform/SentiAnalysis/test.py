import spacy
import torch
from dataload import device, generate_bigrams, TEXT
from model import model
import json
import jsonlines

nlp = spacy.load('en_core_web_sm')


def predict_sentiment(model, sentence):
    model.eval()
    tokenized = generate_bigrams([tok.text for tok in nlp.tokenizer(sentence)])
    indexed = [TEXT.vocab.stoi[t] for t in tokenized]
    tensor = torch.LongTensor(indexed).to(device)
    tensor = tensor.unsqueeze(1)
    prediction = torch.sigmoid(model(tensor))
    return prediction.item()


model.load_state_dict(torch.load('tut3-model.pt'))

file = open('../../data/movies2.json', 'r', encoding='utf-8')
file2 = jsonlines.open('../../data/movies3.json', 'a')
for line in file.readlines():
    dic = json.loads(line)
    if len(dic['reviews']) >= 1:
        for item in dic['reviews']:
            review_sentiment = predict_sentiment(model, item['text'])
            # print(item['text'], review_sentiment)
            item['sentiment'] = round(review_sentiment, 4)
            print(item['sentiment'])

    file2.write(dic)

file2.close()