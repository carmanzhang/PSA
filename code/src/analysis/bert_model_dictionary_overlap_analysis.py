import os

dictionary = ''

based_path = ''
specter_words = set([line.strip() for line in open(
    '/home/zhangli/.cache/torch/sentence_transformers/sbert.net_models_allenai-specter/0_Transformer/vocab.txt')])
biobert_words = set([line.strip() for line in open('/home/zhangli/pre-trained-models/biobert-v1.1/vocab.txt')])
scibert_words = set([line.strip() for line in open('/home/zhangli/pre-trained-models/scibert_scivocab_uncased/vocab.txt')])

print(len(scibert_words), len(specter_words), len(biobert_words))
print(len(scibert_words.intersection(specter_words)) / len(scibert_words),
      len(scibert_words.intersection(biobert_words)) / len(scibert_words))
