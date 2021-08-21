'''
ElMo BOW
fastText BOW
Glove BOW
BioWordVec BOW
Note GloVe Positional Encoding ?
'''

import sent2vec
from nltk import word_tokenize
from nltk.corpus import stopwords
from scipy.spatial import distance
from typing import List

from scoler.scorer import Scorer

"""
Note please refer to https://github.com/epfml/sent2vec
and Unsupervised Learning of Sentence Embeddings using Compositional n-Gram Features, NAACL, 2018
"""


class CompNGramScorer(Scorer):
    def __init__(self):
        # Note add a method_signature
        self.stop_words = set(stopwords.words('english'))
        self._load_model()
        super().__init__('sent2vec')

    def _load_model(self):
        # Note Load BioSentVec model
        model_path = '/home/zhangli/pre-trained-models/wiki_unigrams.bin'
        model = sent2vec.Sent2vecModel()
        try:
            model.load_model(model_path, inference_mode=True)
        except Exception as e:
            print(e)
        print('model successfully loaded')
        self.model = model

    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        def preprocess_sentence(text):
            text = text.replace('/', ' / ')
            text = text.replace('.-', ' .- ')
            text = text.replace('.', ' . ')
            text = text.replace('\'', ' \' ')
            text = text.lower()

            tokens = [token for token in word_tokenize(text) if token not in self.stop_words]

            return ' '.join(tokens)

        q_content_embedding = preprocess_sentence(q_content)
        scores = []
        for c_content in c_contents:
            c_content_embedding = self.model.embed_sentence(c_content)
            score = 1 - distance.cosine(q_content_embedding, c_content_embedding)
            scores.append(score)
        return scores

    def cosine_similarity(l1, l2):
        return 1 - distance.cosine(l1, l2)
