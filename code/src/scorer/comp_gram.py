import os
import sent2vec
from scipy.spatial import distance
from typing import List

from config import pretrained_model_path
from scorer.scorer import SimpleScorer

"""
Note please refer to https://github.com/epfml/sent2vec
and Unsupervised Learning of Sentence Embeddings using Compositional n-Gram Features, NAACL, 2018
"""


class CompNGramScorer(SimpleScorer):
    def __init__(self, model_name):
        # Note add a method_signature
        self.model_name = model_name
        self.model = None
        super().__init__('sent2vec-%s' % model_name)

    def _load_model(self):
        model = sent2vec.Sent2vecModel()
        model_path = os.path.join(pretrained_model_path, self.model_name + '.bin')
        model.load_model(model_path)
        self.model = model

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        if self.model is None:
            self._load_model()
        q_content_emb = self.model.embed_sentence(q_content)
        c_content_embs = self.model.embed_sentences(c_contents)
        scores = []
        for c_content_emb in c_content_embs:
            score = 1 - distance.cosine(q_content_emb, c_content_emb)
            scores.append(score)
        return scores
