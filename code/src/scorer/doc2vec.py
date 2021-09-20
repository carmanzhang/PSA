import numpy as np
import os
import pickle
from gensim.models import Doc2Vec
from scipy.spatial import distance
from sentence2vec.word2vec import Sent2Vec
from typing import List

from config import model_dir, doc2vec_based_path
from scorer.scorer import SimpleScorer


class Doc2vecScorer(SimpleScorer):
    def __init__(self, model_based_path):
        # Note add a method_signature
        self.model_based_path = model_based_path
        self.doc2vec = None
        super().__init__('doc2vec')

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        if self.doc2vec is None:
            self._load_model()

        q_doc_vec = self.doc2vec.infer_vector(q_content.split())
        c_doc_vecs = [self.doc2vec.infer_vector(n.split()) for n in c_contents]

        scores = []
        for c_doc_vec in c_doc_vecs:
            score = 1 - distance.cosine(q_doc_vec, c_doc_vec)
            scores.append(score)

        # for c_doc_id in c_doc_ids:
        #     # Compute cosine similarity between two sentences. sent1 and sent2 are
        #     score = self.doc2vec.similarity([q_doc_id], [c_doc_id])
        #     scores.append(score)
        return scores

    def _load_model(self):
        self.doc2vec = Doc2Vec.load(os.path.join(doc2vec_based_path, "pmc_doc2vec_model.pkl"))
