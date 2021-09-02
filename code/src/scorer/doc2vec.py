import numpy as np
import os
import pickle
from scipy.spatial import distance
from sentence2vec.word2vec import Sent2Vec
from typing import List

from config import model_dir
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
        q_doc_id = self.pm_id_doc_id_mapping[q_pm_id]
        c_doc_ids = [self.pm_id_doc_id_mapping[n] for n in c_pm_ids]

        q_doc_vec = self.pm_id_doc_vecs[q_doc_id]
        c_doc_vecs = self.pm_id_doc_vecs[c_doc_ids]

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
        self.doc2vec = Sent2Vec.load(self.model_based_path)
        self.pm_id_doc_id_mapping = pickle.load(open(os.path.join(model_dir, 'doc2vec.model-pm-id-mapping.pkl'), 'rb'))
        self.pm_id_doc_vecs = np.load(os.path.join(model_dir, 'doc2vec.model.model.sents.npy'))
