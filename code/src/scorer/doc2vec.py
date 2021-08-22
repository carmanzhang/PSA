from sentence2vec.word2vec import Sent2Vec
from typing import List

from scorer.scorer import SimpleScorer


class Doc2vecScorer(SimpleScorer):
    def __init__(self, model_based_path):
        # Note add a method_signature
        self.model_based_path = model_based_path
        self.doc2vec = None
        super().__init__('doc2vec')

    # TODO infer the topic for test set
    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        if self.doc2vec is None:
            self._load_model()
        scores = []
        for c_content in c_contents:
            # Compute cosine similarity between two sentences. sent1 and sent2 are
            score = self.doc2vec.similarity([q_content], [c_content])
            scores.append(score)
        return scores

    def _load_model(self):
        self.doc2vec = Sent2Vec.load(self.model_based_path)