from gensim.summarization.bm25 import BM25
from typing import List

from scorer.scorer import SimpleScorer


class BM25Scorer(SimpleScorer):
    def __init__(self):
        # Note add a method_signature
        super().__init__('bm25')

    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        query = q_content.split()
        tok_corpus = [c_content.split() for c_content in c_contents]
        bm25 = BM25(tok_corpus)
        scores = bm25.get_scores(query)
        return scores
