from gensim.summarization.bm25 import BM25
from typing import List

from scorer.scorer import SimpleScorer


class BM25Scorer(SimpleScorer):
    def __init__(self):
        # Note add a method_signature
        super().__init__('bm25')
    #     Note
    #     filter = PorterStemFilter(filter) # transform the token stream as per the Porter stemming algorithm
    # filter = StopFilter(Version.LUCENE_CURRENT, filter,
    #                     StopAnalyzer.ENGLISH_STOP_WORDS_SET)
    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        query = q_content.split()
        tok_corpus = [c_content.split() for c_content in c_contents]
        bm25 = BM25(tok_corpus)
        scores = bm25.get_scores(query)
        return scores
