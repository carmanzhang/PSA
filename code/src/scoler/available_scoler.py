from typing import List

from config import num_lda_topics, lda_based_path
from scoler.bm25 import BM25Scorer
from scoler.comp_gram import CompNGramScorer
from scoler.lda import LDAScorer
from scoler.random import RandomScorer
from scoler.scorer import Scorer
from scoler.word_embedding import WordEmbeddingSAVGScorer


class ScorerMethodProvider:
    @staticmethod
    def methods() -> List[Scorer]:
        return [
            RandomScorer(),
            BM25Scorer(),
            WordEmbeddingSAVGScorer('biowordvec'),
            WordEmbeddingSAVGScorer('fasttext'),
            WordEmbeddingSAVGScorer('glove'),
            LDAScorer(num_topics=num_lda_topics, model_based_path=lda_based_path),
            CompNGramScorer(),
        ]
