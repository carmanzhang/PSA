import os

from spacy.scorer import Scorer
from typing import List

from config import num_lda_topics, doc2vec_based_path, saved_model_base_path, lda_based_path
from scorer.bilstm import BiLSTMScorer
from scorer.bm25 import BM25Scorer
from scorer.comp_gram import CompNGramScorer
from scorer.doc2vec import Doc2vecScorer
from scorer.infersent import InferSentScorer
from scorer.lda import LDAScorer
from scorer.prc import PMRAScorer
from scorer.rand import RandomScorer
from scorer.word_embedding import WordEmbeddingSAVGScorer
from scorer.sbert_scorer import SBertScorer
from scorer.xprc import XPRCScorer


class ScorerMethodProvider:
    def methods(self) -> List[Scorer]:
        return [
            # RandomScorer(),
            # BM25Scorer(),
            # WordEmbeddingSAVGScorer('biowordvec'),
            # WordEmbeddingSAVGScorer('fasttext'),
            # WordEmbeddingSAVGScorer('glove'),
            # LDAScorer(num_topics=num_lda_topics, model_based_path=lda_based_path),
            # Doc2vecScorer(model_based_path=doc2vec_based_path),
            # CompNGramScorer(model_name='wiki_unigrams'),
            # CompNGramScorer(model_name='BioSentVec_PubMed_MIMICIII-bigram_d700'),
            # InferSentScorer(model_version=2),
            # BiLSTMScorer(model_path='untrained'),
            # BiLSTMScorer(model_path='psa'),  # on this task: PubMed Similar Article
            # BiLSTMScorer(model_path='snli'),
            # PMRAScorer(),
            XPRCScorer(),
            # SBertScorer(model_name_or_path='allenai-specter/tuned_lsTRIPLET-ep12-bs8-lr0.000020-vl10-sl200'),
        ]
