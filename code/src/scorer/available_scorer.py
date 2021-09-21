from typing import List

from config import num_lda_topics, doc2vec_based_path, lda_based_path
# from scorer.bilstm import BiLSTMScorer
from scorer.bioreader_scorer import BioreaderScorer
from scorer.bm25 import BM25Scorer
from scorer.comp_gram import CompNGramScorer
from scorer.doc2vec import Doc2vecScorer
from scorer.infersent import InferSentScorer
from scorer.lda import LDAScorer
from scorer.medlineranker_scorer import MedlineRankerScorer
from scorer.mscanner_scorer import MScannerScorer
from scorer.prc import PMRAScorer
from scorer.rand import RandomScorer, NoQueryRandomScorer
from scorer.sbert_scorer import SBertScorer, NoQuerySBertScorer
from scorer.scorer import SimpleScorer, NoQueryScorer
from scorer.word_embedding import WordEmbeddingSAVGScorer
from scorer.xprc import XPRCScorer


class ScorerMethodProvider:
    def methods(self) -> List[SimpleScorer]:
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
            # InferSentScorer(model_version=1),
            # InferSentScorer(model_version=2),

            # PMRAScorer(),
            # XPRCScorer(), # TODO

            # SBertScorer(model_name_or_path='allenai/specter'),
            SBertScorer(model_name_or_path='dmis-lab/biobert-v1.1'),
            SBertScorer(model_name_or_path='allenai/scibert_scivocab_uncased'),

            SBertScorer(model_name_or_path='allenai-specter/tuned_relish_v1-lsTRIPLET-ep3-bs16-lr0.000010-vl10-sl200'),
            SBertScorer(model_name_or_path='allenai-specter/tuned_trec_genomic_2005-lsTRIPLET-ep3-bs16-lr0.000010-vl10-sl200'),

            SBertScorer(model_name_or_path='biobert-v1.1/tuned_relish_v1-lsTRIPLET-ep3-bs16-lr0.000010-vl10-sl200'),
            SBertScorer(model_name_or_path='biobert-v1.1/tuned_trec_genomic_2005-lsTRIPLET-ep3-bs16-lr0.000010-vl10-sl200'),

            SBertScorer(model_name_or_path='scibert_scivocab_uncased/tuned_relish_v1-lsTRIPLET-ep3-bs16-lr0.000010-vl10-sl200'),
            SBertScorer(model_name_or_path='scibert_scivocab_uncased/tuned_trec_genomic_2005-lsTRIPLET-ep3-bs16-lr0.000010-vl10-sl200'),
        ]

    def no_query_methods(self) -> List[NoQueryScorer]:
        return [
            # NoQueryRandomScorer(),
            # MScannerScorer(),
            # MedlineRankerScorer(),
            # BioreaderScorer(),

            NoQuerySBertScorer(model_name_or_path='allenai/specter'),
            NoQuerySBertScorer(model_name_or_path='dmis-lab/biobert-v1.1'),
            NoQuerySBertScorer(model_name_or_path='allenai/scibert_scivocab_uncased'),

            NoQuerySBertScorer(model_name_or_path='allenai-specter/tuned_no-query-relish_v1-lsCONTRASTIVE-ep3-bs16-lr0.000010-vl10-sl200'),
            NoQuerySBertScorer(model_name_or_path='allenai-specter/tuned_no-query-trec_genomic_2005-lsCONTRASTIVE-ep3-bs16-lr0.000010-vl10-sl200'),

            NoQuerySBertScorer(model_name_or_path='biobert-v1.1/tuned_no-query-relish_v1-lsCONTRASTIVE-ep3-bs16-lr0.000010-vl10-sl200'),
            NoQuerySBertScorer(model_name_or_path='biobert-v1.1/tuned_no-query-trec_genomic_2005-lsCONTRASTIVE-ep3-bs16-lr0.000010-vl10-sl200'),

            NoQuerySBertScorer(model_name_or_path='scibert_scivocab_uncased/tuned_no-query-relish_v1-lsCONTRASTIVE-ep3-bs16-lr0.000010-vl10-sl200'),
            NoQuerySBertScorer(model_name_or_path='scibert_scivocab_uncased/tuned_no-query-trec_genomic_2005-lsCONTRASTIVE-ep3-bs16-lr0.000010-vl10-sl200'),

        ]
