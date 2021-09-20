from gensim.summarization.bm25 import BM25
from nltk.corpus import stopwords
from typing import List

from helper.word_helper import Stemmer
from scorer.scorer import SimpleScorer


class BM25Scorer(SimpleScorer):
    def __init__(self):
        super().__init__('bm25')
        self.stopwords = set(stopwords.words("english"))
        # self.stemmer = PorterStemmer()
        # self.stemmer = snowballstemmer.stemmer('english')  # faster than others
        # self.stemmer = spacy.load('en_core_web_sm')

    def clean(self, words, use_basic_form=True):
        if use_basic_form:
            words = Stemmer.stem(words)
        words = [word for word in words if word not in self.stopwords]
        return words

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        query = self.clean(q_content.split())
        tok_corpus = [self.clean(c_content.split()) for c_content in c_contents]
        bm25 = BM25(tok_corpus)
        scores = bm25.get_scores(query)
        return scores
