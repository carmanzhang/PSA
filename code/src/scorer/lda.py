import joblib
import multiprocessing
import os
from gensim.models import LdaModel
from gensim.test.utils import datapath
from nltk.corpus import stopwords
from scipy.spatial import distance
from typing import List

from scorer.scorer import SimpleScorer


class LDAScorer(SimpleScorer):
    def __init__(self, num_topics, model_based_path):
        # Note add a method_signature
        self.num_topics = 64
        # self.cpu_cnt = multiprocessing.cpu_count()
        # self.stop_words = set(stopwords.words('english'))
        self.dictionary_path = model_based_path + "/pmc_lda_dictionary.pkl"
        self.model_path = model_based_path + "/pmc_lda_t%d.lda" % num_topics
        self.lda = None
        self.dictionary = None

        super().__init__('lda')

    # TODO infer the topic for test set
    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        if self.dictionary is None or self.lda is None:
            self._load_or_train_lda()
        q_content_bow = [self.dictionary.doc2bow([m for m in q_content.split() if len(m) > 0])]
        q_content_embedding = self.lda.get_document_topics(q_content_bow, minimum_probability=0)
        q_content_embedding = [[m[1] for m in sorted(n, key=lambda x: x[0], reverse=False)] for n in
                               q_content_embedding if len(n) == self.num_topics]
        q_content_embedding = q_content_embedding[0]
        c_contents_bow = [self.dictionary.doc2bow([m for m in n.split() if len(m) > 0]) for n in c_contents]
        c_contents_embeddings = self.lda.get_document_topics(c_contents_bow, minimum_probability=0)
        c_contents_embeddings = [[m[1] for m in sorted(n, key=lambda x: x[0], reverse=False)] for n in
                                 c_contents_embeddings if len(n) == self.num_topics]
        scores = []
        for c_content_embedding in c_contents_embeddings:
            score = 1 - distance.cosine(q_content_embedding, c_content_embedding)
            scores.append(score)
        return scores

    def _load_or_train_lda(self):
        # Save a model to disk, or reload a pre-trained model
        # Save model to disk.
        temp_file = datapath(self.model_path)
        if os.path.exists(self.model_path):
            self.lda = LdaModel.load(temp_file)
            self.dictionary = joblib.load(self.dictionary_path)
            print('loaded from local')
