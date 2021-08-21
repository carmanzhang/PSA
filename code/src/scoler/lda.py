import joblib
import multiprocessing
import os
from gensim.corpora.dictionary import Dictionary
from gensim.models import LdaModel
from gensim.models.ldamulticore import LdaMulticore
from gensim.test.utils import datapath
from scipy.spatial import distance
from typing import List

from myio.data_reader import DBReader
from scoler.scorer import Scorer




class LDAScorer(Scorer):
    def __init__(self, num_topics, model_based_path):
        # Note add a method_signature
        self.num_topics = 64
        self.cpu_cnt = multiprocessing.cpu_count()
        self.dictionary_path = model_based_path + "/pmc_lda_dictionary.pkl"
        self.model_path = model_based_path + "/cached/pmc_lda_t%d.lda" % num_topics

        super().__init__('lda')

    # TODO infer the topic for test set
    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        q_content_bow = [self.dictionary.doc2bow([m for m in q_content.split() if len(m) > 0])]
        q_content_embedding = self.lda.get_document_topics(q_content_bow, minimum_probability=0)
        q_content_embedding = [[m[1] for m in sorted(n, key=lambda x: x[0], reverse=False)] for n in
                               q_content_embedding if len(n) == self.num_topics]
        q_content_embedding = q_content_embedding[0]
        c_contents_bow = [self.dictionary.doc2bow([m for m in n.split() if len(m) > 0]) for n in c_contents]
        c_contents_embeddings = self.lda.get_document_topics(c_contents_bow, minimum_probability=0)
        c_contents_embeddings = [[m[1] for m in sorted(n, key=lambda x: x[0], reverse=False)] for n in
                                 c_contents if len(n) == self.num_topics]
        scores = []
        for c_content_embedding in c_contents_embeddings:
            score = 1 - distance.cosine(q_content_embedding, c_content_embedding)
            scores.append(score)
        return scores

    # Note train the model is not found
    def load_or_train_lda(self):
        # Save a model to disk, or reload a pre-trained model
        # Save model to disk.
        temp_file = datapath(self.model_path)
        if os.path.exists(self.model_path):
            lda = LdaModel.load(temp_file)
            dictionary = joblib.load(self.dictionary_path)
            print('loaded from local')
        else:
            # Note prepare corpus
            # more than 2 million PMC papers
            corpus = DBReader.tcp_model_cached_read("XXXX",
                                                    """select concat(clean_title, ' ', clean_abstract,  ' ', mesh_keywords) as content from pr.pmc_paper_clean_merge_content
                                                    where rand() % 100 <= 20 
                                                    """,
                                                    cached=False)['content'].values

            corpus = [[m for m in n.split() if len(m) > 0] for n in corpus]
            print('number of corpus: %d' % len(corpus), corpus[:5])

            # Create a corpus from a list of texts
            dictionary = Dictionary(corpus)
            joblib.dump(dictionary, self.dictionary_path)

            corpus = [dictionary.doc2bow(text) for text in corpus]
            # ldalda = LdaModel(corpus, num_topics=num_topics, id2word=dictionary)
            # Train the model on the corpus.
            # None If workers=None all available cores (as estimated by `workers=cpu_count()-1` will be used.
            lda = LdaMulticore(corpus=corpus, num_topics=self.num_topics, id2word=dictionary, workers=self.cpu_cnt - 2)
            lda.save(temp_file)
            print('persistented lda model')

            topic_list = lda.print_topics(self.num_topics)
            for topic in topic_list:
                print(topic)
            self.lda = lda
