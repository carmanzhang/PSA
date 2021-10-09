'''
ElMo BOW
fastText BOW
Glove BOW
BioWordVec BOW
'''
import io
import numpy as np
import os
from gensim.models import KeyedVectors
from nltk.corpus import stopwords
from scipy.spatial import distance
from tqdm import tqdm
from typing import List, Union

from config import pretrained_model_path
from model.ml import MLModel
from scorer.scorer import SimpleScorer, NoQueryScorer


class WordEmbeddingSAVGScorer(SimpleScorer, NoQueryScorer):
    def __init__(self, use_word_vec_method, with_query=''):
        self.available_use_word_vec_methods = ['fasttext', 'glove']
        self.use_word_vec_method = use_word_vec_method
        self.stop_words = set(stopwords.words('english'))

        self.word_embeddings_dict = None
        super().__init__(('word-ebm-%s' % use_word_vec_method) + with_query)

    def _lazy_read_all_word_vec_dict(self):
        if self.use_word_vec_method == 'glove':
            word_embeddings_dict = {}
            with open(os.path.join(pretrained_model_path, 'glove.840B/glove.840B.300d.txt'), 'r') as f:
                for line in tqdm(f):
                    values = line.split()
                    if len(values) % 100 != 1:  # 101, 201, 301, ...
                        continue
                    word = values[0]
                    try:
                        vector = np.asarray(values[1:], "float32")
                    except Exception as e:
                        print(len(values), values)
                        continue
                    word_embeddings_dict[word] = vector
        elif self.use_word_vec_method == 'fasttext':
            word_embeddings_dict = {}
            fin = io.open(os.path.join(pretrained_model_path, 'wiki-news-300d-1M.vec'), 'r', encoding='utf-8',
                          newline='\n',
                          errors='ignore')
            n, d = map(int, fin.readline().split())
            for line in tqdm(fin):
                tokens = line.rstrip().split(' ')
                vector = np.asarray(tokens[1:], "float32")
                word_embeddings_dict[tokens[0]] = vector
        elif self.use_word_vec_method == 'biowordvec':
            word_vector_model = KeyedVectors.load_word2vec_format(
                os.path.join(pretrained_model_path, 'BioWordVec/bio_embedding_extrinsic'),
                binary=True)
            word_embeddings_dict = dict(zip(word_vector_model.index2word, word_vector_model.vectors))
        self.word_embeddings_dict = word_embeddings_dict

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        if self.word_embeddings_dict is None:
            self._lazy_read_all_word_vec_dict()
        q_content_embedding = [self.word_embeddings_dict.get(n) for n in q_content.split(' ') if
                               n in self.word_embeddings_dict]
        q_content_embedding = self.pooling_word_embedding_to_sentence_embedding(q_content_embedding)
        scores = []
        for c_content in c_contents:
            c_content_embedding = [self.word_embeddings_dict.get(n) for n in c_content.split(' ') if
                                   n in self.word_embeddings_dict]
            c_content_embedding = self.pooling_word_embedding_to_sentence_embedding(c_content_embedding)
            try:
                score = 1 - distance.cosine(q_content_embedding, c_content_embedding)
            except Exception as e:
                # Note In order to be a fair competitor, we assign random values to the rare cases
                print('error!', e)
                score = np.random.random()
            scores.append(score)
        return scores

    def noquery_score(self, train_id: List[str], train_contents: List[str], train_orders: List[int], test_id: List[str],
                      test_contents: List[str]) -> Union[List[float], None]:
        if self.word_embeddings_dict is None:
            self._lazy_read_all_word_vec_dict()

        train_contents = [n[0] for n in train_contents]
        test_contents = [n[0] for n in test_contents]

        train_ebs = [
            self.pooling_word_embedding_to_sentence_embedding([self.word_embeddings_dict.get(n) for n in content.split(' ') if
                                                               n in self.word_embeddings_dict]) for content in train_contents]
        test_ebs = [
            self.pooling_word_embedding_to_sentence_embedding([self.word_embeddings_dict.get(n) for n in content.split(' ') if
                                                               n in self.word_embeddings_dict]) for content in test_contents]
        if len(test_ebs) == 0:
            return None
        else:
            _, scores, _ = MLModel.svm_regressor(train_ebs, train_orders, test_ebs)
            return scores

    def cosine_similarity(l1, l2):
        return 1 - distance.cosine(l1, l2)

    def pooling_word_embedding_to_sentence_embedding(self, emds):
        if emds is None or len(emds) == 0:
            return None
        else:
            emds = np.average([n for n in emds if n is not None], axis=0)
            if len(emds) == 0:
                return None
            else:
                return emds
