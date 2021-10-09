import os
import sent2vec
from scipy.spatial import distance
from typing import List, Union

from config import pretrained_model_path
from model.ml import MLModel
from scorer.scorer import SimpleScorer, NoQueryScorer

"""
Note please refer to https://github.com/epfml/sent2vec
and Unsupervised Learning of Sentence Embeddings using Compositional n-Gram Features, NAACL, 2018
"""


class CompNGramScorer(SimpleScorer, NoQueryScorer):
    def __init__(self, model_name, with_query=''):
        # Note add a method_signature
        self.model_name = model_name
        self.model = None
        super().__init__(('sent2vec-%s' % model_name) + with_query)

    def _load_model(self):
        model = sent2vec.Sent2vecModel()
        model_path = os.path.join(pretrained_model_path, self.model_name + '.bin')
        model.load_model(model_path)
        self.model = model

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        if self.model is None:
            self._load_model()
        q_content_emb = self.model.embed_sentence(q_content)
        c_content_embs = self.model.embed_sentences(c_contents)
        scores = []
        for c_content_emb in c_content_embs:
            score = 1 - distance.cosine(q_content_emb, c_content_emb)
            scores.append(score)
        return scores

    def noquery_score(self, train_id: List[str], train_contents: List[str], train_orders: List[int], test_id: List[str],
                      test_contents: List[str]) -> Union[List[float], None]:
        if self.model is None:
            self._load_model()

        contents = train_contents + test_contents
        # title_abstract, c_mesh_headings, c_mesh_qualifiers, c_journal
        contents = [n[0] for n in contents]

        sent_ebs = self.model.embed_sentences(contents)
        # Note learning embedding -> label mapping using ML
        train_ebs = sent_ebs[:len(train_contents)]
        test_ebs = sent_ebs[len(train_contents):]
        if len(test_ebs) == 0:
            return None
        else:
            _, scores, _ = MLModel.svm_regressor(train_ebs, train_orders, test_ebs)
            return scores
