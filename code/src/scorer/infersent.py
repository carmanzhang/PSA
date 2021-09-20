import os
import torch
from scipy.spatial import distance
from typing import List

from config import glove840b300d_path, fasttextcrawl300d2m_path, infersent_based_path
from model.infersent import InferSent
from scorer.scorer import SimpleScorer


class InferSentScorer(SimpleScorer):
    def __init__(self, model_version):
        # Note add a method_signature
        self.model_version = model_version
        self.model = None
        self.use_cuda = True
        super().__init__('infersent-v%d' % model_version)

    def _load_model(self):
        model_path = os.path.join(infersent_based_path, "infersent%s.pkl" % self.model_version)
        params_model = {'bsize': 64, 'word_emb_dim': 300, 'enc_lstm_dim': 2048,
                        'pool_type': 'max', 'dpout_model': 0.0, 'version': self.model_version}
        model = InferSent(params_model)
        model.load_state_dict(torch.load(model_path))

        model = model.cuda() if self.use_cuda else model
        # If infersent1 -> use GloVe embeddings. If infersent2 -> use InferSent embeddings.
        wordvec_path = glove840b300d_path if self.model_version == 1 else fasttextcrawl300d2m_path
        model.set_w2v_path(wordvec_path)
        # build w2v vocab with k most frequent words
        model.build_vocab_k_words(K=500000)
        self.model = model

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        if self.model is None:
            self._load_model()

        embeddings = self.model.encode([q_content] + c_contents, bsize=8, tokenize=False) # , verbose=True
        # print('nb sentences encoded : {0}'.format(len(embeddings)))
        q_content_emb = embeddings[0]
        c_content_embs = embeddings[1:]
        scores = []

        for c_content_emb in c_content_embs:
            score = 1 - distance.cosine(q_content_emb, c_content_emb)
            scores.append(score)
        return scores
