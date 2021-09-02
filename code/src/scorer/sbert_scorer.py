from scipy.spatial import distance
from typing import List

from config import *
from model.action_processing import ActionProcessor
from scorer.scorer import SimpleScorer


class SBertScorer(SimpleScorer):
    def __init__(self, model_name_or_path):
        model_name = model_name_or_path.split('/')[0]
        super().__init__('sbert-%s' % model_name)
        self.model_path = os.path.join(saved_model_base_path, model_name_or_path)
        self.processor = None
        # df_train = df_train[['q_content', 'c_pos_content', 'c_neg_content']]
        # df_val = df_val_test[df_val_test['train1_val2_test0'] == 2].explode('c_tuples').reset_index(drop=True)
        # df_test = df_val_test[df_val_test['train1_val2_test0'] == 0].explode('c_tuples').reset_index(drop=True)
        #
        # df_val['c_pm_id'], df_val['c_content'], df_val['score'] = df_val['c_tuples'].apply(lambda x: x[0]), df_val[
        #     'c_tuples'].apply(lambda x: x[1]), df_val['c_tuples'].apply(lambda x: x[2])
        # del df_val['c_tuples']
        # df_test['c_pm_id'], df_test['c_content'], df_test['score'] = df_test['c_tuples'].apply(lambda x: x[0]), df_test[
        #     'c_tuples'].apply(lambda x: x[1]), df_test['c_tuples'].apply(lambda x: x[2])
        # del df_test['c_tuples']
        # print('load train/val/test data', df_train.shape, df_val.shape, df_test.shape)

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        if self.processor is None:
            self.processor = ActionProcessor(self.model_path, data=None)
            print('max_seq_length: ', self.processor.model.max_seq_length)
        content_list = [q_content] + c_contents
        sent_ebs = [n[1] for n in self.processor.infer(content_list, batch_size=320)]
        q_sent_eb = sent_ebs[0]
        c_sent_ebs = sent_ebs[1:]
        scores = [1 - distance.cosine(q_sent_eb, c_sent_eb) for c_sent_eb in c_sent_ebs]
        # print(scores)
        return scores
