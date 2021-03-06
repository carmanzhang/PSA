from scipy.spatial import distance
from typing import List, Union

from config import *
from model.action_processing import ActionProcessor
from model.ml import MLModel
from scorer.scorer import SimpleScorer, NoQueryScorer


class SBertScorer(SimpleScorer):
    def __init__(self, model_name_or_path):
        dataset_names = [n.value for n in AvailableDataset.aslist()]
        fine_tune_dataset = [1 if n in model_name_or_path else 0 for n in dataset_names]
        if sum(fine_tune_dataset) == 1:
            fine_tune_dataset = dataset_names[fine_tune_dataset.index(1)]
            model_name = model_name_or_path.split('/')[0]
            model_signature = 'sbert-%s-%s' % (model_name, fine_tune_dataset)
            model_name_or_path = os.path.join(saved_model_base_path, model_name_or_path)
        else:
            model_name = model_name_or_path.split('/')[1]
            model_signature = 'sbert-origin-%s' % model_name

        super().__init__(model_signature)
        self.model_path = model_name_or_path
        self.processor = None

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        if self.processor is None:
            self.processor = ActionProcessor(self.model_path, data=None)
            self.processor.model.max_seq_length = ModelConfig.max_seq_length
            print('max_seq_length: ', self.processor.model.max_seq_length)
        content_list = [q_content] + c_contents
        sent_ebs = [n[1] for n in self.processor.infer(content_list, batch_size=320)]
        q_sent_eb = sent_ebs[0]
        c_sent_ebs = sent_ebs[1:]
        scores = [1 - distance.cosine(q_sent_eb, c_sent_eb) for c_sent_eb in c_sent_ebs]
        # print(scores)
        return scores


class NoQuerySBertScorer(NoQueryScorer):
    def __init__(self, model_name_or_path):
        dataset_names = [n.value for n in AvailableDataset.aslist()]
        fine_tune_dataset = [1 if n in model_name_or_path else 0 for n in dataset_names]
        if sum(fine_tune_dataset) == 1:
            fine_tune_dataset = dataset_names[fine_tune_dataset.index(1)]
            model_name = model_name_or_path.split('/')[0]
            model_signature = 'sbert-%s-%s-no-query' % (model_name, fine_tune_dataset)
            model_name_or_path = os.path.join(saved_model_base_path, model_name_or_path)
        else:
            model_name = model_name_or_path.split('/')[1]
            model_signature = 'sbert-origin-%s-no-query' % model_name

        super().__init__(model_signature)
        self.model_path = model_name_or_path
        self.processor = None

    def noquery_score(self, train_id: List[str], train_contents: List[str], train_orders: List[int], test_id: List[str],
                      test_contents: List[str]) -> Union[List[float], None]:
        if self.processor is None:
            self.processor = ActionProcessor(self.model_path, data=None)
            self.processor.model.max_seq_length = ModelConfig.max_seq_length
            print('max_seq_length: ', self.processor.model.max_seq_length)

        contents = train_contents + test_contents
        # title_abstract, c_mesh_headings, c_mesh_qualifiers, c_journal
        contents = [n[0] for n in contents]

        sent_ebs = [n[1] for n in self.processor.infer(contents, batch_size=320)]
        # Note learning embedding -> label mapping using ML
        train_ebs = sent_ebs[:len(train_contents)]
        test_ebs = sent_ebs[len(train_contents):]
        if len(test_ebs) == 0:
            return None
        else:
            _, scores, _ = MLModel.svm_regressor(train_ebs, train_orders, test_ebs)
            return scores
