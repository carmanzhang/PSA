import numpy as np
from typing import List, Union

from scorer.scorer import SimpleScorer, NoQueryScorer


class RandomScorer(SimpleScorer):
    def __init__(self):
        # Note add a method_signature
        super().__init__('random')

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        return np.random.random(len(c_contents))


class NoQueryRandomScorer(NoQueryScorer):
    def __init__(self):
        # Note add a method_signature
        super().__init__('random-no-query')

    def noquery_score(self, train_id: List[str], train_contents: List[str], train_orders: List[int], test_id: List[str],
              test_contents: List[str]) -> Union[List[float], None]:
        scores = np.random.randn(len(test_id)).tolist()
        return scores
