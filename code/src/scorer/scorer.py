from abc import ABC, abstractmethod
from typing import List, Union


class SimpleScorer(ABC):
    def __init__(self, signature):
        self.signature = signature

    @abstractmethod
    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        pass


class NoQueryScorer(ABC):
    def __init__(self, signature):
        self.signature = signature

    @abstractmethod
    def noquery_score(self,
              train_id: List[str],
              train_contents: List[str],
              train_orders: List[int],
              test_id: List[str],
              test_contents: List[str]) -> Union[List[float], None]:
        pass
