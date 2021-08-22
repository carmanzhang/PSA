from abc import ABC, abstractmethod
from typing import List


class SimpleScorer(ABC):
    def __init__(self, signature):
        self.signature = signature

    @abstractmethod
    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        pass


class SupervisedScorer(SimpleScorer):
    def __init__(self, signature):
        self.model = None
        super().__init__(signature)
        # self.load_or_train(train_data_df)

    # @abstractmethod
    # def load_or_train(self, train_data_df):
    #     pass
