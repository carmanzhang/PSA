from abc import ABC, abstractmethod
from typing import List



class Scorer(ABC):
    def __init__(self, signature):
        self.signature = signature

    @abstractmethod
    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        pass

