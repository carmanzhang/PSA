import numpy as np
from typing import List

from scoler.scorer import Scorer


class RandomScorer(Scorer):
    def __init__(self):
        # Note add a method_signature
        super().__init__('random')

    def score(self, q_content: str, c_contents: List[str]) -> List[float]:
        return np.random.random(len(c_contents))
