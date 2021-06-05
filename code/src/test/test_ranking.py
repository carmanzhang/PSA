import rank_eval

# DCG & NDCG are one of the few metrics that take into account the non-binary utility function
# MAP is supposed to be a classic and a 'go-to' metric for this problem and it seems to be a standard in the field.
# Mean Reciprocal Rank only marks the position of the first relevant document,
# so if you care about as many relevant docs as possible to be high on the list, then this should not be your choice
#
# rank_eval.ndcg()
# rank_eval.map()
# rank_eval.mrr()
import numpy as np
from sklearn.metrics import ndcg_score
# we have groud-truth relevance of some answers to a query:
true_relevance = np.asarray([[10, 0, 0, 1, 5]])
# we predict some scores (relevance) for the answers
scores = np.asarray([[.1, .2, .3, 4, 7]])
res = ndcg_score(true_relevance, scores)
print(res)