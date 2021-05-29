import rank_eval

# DCG & NDCG are one of the few metrics that take into account the non-binary utility function
# MAP is supposed to be a classic and a 'go-to' metric for this problem and it seems to be a standard in the field.
# Mean Reciprocal Rank only marks the position of the first relevant document,
# so if you care about as many relevant docs as possible to be high on the list, then this should not be your choice

rank_eval.ndcg()
rank_eval.map()
rank_eval.mrr()