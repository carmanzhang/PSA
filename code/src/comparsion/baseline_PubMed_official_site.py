import numpy as np
from sklearn import metrics

from myio.data_reader import DBReader

df = DBReader.tcp_model_cached_read(cached_file_path="cdsvdv",
                                    sql="select pm_id, true_relevance, scores from sp.oneline_evaluating_pubmed_official_result where rand() % 100 < 30",
                                    cached=False)
print(df.shape)
print(df.columns.values)

true_relevances = df['true_relevance'].values
scores = df['scores'].values

for topn in range(10, 201, 10):
    true_relevances_k = np.array([np.array(n[:topn]) for i, n in enumerate(true_relevances) if len(n) >= topn])
    scores_k = np.array([np.array(n[:topn]) for i, n in enumerate(scores) if len(n) >= topn])
    assert len(true_relevances_k) == len(scores_k)
    # print(type(true_relevances_k), type(scores_k))
    res = metrics.ndcg_score(y_true=true_relevances_k, y_score=scores_k, k=topn)
    print(topn, len(scores_k), res)

# true_rankings = df['true_ranking'].values
# true_rankings = np.array([np.array(n) for n in true_rankings])
# pred_rankings = df['pred_ranking'].values
# rank_eval.ndcg(true_rankings, pred_rankings, k=10, threads=12)
#
# results = []
# for i, ebd in enumerate(true_rankings):
#     y_true = np.array(ebd)
#     y_pred = np.array(pred_rankings[i])
#     # y_pred = [n[1] for n in y_pred]
#     res = rank_eval.ndcg(y_true, y_pred, k=len(y_true), threads=12)
#     results.append(res)
#     # res = metrics.ndcg_score(true_ranking, pred_ranking)
#     # print(res)
# res = np.average(results)
# print(res)
#
# save_result(spec='PubMed_official_site.txt', metrics=json.dumps({'ndcg': res}, indent=4), desc=None)
