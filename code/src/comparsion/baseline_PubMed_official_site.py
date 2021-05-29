import json

import numpy as np
import rank_eval

from helper.resultsaver import save_result
from myio.data_reader import DBReader

df = DBReader.tcp_model_cached_read(cached_file_path="xxx",
                                    sql="select pm_id, pred_ranking, true_ehcmeshref_ranking as true_ranking from sp.pubmed_similar_paper_bias_dataset where rand() % 100 < 2",
                                    cached=False)
print(df.shape)

true_rankings = df['true_ranking'].values
pred_rankings = df['pred_ranking'].values

results = []
for i, ebd in enumerate(true_rankings):
    y_true = np.array(ebd, dtype=int)
    y_pred = np.array(pred_rankings[i], dtype=int)
    res = rank_eval.ndcg(y_true, y_pred, k=len(y_true), threads=12)
    results.append(res)
    # res = metrics.ndcg_score(true_ranking, pred_ranking)
    # print(res)
res = np.average(results)
print(res)

save_result(spec='PubMed_official_site.txt', metrics=json.dumps({'ndcg': res}, indent=4), desc=None)
