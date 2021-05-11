from myio.data_reader import DBReader

sql = """
with (select groupArray((pm_id, embedding))
      from and.pubmed_all_paper_bert_embedding any
               inner join (select arrayJoin(arrayDistinct(arrayConcat(groupArray(pm_id1), groupArray(pm_id2)))) as pm_id
                           from and.GS_dataset) using pm_id) as gs_paper_embeddings_arr
select tupleElement(arrayFilter(x->tupleElement(x, 1) == pm_id1, gs_paper_embeddings_arr)[1], 2) as emb1,
       tupleElement(arrayFilter(x->tupleElement(x, 1) == pm_id2, gs_paper_embeddings_arr)[1], 2) as emb2,
       same_author
from and.GS_dataset;
"""

df = DBReader.tcp_model_cached_read("cached/xxx",
                                    sql,
                                    cached=False)
print(df.shape)

from scipy.spatial import distance


def sim_score(v1, v2):
    return 1 - distance.cosine(v1, v2)


emb1 = df['emb1'].values
emb2 = df['emb2'].values
from sklearn import metrics

score = [sim_score(a, b) for a, b in zip(emb1, emb2)]
for threshold in range(20, 90, 10):
    labels = [1 if n >= threshold * 0.01 else 0 for n in score]
    same_author = df['same_author'].values
    print(threshold,
        # metrics.auc(score, same_author),
        metrics.accuracy_score(same_author, labels),
        metrics.f1_score(same_author, labels))
