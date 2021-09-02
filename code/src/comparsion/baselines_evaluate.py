from tqdm import tqdm

from config import which_datasets
from metric.all_metric import eval_metrics
from myio.data_reader import DBReader
from scorer.available_scorer import ScorerMethodProvider


def run(method, ds):
    ds_name = ds.value
    model_name = method.signature
    running_desc = '_'.join([model_name, ds_name])
    df = DBReader.tcp_model_cached_read('vdsvfn',
                                        sql='''select  q_pm_id,
                                                       concat(q_content[1], ' ', q_content[2]) as q_content,
                                                       arrayMap(x->
                                                                    (tupleElement(x, 1),
                                                                     concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                                                     tupleElement(x, 3))
                                                           , c_tuples)                         as c_tuples
                                                from sp.eval_data_%s_with_content where train1_val2_test0 in (0);''' % ds_name,
                                        cached=False)
    df_test = df
    # Note there is no need training
    print('shape test dataframes: ', df_test.shape)

    items = df_test[['q_pm_id', 'q_content', 'c_tuples']].values
    all_query_ranks = []
    for item in tqdm(items):
        q_pm_id, q_content, c_tuples = item
        # rcm_id, c_content, label
        rcm_ids = [rcm_id for rcm_id, c_content, label in c_tuples]
        c_contents = [c_content for rcm_id, c_content, label in c_tuples]
        orders = [label for rcm_id, c_content, label in c_tuples]

        # Note add scorer
        scores = method.score(q_content, c_contents, q_pm_id, rcm_ids)
        # scores = bm25_score(q_content, c_contents)

        assert len(scores) == len(orders)
        query_rank = sorted(zip(rcm_ids, scores, orders), key=lambda x: x[1], reverse=True)
        all_query_ranks.append(query_rank)

    eval_metrics(all_query_ranks, running_desc)
    print('current dataset: ', ds.value, 'method: ', model_name)


if __name__ == '__main__':
    methods = ScorerMethodProvider().methods()
    for i, method in enumerate(methods):
        for j, ds in enumerate(which_datasets):
            # if j > 0:
            #     break
            run(method, ds)
        methods[i] = None
print("*" * 100)
