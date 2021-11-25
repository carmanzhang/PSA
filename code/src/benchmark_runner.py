import copy

import json
import os
from tqdm import tqdm

from config import cached_dir, ranking_result_dir, AvailableDataset
from metric.all_metric import eval_metrics
from myio.data_reader import DBReader
from scorer.available_scorer import ScorerMethodProvider


def eval_query_based_method(method, ds_name, df_test):
    model_name = method.signature
    running_desc = '_'.join([model_name, ds_name])
    print(running_desc)

    # Note no training here
    query_rank_dict = {}
    all_query_ranks = []
    items = df_test[['q_pm_id', 'q_content', 'c_tuples']].values
    for item in tqdm(items):
        q_pm_id, q_content, c_tuples = item
        # rcm_id, c_content, label
        rcm_ids = [rcm_id for rcm_id, c_content, label in c_tuples]
        c_contents = [c_content for rcm_id, c_content, label in c_tuples]
        orders = [label for rcm_id, c_content, label in c_tuples]

        scores = method.score(q_content, c_contents, q_pm_id, rcm_ids)
        # scores = bm25_score(q_content, c_contents)

        assert len(scores) == len(orders)
        query_rank = sorted(zip(scores, orders), key=lambda x: x[0], reverse=True)
        all_query_ranks.append(query_rank)
        query_rank_dict[q_pm_id] = query_rank

    with open(os.path.join(ranking_result_dir, running_desc + '.json'), 'w') as fw:
        dump_str = json.dumps(query_rank_dict)
        fw.write(dump_str)
        fw.write('\n')

    eval_metrics(all_query_ranks, running_desc)
    print('current dataset: ', ds.value, 'method: ', model_name)


def eval_no_query_based_method(method, ds_name, df):
    model_name = method.signature
    running_desc = '_'.join([model_name, ds_name])
    print(running_desc)

    query_rank_dict = {}
    all_query_ranks = []
    for i, row in tqdm(df.iterrows(), total=df.shape[0]):
        id, train_data, val_data, test_data = row

        train_id = [n[0] for n in train_data]
        val_id = [n[0] for n in val_data]
        test_id = [n[0] for n in test_data]

        train_contents = [n[1] for n in train_data]
        val_contents = [n[1] for n in val_data]
        test_contents = [n[1] for n in test_data]

        train_orders = [n[2] for n in train_data]
        val_orders = [n[2] for n in val_data]
        test_orders = [n[2] for n in test_data]

        # Note that we combine val and test folds as the final test data, because the validation set will not being used
        test_id = test_id + val_id
        test_contents = test_contents + val_contents
        test_orders = test_orders + val_orders

        scores = method.noquery_score(train_id, train_contents, train_orders, test_id, test_contents)
        # print(p_docs, n_docs, len(scores), scores)
        if scores is None or len(scores) == 0:
            continue
        query_rank = sorted(zip(scores, test_orders), key=lambda x: x[0], reverse=True)
        all_query_ranks.append(query_rank)
        query_rank_dict[id] = query_rank

    with open(os.path.join(ranking_result_dir, running_desc + '.json'), 'w') as fw:
        dump_str = json.dumps(query_rank_dict)
        fw.write(dump_str)
        fw.write('\n')

    eval_metrics(all_query_ranks, running_desc)
    print('current dataset: ', ds.value, 'method: ', model_name)


sql_template = '''select  q_pm_id,
                           concat(q_content[1], ' ', q_content[2]) as q_content,
                           arrayMap(x->
                                        (tupleElement(x, 1),
                                         concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                         tupleElement(x, 3))
                               , c_tuples)                         as c_tuples
                    from sp.eval_data_%s_with_content where train1_val2_test0 in (0);'''

no_query_sql_template = '''select id,
       arrayMap(x-> (tupleElement(x, 1),
                     (concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                      arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                                             splitByChar('|', tupleElement(x, 2)[3]) as train_mesh_arr)),
                      arrayDistinct(arrayFilter(n->length(n) > 0,
                                                arrayMap(m->splitByString('; ', m)[2], train_mesh_arr))),
                      tupleElement(x, 2)[4]),
                     tupleElement(x, 3)), arraySort(z->xxHash32(z.1), arrayFlatten(train_part))) as train_part,

       arrayMap(x-> (tupleElement(x, 1),
                     (concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                      arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                                             splitByChar('|', tupleElement(x, 2)[3]) as val_mesh_arr)),
                      arrayDistinct(arrayFilter(n->length(n) > 0,
                                                arrayMap(m->splitByString('; ', m)[2], val_mesh_arr))),
                      tupleElement(x, 2)[4]),
                     tupleElement(x, 3)), arraySort(z->xxHash32(z.1), arrayFlatten(val_part)))   as val_part,

       arrayMap(x-> (tupleElement(x, 1),
                     (concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                      arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                                             splitByChar('|', tupleElement(x, 2)[3]) as test_mesh_arr)),
                      arrayDistinct(arrayFilter(n->length(n) > 0,
                                                arrayMap(m->splitByString('; ', m)[2], test_mesh_arr))),
                      tupleElement(x, 2)[4]),
                     tupleElement(x, 3)), arraySort(z->xxHash32(z.1), arrayFlatten(test_part)))  as test_part
from sp.eval_data_%s_with_content_without_query;'''

if __name__ == '__main__':
    which_datasets = AvailableDataset.aslist()
    methods = ScorerMethodProvider().methods()
    for i, method in enumerate(methods):
        for j, ds in enumerate(which_datasets):
            ds_name = ds.value
            sql = sql_template % ds_name
            print(sql)
            df = DBReader.tcp_model_cached_read(os.path.join(cached_dir, ds_name + '-test.pkl'), sql=sql, cached=True)
            df_copy = copy.deepcopy(df)
            # if j > 0:
            #     break
            eval_query_based_method(method, ds_name, df_copy)
        methods[i] = None

    no_query_methods = ScorerMethodProvider().no_query_methods()
    for i, method in enumerate(no_query_methods):
        for j, ds in enumerate(which_datasets):
            ds_name = ds.value
            sql = no_query_sql_template % ds_name
            print(sql)
            # Note the average number instances of training/evaluation/test is: 47.9, 6.08,	5.94 in this evaluation scenario
            df = DBReader.tcp_model_cached_read(os.path.join(cached_dir, '%s-no-query.pkl' % ds_name),
                                                sql=sql,
                                                cached=True)
            df_copy = copy.deepcopy(df)
            # if j > 0:
            #     break
            eval_no_query_based_method(method, ds_name, df_copy)
        no_query_methods[i] = None
print("*" * 100)
