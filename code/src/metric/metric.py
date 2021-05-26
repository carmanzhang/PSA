import json
from collections import Counter
from itertools import chain, groupby
from random import random

import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, accuracy_score, precision_score, recall_score, classification_report, \
    roc_auc_score

from metric import rank_metrics

metric_names = ['acc', 'p', 'r', 'f1', 'macro_f1', 'micro_f1']

pd.set_option('display.unicode.ambiguous_as_wide', True)  # 设置列名对齐
pd.set_option('display.unicode.east_asian_width', True)  # 设置列名对齐
pd.set_option('display.max_rows', None)  # 显示所有行
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('expand_frame_repr', False)  # 设置不换行

field_split = '#'
set_list_length = 60


def pretty_metrics(kv: list, decimal=2, pctg=False, sep=field_split):
    k = [item[0] for item in kv]
    if pctg:
        v = [round(item[1] * 100.0, decimal) for item in kv]
    else:
        v = [round(item[1], decimal) for item in kv]
    if not sep:
        df = pd.DataFrame(data=[v], columns=k)
        return df.head()
    else:
        return sep.join([str(s) for s in v])


def calc_confuse_matrix_based_metrics(test_y, pred_y):
    pred_y_label = [1 if i > 0.5 else 0 for i in pred_y]

    acc = accuracy_score(test_y, pred_y_label)
    p = precision_score(test_y, pred_y_label)
    r = recall_score(test_y, pred_y_label)
    f1 = f1_score(test_y, pred_y_label, average='binary')
    macro_f1 = f1_score(test_y, pred_y_label, average='macro')
    micro_f1 = f1_score(test_y, pred_y_label, average='micro')
    return metric_names, [acc, p, r, f1, macro_f1, micro_f1]


def tuple_group_rank(list1, list2, list3):
    all_query_ranks = [sorted(list(g), key=lambda x: -1.0 * x[1]) for k, g in
                       groupby(sorted(zip(list1, list2, list3), key=lambda x: x[0]), lambda s: s[0])]
    return all_query_ranks


def citation_mesh_group_rank(citing_pm_id, pred, ground_truth, candidate_mesh_citation):
    all_query_ranks = [sorted(list(g), key=lambda x: -1.0 * x[1]) for k, g in
                       groupby(
                           sorted(zip(citing_pm_id, pred, ground_truth, candidate_mesh_citation), key=lambda x: x[0]),
                           lambda s: s[0])]
    return all_query_ranks


def report_metric(preds, reals):
    preds = [1 if n > 0.5 else 0 for n in preds]
    return classification_report(reals, preds, digits=4)


def report_acc(preds, reals):
    preds = preds.cpu().numpy()
    print('num of instances [0.1-0.9]: ', len([n for n in preds if n < 0.9 and n > 0.1]))
    preds = [1 if n > 0.5 else 0 for n in preds]
    reals = reals.cpu().numpy()
    return accuracy_score(reals, preds)


def linear_scale(data):
    data = np.array(data)
    scaled_data = (data - np.min(data)) / (np.max(data) - np.min(data))
    return scaled_data


def report_ir_metrics(positions_scores, need_scale=False, check_length=True, list_length=set_list_length,
                      check_sum=True,
                      list_sum=1):
    try:
        l = len(positions_scores)
        if check_sum:
            positions_scores = [n for n in positions_scores if sum(n[0]) == list_sum]
            if l != len(positions_scores):
                print('remove sum value broken query result: ', l - len(positions_scores))
        l = len(positions_scores)
        if check_length:
            positions_scores = [n for n in positions_scores if len(n[0]) == list_length]
            if l != len(positions_scores):
                print('remove length value broken query result: ', l - len(positions_scores))

        group_roc_auc = np.average(
            [roc_auc_score(positions, linear_scale(scores) if need_scale else scores) for positions, scores in
             positions_scores])
        positions = list(chain.from_iterable([positions for positions, scores in positions_scores]))
        scores = list(chain.from_iterable([scores for positions, scores in positions_scores]))
        assert len(positions) == len(scores)
        print('supportive cases of roc-auc: ', len(positions))
        # idx = np.argsort(scores)
        # positions = np.array(positions)[idx]
        # scores = np.array(scores)[idx]
        scores = linear_scale(scores) if need_scale else scores
        roc_auc = roc_auc_score(positions, scores)
        ir_metrics_names, ir_metrics = calc_confuse_matrix_based_metrics(positions, scores)
        # ir_metrics.extend([('auc', roc_auc), ('gauc', group_roc_auc)])
        ir_metrics_names = ir_metrics_names + ['auc', 'gauc']
        ir_metrics = ir_metrics + [roc_auc, group_roc_auc]
        ir_metrics_in_string = json.dumps({
            'metric_name': field_split.join(ir_metrics_names),
            'value': pretty_metrics(list(zip(ir_metrics_names, ir_metrics)), decimal=2, pctg=True, sep=field_split)
        }, indent=4, sort_keys=True).replace('#', '\t')
        return dict(zip(ir_metrics_names, ir_metrics)), ir_metrics_in_string
    except Exception as e:
        print(e)
        return {}, ''



def report_ranking_metrics(qid, preds, reals, print_intermediate_result=True):
    all_queries = sorted(list(zip(qid, reals, preds)), key=lambda x: x[0])
    pred_real_batch = [sorted(list(g), key=lambda x: -1.0 * x[2]) for k, g in
                       groupby(all_queries, lambda s: s[0])]
    all_query_ranks = [[m[1] for m in n] for n in pred_real_batch]
    for n in all_query_ranks:
        if sum(n) != 1:
            print('do not have a positive sample: ', sum(n), list(n))
        if len(n) != set_list_length:
            print('do not enough length: ', len(n), list(n))
        if print_intermediate_result and random() < 0.002:
            print(n)

    print('query distribution: ', Counter([len(n) for n in all_query_ranks]))
    topns, num_queries, maps, mrrs, ndcgs, ranking_metrics_in_string = get_ranking_metric_values(all_query_ranks)
    return topns, num_queries, maps, mrrs, ndcgs, ranking_metrics_in_string


def get_ranking_metric_values(all_query_ranks, check_length=True, list_length=set_list_length, check_sum=True,
                              list_sum=1):
    l = len(all_query_ranks)
    if check_sum:
        all_query_ranks = [n for n in all_query_ranks if sum(n) == list_sum]
        if l != len(all_query_ranks):
            print('remove sum value broken query result: ', l - len(all_query_ranks))
    l = len(all_query_ranks)
    if check_length:
        all_query_ranks = [n for n in all_query_ranks if len(n) == list_length]
        if l != len(all_query_ranks):
            print('remove length value broken query result: ', l - len(all_query_ranks))
    print('query distribution: ', Counter([len(n) for n in all_query_ranks]))
    topns, num_queries, maps, mrrs, ndcgs = [], [], [], [], []
    for i, top_n in enumerate(list(range(10, set_list_length + 1, 10))):
        # query result 的长度不一致，会对结果不公平，这里过滤掉这样的结果，截断过长的结果，只取topn个结果
        all_query_ranks_topn = [n[:top_n] for n in all_query_ranks if len(n) >= top_n]
        num_query = len(all_query_ranks_topn)
        # print('evaluate %s queries after delete shorter query' % len(all_query_ranks))
        map = rank_metrics.mean_average_precision(all_query_ranks_topn)
        mrr = rank_metrics.mean_reciprocal_rank(all_query_ranks_topn)
        ndcg = np.average([rank_metrics.ndcg_at_k(r, k=top_n) for r in all_query_ranks_topn])

        topns.append(top_n)
        num_queries.append(num_query)
        maps.append(map)
        mrrs.append(mrr)
        ndcgs.append(ndcg)

    ranking_metrics_in_string = json.dumps({
        'top_n': pretty_metrics(list(zip(topns, topns)), decimal=4, pctg=False, sep=field_split),
        'num_query': pretty_metrics(list(zip(topns, num_queries)), decimal=4, pctg=False, sep=field_split),
        'map': pretty_metrics(list(zip(topns, maps)), decimal=4, pctg=False, sep=field_split),
        'mrr': pretty_metrics(list(zip(topns, mrrs)), decimal=4, pctg=False, sep=field_split),
        'ndcg': pretty_metrics(list(zip(topns, ndcgs)), decimal=4, pctg=False, sep=field_split)
    }, indent=4, sort_keys=True).replace('#', '\t')

    return topns, num_queries, maps, mrrs, ndcgs, ranking_metrics_in_string
