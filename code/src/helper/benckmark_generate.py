"""
read result metric files in this fold, and generate the benchmark
"""
import pandas as pd
from itertools import groupby

import numpy as np
import os

from config import AvailableDataset

dataset_names = [n.value for n in AvailableDataset.aslist()]

result_base_dir = '../result'
eval_entries = []
for file in os.listdir(result_base_dir):
    if '.md' in file or '.tsv' in file:
        print('skip: %s' % file)
        continue

    # a valid result file name should contain a specific dataset name
    eval_dataset = [1 if file.endswith(n) else 0 for n in dataset_names]
    if sum(eval_dataset) != 1:
        print('error')
        continue

    # Note parse evaluation dataset and method
    eval_dataset = dataset_names[eval_dataset.index(1)]
    eval_method = file[:file.index('_' + eval_dataset)]
    print(file, eval_method, eval_dataset)

    # Note parse metrics
    file_path = os.path.join(result_base_dir, file)
    lines = open(file_path, 'r').readlines()
    lines = ['\n' if n.startswith('---') else n for n in lines]
    content = ''.join(lines)
    # print(content)
    blocks = content.strip().split('\n\n')
    cate_metrics, ranking_metrics = blocks[-2].strip(), blocks[-1].strip()
    assert 'auc' in cate_metrics
    assert 'map' in ranking_metrics

    # Note parse classification metrics
    cate_metrics_names = [n.replace('"metric_name":', '').replace('"', '').replace(',', '').upper().strip() for n in cate_metrics.split('\n') if
                          '"metric_name":' in n]
    assert len(cate_metrics_names) == 1
    cate_metrics_names = cate_metrics_names[0].split('\t')

    cate_metrics = [n.replace('"value":', '').replace('"', '').strip() for n in cate_metrics.split('\n') if
                    '"value":' in n]
    assert len(cate_metrics) == 1
    cate_metrics = cate_metrics[0].split('\t')
    # "acc	p	r	f1	macro_f1	micro_f1	auc	gauc"
    assert len(cate_metrics_names) == len(cate_metrics) == 8

    # Note parse ranking metrics
    ranking_metrics = [n.replace('"', '').replace(',', '').strip().split(':') for n in ranking_metrics.split('\n') if
                       '":' in n]
    ranking_metrics_names = [m.upper() for m, n in ranking_metrics]
    ranking_metrics = np.array([n.split('\t') for m, n in ranking_metrics])
    ranking_metrics_names = np.array(
        [[n + '@' + ranking_metrics[-1][i] for i in range(ranking_metrics.shape[1])] for n in
         ranking_metrics_names[:-1]])
    ranking_metrics = ranking_metrics[:-1, ].reshape(-1)
    ranking_metrics_names = ranking_metrics_names.reshape(-1)
    assert len(ranking_metrics_names) == len(ranking_metrics)

    # print(ranking_metrics)
    eval_entries.append(
        [eval_dataset, eval_method, ranking_metrics_names, ranking_metrics, cate_metrics_names, cate_metrics])

# Note split by dataset name, then on each dataset, we sort the result by method name
eval_entries = {k: sorted(g, key=lambda x: x[1]) for k, g in
                groupby(sorted(eval_entries, key=lambda x: x[0]), key=lambda x: x[0])}
for eval_dataset, entries in eval_entries.items():
    print('>>>>>>>>>>evaluation on %s<<<<<<<<<<<<<<<' % eval_dataset)

    ranking_metrics_names = np.array(list(map(lambda x:x[2], entries)))
    ranking_metrics_names = np.unique(ranking_metrics_names, axis=0)
    assert ranking_metrics_names.shape[0] == 1

    cate_metrics_names = np.array(list(map(lambda x:x[4], entries)))
    cate_metrics_names = np.unique(cate_metrics_names, axis=0)
    assert cate_metrics_names.shape[0] == 1

    ranked_eval_methods = list(map(lambda x:x[1], entries))

    all_ranking_metrics = np.array(list(map(lambda x:x[3], entries)))
    all_cate_metrics = np.array(list(map(lambda x:x[5], entries)))

    all_metrics_names = np.concatenate((ranking_metrics_names, cate_metrics_names), axis=1)[0]
    all_metrics = np.concatenate((all_ranking_metrics, all_cate_metrics), axis=1)
    print(all_metrics_names.shape, all_metrics.shape)
    df = pd.DataFrame(all_metrics, columns=all_metrics_names, index=ranked_eval_methods)

    query_bench_df = df.loc[df.index.str.find('no-query') == -1]
    no_query_bench_df = df.loc[df.index.str.find('no-query') != -1]

    query_bench_df.to_csv('../result/eval-result-%s.tsv' % eval_dataset, sep='\t')
    query_bench_df.iloc[:, :5].to_markdown(open('../result/eval-result-%s.md' % eval_dataset, 'w'))

    no_query_bench_df.to_csv('../result/eval-result-%s-no-query.tsv' % eval_dataset, sep='\t')
    no_query_bench_df.iloc[:, :5].to_markdown(open('../result/eval-result-%s-no-query.md' % eval_dataset, 'w'))
    # df.iloc[:, :5].to_markdown(open('../result/eval-result.md', 'a'))