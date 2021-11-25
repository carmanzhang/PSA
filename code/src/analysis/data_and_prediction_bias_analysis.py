'''
This script analysis the potential bias in the training data and the predictions
We indent to investigate whether there is bias in training, which finally results in predictions bias.
'''
import json
# Note loaded the dataset and its corresponding research area
import numpy as np
import os
from matplotlib import pyplot as plt

from config import latex_doc_base_dir
from metric.all_metric import eval_metrics
from myio.data_reader import DBReader

JD_ST = DBReader.tcp_model_cached_read(cached_file_path='XX', sql='''
select groupUniqArray(pm_id) as pm_ids, arrayJoin(arrayMap(x->x.2, JDs)) as JD from sp.eval_data_relish_v1_related_JD_ST group by JD;
''', cached=False).values

print(JD_ST.shape)

# sorted by area size
JD_ST = np.array(sorted(JD_ST, key=lambda x: len(x[0]), reverse=True))
areas = [n[1] for n in JD_ST]
area_sizes = [len(n[0]) for n in JD_ST]


# JD_ST = {jd: pm_ids for pm_ids, jd in JD_ST}

def split_predictions_into_areas(based_prediction, map_idx_id=0):
    area_perf = []
    for pm_ids, area in JD_ST:
        area_predictions = [based_prediction[pm_id] for pm_id in pm_ids if pm_id in based_prediction]
        num_predictions = len(area_predictions)
        topns, num_queries, maps, mrrs, ndcgs, _, _, _, _ = eval_metrics(area_predictions, method_name=None,
                                                                         saved_result=False)
        map = float(maps[map_idx_id])
        area_perf.append([area, num_predictions, map])
    # area_perf = sorted(area_perf, key=lambda x: x[1], reverse=True)
    area_perf = np.array(area_perf)
    return area_perf


# Note loaded the prediction data, predicted by original sentence-transformer model
# Note loaded the prediction data, predicted by fine-tuning sentence-transformer model
based_path = '/home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/ranking_result'
prediction_files = {
    'BioBERT': ['sbert-origin-biobert-v1.1_relish_v1.json',
                'sbert-biobert-v1.1-relish_v1_relish_v1.json'],
    'SPECTER': ['sbert-origin-specter_relish_v1.json',
                'sbert-allenai-specter-relish_v1_relish_v1.json'],
}

colors = ['green', 'black', 'red', 'cyan', 'blue', 'magenta', 'purple', 'gray', 'fuchsia', 'orange', 'yellow']
linestyles = [':', '-.', '--', '--']
line_markers = ['<', '>', '^', 'v']
linewidth = 2.5

# Note plot area size distribution
plt.bar(range(len(areas)), area_sizes, linestyle=linestyles[1], color=colors[1], linewidth=linewidth)
plt.xlabel('Journal descriptor order', fontsize=12)
plt.ylabel('Frequency', fontsize=12)
# plt.xticks(np.arange(1, len(areas) + 1, 1))
# Note add figure border
for border in ['top', 'bottom', 'left', 'right']:
    plt.gca().spines[border].set_linewidth(1.5)  # change width
plt.savefig(os.path.join(latex_doc_base_dir, 'figures/dataset-relishv1-bias.pdf'), dpi=600)
plt.show()

ordered_map_metrics = ['MAP@5', 'MAP@10', 'MAP@15']

for map_idx_id, metric_name in enumerate(ordered_map_metrics):
    plt.figure(21, figsize=(12, 6), dpi=600)

    for method_idx, item in enumerate(prediction_files.items()):
        model_name, (based_prediction_file_path, tuned_prediction_file_path) = item
        print(metric_name, model_name)

        based_prediction_file_path = os.path.join(based_path, based_prediction_file_path)
        tuned_prediction_file_path = os.path.join(based_path, tuned_prediction_file_path)

        based_area_perf = split_predictions_into_areas(json.load(open(based_prediction_file_path, 'r')), map_idx_id)
        assert areas == list(based_area_perf[:, 0])
        tuned_area_perf = split_predictions_into_areas(json.load(open(tuned_prediction_file_path, 'r')), map_idx_id)
        assert areas == list(tuned_area_perf[:, 0])
        # print(based_area_perf)
        print()
        # print(tuned_area_perf)

        plt.subplot(211 + method_idx)
        plt.bar(range(len(areas)),
                list(tuned_area_perf[:, 2].astype(np.float) - based_area_perf[:, 2].astype(np.float)),
                linestyle=linestyles[1],
                # marker=line_markers[idx], markersize=8, markevery=0.2,
                color=colors[1], linewidth=linewidth)
        # idx = 2
        # plt.plot(areas, list(tuned_area_perf[:, 2].astype(np.float)), linestyle=linestyles[idx],
        #           # marker=line_markers[idx], markersize=8, markevery=0.2,
        #           color=colors[idx], label='Fine-tuning', linewidth=linewidth)

        # plt.yscale('log')
        # plt.title('journal descriptor distribution', fontsize=18)
        plt.xlabel('%s' % model_name, fontsize=12)
        plt.ylabel('%s Difference' % metric_name, fontsize=12)
        # plt.legend(loc='best')

        # Note set invisible to x axis
        # plt.xticks([])

    plt.tight_layout()

    metric_name = metric_name.lower().replace('@', '')
    plt.savefig(os.path.join(latex_doc_base_dir, 'figures/prediction-%s-bias.pdf' % metric_name), dpi=600)
    plt.show()
