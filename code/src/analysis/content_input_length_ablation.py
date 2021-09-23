import sys
from scipy.spatial import distance

sys.path.append("..")

import copy

import os
from tqdm import tqdm

from metric.all_metric import eval_metrics
from config import ModelConfig, AvailableDataset, saved_model_parameter_ablation_base_path, cached_dir
from model.action_processing import ActionProcessor
from myio.data_reader import DBReader

# model_name_or_path = 'allenai-specter'
model_name_or_path = 'dmis-lab/biobert-v1.1'
# model_name_or_path='allenai/scibert_scivocab_uncased'

all_max_seq_lengths = [200, 250, 300, 350, 400, 450, 500]
model_name = model_name_or_path[
             model_name_or_path.rindex('/') + 1:] if '/' in model_name_or_path else model_name_or_path
save_model_dir = os.path.join(saved_model_parameter_ablation_base_path, model_name)
if not os.path.exists(save_model_dir):
    os.mkdir(save_model_dir)

train_data_sql_template = """
select q_id,
       train1_val2_test0,
       q_pm_id,
       q_content,
       c_pos_pm_id,
       c_pos_content,
       c_neg_pm_id,
       c_neg_content
from (
      select q_id,
             train1_val2_test0,
             q_pm_id,
             q_content,
             arrayMap(i->
                          [xxHash32(i, randomPrintableASCII(5)) %% num_pos + 1, xxHash32(i, randomPrintableASCII(5)) %% num_neg + 1],
                      range(num_sampled_instances))                 as pos_neg_idx,
             arrayJoin(arrayFilter(y->length(y[1].1) > 0 and length(y[2].1) > 0,
                                   arrayDistinct(
                                           arrayMap(idx->
                                                        [pos_arr[idx[1]],neg_arr[idx[2]]],
                                                    pos_neg_idx)))) as pos_neg_item,
             tupleElement(pos_neg_item[1], 1)                       as c_pos_pm_id,
             tupleElement(pos_neg_item[1], 2)                       as c_pos_content,
             tupleElement(pos_neg_item[2], 1)                       as c_neg_pm_id,
             tupleElement(pos_neg_item[2], 2)                       as c_neg_content
      from (with ['relish_v1', 'trec_genomic_2005', 'trec_cds_2014'] as available_datasets,
                [7, 0.07, 0.07] as sampling_factors,
                indexOf(available_datasets, '%s') as dataset_idx,
                sampling_factors[dataset_idx] as dataset_sampling_factor
            select q_id,
                   train1_val2_test0,
                   q_pm_id,
                   concat(q_content[1], ' ', q_content[2])                                     as q_content,
                   arrayFilter(y->tupleElement(y, 3) in (2), arrayMap(x-> (tupleElement(x, 1),
                                                                              concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                                                              tupleElement(x, 3))
                       , c_tuples) as tmp_arr)                                                    pos_arr,
                   arrayFilter(y->tupleElement(y, 3) in (0), tmp_arr)                             neg_arr,
                   length(pos_arr)                                                             as num_pos,
                   length(neg_arr)                                                             as num_neg,
                   toUInt32((num_pos > num_neg ? num_pos : num_neg) * dataset_sampling_factor) as num_sampled_instances
            from sp.eval_data_%s_with_content
            where train1_val2_test0 in (1))
      where num_pos > 0
        and num_neg > 0
         );"""

val_test_data_sql_template = """
select q_id,
    train1_val2_test0,
    q_pm_id,
    q_content,
    c_tuples
from (
   select q_id,
          train1_val2_test0,
          q_pm_id,
          concat(q_content[1], ' ', q_content[2]) as q_content,
          arrayMap(x->
                       (tupleElement(x, 1),
                        concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2], ' ', tupleElement(x, 2)[3]),
                        tupleElement(x, 3))
              , c_tuples)                                            as c_tuples
   from sp.eval_data_%s_with_content
   where train1_val2_test0 in (0, 2));"""

ds = AvailableDataset.RELISHV1
params = ModelConfig
for max_seq_length in all_max_seq_lengths:
    params.max_seq_length = max_seq_length
    model_param_spec = params.one_line_string_config()
    print(model_param_spec)
    # >>>>> Note Step 0. making the running config <<<<<<
    ds_name = ds.value
    running_config = ds_name + '-' + model_param_spec
    print('running config: %s' % running_config)

    # >>>>> Note Step 1. load dataset <<<<<<
    train_data_sql = train_data_sql_template % (ds_name, ds_name)
    val_test_data_sql = val_test_data_sql_template % (ds_name)
    print('>' * 30 + 'using the sql for dataset retrieval' + '<' * 30)
    print(train_data_sql)
    print(val_test_data_sql)

    df_train = DBReader.tcp_model_cached_read(os.path.join(cached_dir, ds_name + '-train.pkl'),
                                              sql=train_data_sql,
                                              cached=True)
    df_val_test = DBReader.tcp_model_cached_read(os.path.join(cached_dir, ds_name + '-val-test.pkl'),
                                                 sql=val_test_data_sql,
                                                 cached=True)
    df_train = df_train[['q_content', 'c_pos_content', 'c_neg_content']]
    df_val = df_val_test[df_val_test['train1_val2_test0'] == 2].explode('c_tuples').reset_index(drop=True)
    df_test = df_val_test[df_val_test['train1_val2_test0'] == 0].explode('c_tuples').reset_index(drop=True)

    df_val['c_pm_id'], df_val['c_content'], df_val['score'] = df_val['c_tuples'].apply(lambda x: x[0]), df_val[
        'c_tuples'].apply(lambda x: x[1]), df_val['c_tuples'].apply(lambda x: x[2])
    del df_val['c_tuples']
    df_test['c_pm_id'], df_test['c_content'], df_test['score'] = df_test['c_tuples'].apply(lambda x: x[0]), df_test[
        'c_tuples'].apply(lambda x: x[1]), df_test['c_tuples'].apply(lambda x: x[2])
    del df_test['c_tuples']

    print('load train/val/test data', df_train.shape, df_val.shape, df_test.shape)

    # >>>>> Note Step 2. choose an action in config.py and to do it <<<<<<
    try:
        save_model_path = os.path.join(save_model_dir, 'tuned_' + running_config)
        if os.path.exists(save_model_path):
            continue
        print(
            'loaded the model: \"%s\", may locate in \"%s\", and fine-tuned model will saved at \"%s\" if applicable' % (
                model_name, model_name_or_path, save_model_dir))

        processor = ActionProcessor(model_name_or_path, [df_train, df_val, df_test])

        processor.model.max_seq_length = params.max_seq_length
        print('updated max_seq_length: ', processor.model.max_seq_length)

        res = processor.fine_tune(save_model_path=save_model_path, model_config=params).evaluate()
        print(res)
        print()
    except Exception as e:
        print(e)


def score(processor, q_content, c_contents):
    content_list = [q_content] + c_contents
    sent_ebs = [n[1] for n in processor.infer(content_list, batch_size=320)]
    q_sent_eb = sent_ebs[0]
    c_sent_ebs = sent_ebs[1:]
    scores = [1 - distance.cosine(q_sent_eb, c_sent_eb) for c_sent_eb in c_sent_ebs]
    return scores


def eval_query_based_method(processor, df_test):
    all_query_ranks = []
    items = df_test[['q_pm_id', 'q_content', 'c_tuples']].values
    for item in tqdm(items):
        q_pm_id, q_content, c_tuples = item
        # rcm_id, c_content, label
        rcm_ids = [rcm_id for rcm_id, c_content, label in c_tuples]
        c_contents = [c_content for rcm_id, c_content, label in c_tuples]
        orders = [label for rcm_id, c_content, label in c_tuples]

        scores = score(processor, q_content, c_contents)
        # scores = bm25_score(q_content, c_contents)

        assert len(scores) == len(orders)
        query_rank = sorted(zip(scores, orders), key=lambda x: x[0], reverse=True)
        all_query_ranks.append(query_rank)

    eval_metrics(all_query_ranks, '', saved_result=False)
    print('current dataset: ', ds.value, 'method: ', model_name)


sql_template = '''select  q_pm_id,
                                                       concat(q_content[1], ' ', q_content[2]) as q_content,
                                                       arrayMap(x->
                                                                    (tupleElement(x, 1),
                                                                     concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                                                     tupleElement(x, 3))
                                                           , c_tuples)                         as c_tuples
                                                from sp.eval_data_%s_with_content where train1_val2_test0 in (0);'''

models = [os.path.join(save_model_dir, model_dir) for model_dir in os.listdir(save_model_dir)]
ds_name = ds.value
for i, model_path in enumerate(models):
    sql = sql_template % ds_name
    print(sql)

    df = DBReader.tcp_model_cached_read(os.path.join(cached_dir, ds_name + '-test.pkl'), sql=sql, cached=True)
    df_copy = copy.deepcopy(df)
    processor = ActionProcessor(model_path, data=None)
    print('max_seq_length: ', processor.model.max_seq_length)
    eval_query_based_method(processor, df_copy)
