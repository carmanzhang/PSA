import sys

sys.path.append("..")

import traceback

import os

from config import ModelConfig, AvailableDataset, saved_model_base_path, Action, to_do_what, models_in_use, cached_dir
from model.action_processing import ActionProcessor
from myio.data_reader import DBReader

# data_sql_template = """
# select id,
#        c_pos_pm_id,
#        c_pos_content,
#        c_neg_pm_id,
#        c_neg_content
# from (
#       select id,
#              arrayMap(i->
#                           [xxHash32(i, randomPrintableASCII(5)) %% num_pos + 1, xxHash32(i, randomPrintableASCII(5)) %% num_neg + 1],
#                       range(num_sampled_instances))                 as pos_neg_idx,
#              arrayJoin(arrayFilter(y->length(y[1].1) > 0 and length(y[2].1) > 0,
#                                    arrayDistinct(
#                                            arrayMap(idx->
#                                                         [pos_arr[idx[1]],neg_arr[idx[2]]],
#                                                     pos_neg_idx)))) as pos_neg_item,
#              tupleElement(pos_neg_item[1], 1)                       as c_pos_pm_id,
#              tupleElement(pos_neg_item[1], 2)                       as c_pos_content,
#              tupleElement(pos_neg_item[2], 1)                       as c_neg_pm_id,
#              tupleElement(pos_neg_item[2], 2)                       as c_neg_content
#       from (with ['relish_v1', 'trec_genomic_2005', 'trec_cds_2014'] as available_datasets,
#                 [1.8, 5, 3.5] as sampling_factors,
#                 indexOf(available_datasets, '%s') as dataset_idx,
#                 sampling_factors[dataset_idx] as dataset_sampling_factor
#             select id,
#                    -- [irrelevant -> partial -> relevant]
#                    -- [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str]
#                    arrayMap(x-> (tupleElement(x, 1), concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2])),
#                             %s[3])                                                        pos_arr,
#                    arrayMap(x-> (tupleElement(x, 1), concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2])),
#                             %s[1])                                                        neg_arr,
#                    length(pos_arr)                                                             as num_pos,
#                    length(neg_arr)                                                             as num_neg,
#                    toUInt32((num_pos > num_neg ? num_pos : num_neg) * dataset_sampling_factor) as num_sampled_instances
#             from sp.eval_data_%s_with_content_without_query
#             where num_pos > 0
#               and num_neg > 0)
#          )
# where length(c_pos_pm_id) > 0
#   and length(c_neg_pm_id) > 0;"""

data_sql_template = """
select id,
       pm_id1,
       content1,
       pm_id2,
       content2,
       score
from (with 5 as dataset_balance_ratio
      select id,
             arrayFilter(x->x[1] != x[2], arrayDistinct(
                     arrayMap(i->
                                  [xxHash32(i, randomPrintableASCII(5)) %% num_pos + 1,
                                      xxHash32(i, 'xxx', randomPrintableASCII(5)) %% num_pos + 1, 1],
                              range(num_sampled_pos_pos_instances))))                    as pos_pos_idx,

             arrayFilter(x->x[1] != x[2], arrayDistinct(
                     arrayMap(i->
                                  [xxHash32(i, randomPrintableASCII(5)) %% num_neg + 1,
                                      xxHash32(i, 'yyy', randomPrintableASCII(5)) %% num_neg + 1, 0],
                              range(num_sampled_neg_neg_instances))))                    as neg_neg_idx,

             length(pos_pos_idx) >
             length(neg_neg_idx) ?
             arrayConcat(arraySlice(pos_pos_idx, 1, length(neg_neg_idx) * dataset_balance_ratio), neg_neg_idx) :
             arrayConcat(pos_pos_idx, arraySlice(neg_neg_idx, 1, length(pos_pos_idx) *
                                                                 dataset_balance_ratio)) as pos_neg_idx,

             arrayJoin(arrayFilter(y->length(y.1.1) > 0 and length(y.2.1) > 0,
                                   arrayDistinct(
                                           arrayMap(idx->
                                                            idx[3] = 1 ?
                                                                     (pos_arr[idx[1]], pos_arr[idx[2]], 1) :
                                                                     (neg_arr[idx[1]], neg_arr[idx[2]], 0),
                                                    pos_neg_idx))))                      as pos_pos_or_neg_neg_pair_item,
             tupleElement(pos_pos_or_neg_neg_pair_item.1, 1)                             as pm_id1,
             tupleElement(pos_pos_or_neg_neg_pair_item.1, 2)                             as content1,
             tupleElement(pos_pos_or_neg_neg_pair_item.2, 1)                             as pm_id2,
             tupleElement(pos_pos_or_neg_neg_pair_item.2, 2)                             as content2,
             pos_pos_or_neg_neg_pair_item.3                                              as score
      from (with ['relish_v1', 'trec_genomic_2005', 'trec_cds_2014'] as available_datasets,
                [1.5, 12, 10] as sampling_factors,
                indexOf(available_datasets, '%s') as dataset_idx,
                sampling_factors[dataset_idx] as dataset_sampling_factor
            select id,
                   -- [irrelevant -> partial -> relevant]
                   -- [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str]
                   arrayMap(x-> (tupleElement(x, 1), concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2])),
                            %s[3])                        pos_arr,
                   arrayMap(x-> (tupleElement(x, 1), concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2])),
                            %s[1])                        neg_arr,
                   length(pos_arr)                             as num_pos,
                   length(neg_arr)                             as num_neg,
                   toUInt32(num_pos * dataset_sampling_factor) as num_sampled_pos_pos_instances,
                   toUInt32(num_neg * dataset_sampling_factor) as num_sampled_neg_neg_instances
            from sp.eval_data_%s_with_content_without_query
            where num_pos > 0
              and num_neg > 0)
         )
where length(pm_id1) > 0
  and length(pm_id2) > 0;"""

params = ModelConfig
# Note set the loss to CONTRASTIVE-LOSS
params.loss = 'CONTRASTIVE'
model_param_spec = params.one_line_string_config()
print(model_param_spec)

available_datasets = AvailableDataset.aslist()
for ds in available_datasets:
    # >>>>> Note Step 0. making the running config <<<<<<
    ds_name = ds.value
    running_config = '-'.join(['no-query', ds_name, model_param_spec])
    print('running config: %s' % running_config)

    # >>>>> Note Step 1. load dataset <<<<<<
    train_data_sql = data_sql_template % (ds_name, 'train_part', 'train_part', ds_name)
    val_data_sql = data_sql_template % (ds_name, 'val_part', 'val_part', ds_name)
    test_data_sql = data_sql_template % (ds_name, 'test_part', 'test_part', ds_name)

    print('>' * 30 + 'using the sql for dataset retrieval' + '<' * 30)
    print(train_data_sql)

    df_train = DBReader.tcp_model_cached_read(os.path.join(cached_dir, ds_name + '-train-no-query.pkl'),
                                              sql=train_data_sql, cached=True)
    df_val = DBReader.tcp_model_cached_read(os.path.join(cached_dir, ds_name + '-val-no-query.pkl'), sql=val_data_sql,
                                            cached=True)
    df_test = DBReader.tcp_model_cached_read(os.path.join(cached_dir, ds_name + '-test-no-query.pkl'),
                                             sql=test_data_sql, cached=True)

    print('load train/val/test data', df_train.shape, df_val.shape, df_test.shape)

    # >>>>> Note Step 2. choose an action in config.py and to do it <<<<<<
    print('available models are: ', models_in_use)
    print()
    for idx, model_name_or_path in enumerate(models_in_use):
        try:
            model_name = model_name_or_path[
                         model_name_or_path.rindex('/') + 1:] if '/' in model_name_or_path else model_name_or_path
            save_model_dir = os.path.join(saved_model_base_path, model_name)
            if not os.path.exists(save_model_dir):
                os.mkdir(save_model_dir)
            print(
                'loaded the %d-th model: \"%s\", may locate in \"%s\", and fine-tuned model will saved at \"%s\" if applicable' % (
                    idx + 1, model_name, model_name_or_path, save_model_dir))

            processor = ActionProcessor(model_name_or_path, [df_train, df_val, df_test])

            if to_do_what == Action.EVALUATE:
                print('max_seq_length: ', processor.model.max_seq_length)
                print('evaluation metrics: ')
                res = processor.evaluate()
                print(res)

            elif to_do_what == Action.FINE_TUNE_EVALUATE:
                processor.model.max_seq_length = params.max_seq_length
                print('updated max_seq_length: ', processor.model.max_seq_length)
                res = processor.fine_tune(
                    save_model_path=os.path.join(save_model_dir, 'tuned_' + running_config),
                    model_config=params).evaluate()
                print(res)
                print()
        except Exception as e:
            traceback.print_exc()
