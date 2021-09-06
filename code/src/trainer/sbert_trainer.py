import os

from config import ModelConfig, AvailableDataset, saved_model_base_path, Action, to_do_what, models_in_use, cached_dir
from model.action_processing import ActionProcessor
from myio.data_reader import DBReader

params = ModelConfig
model_param_spec = params.one_line_string_config()
print(model_param_spec)

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

available_datasets = AvailableDataset.aslist()
for ds in available_datasets:
    # >>>>> Note Step 0. making the running config <<<<<<
    ds_name = ds.value
    model_param_spec = ds_name + '-' + model_param_spec
    print('running config: %s' % model_param_spec)

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
                    save_model_path=os.path.join(save_model_dir, 'tuned_' + model_param_spec),
                    model_config=params).evaluate()
                print(res)
                print()
        except Exception as e:
            print(e)
