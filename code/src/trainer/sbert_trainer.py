import nni
import pandas as pd

from config import *
from model.action_processing import ActionProcessor
from myio.data_reader import DBReader

params = ModelConfig
model_param_spec = params.one_line_string_config()
print(model_param_spec)

# >>>>> Step 1. load dataset <<<<<<
df_train = DBReader.tcp_model_cached_read("",  # development_path
                                    """select q_id,
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
                                                     concat(q_content[1], ' ', q_content[2]) as q_content,
                                                     tupleElement(arrayJoin(arrayFilter(y->tupleElement(y, 3) in (1, 2) and xxHash32(y) % 100 < 25,
                                                                                        tmp_arr) as relevant_or_partial_tuples) as pos_item,
                                                                  1)                         as c_pos_pm_id,
                                                     tupleElement(pos_item, 2)               as c_pos_content,
                                                     tupleElement(arrayJoin(
                                                                          arrayFilter(y->tupleElement(y, 3) = 0 and xxHash32(y) % 100 < 25,
                                                                                      arrayMap(x-> (tupleElement(x, 1),
                                                                                                    concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                                                                                    tupleElement(x, 3))
                                                                                          , c_tuples) as tmp_arr) as irrelevant_tuples) as neg_item,
                                                                  1)                         as c_neg_pm_id,
                                                     tupleElement(neg_item, 2)               as c_neg_content
                                              from sp.eval_data_relish_v1_with_content
                                              where train1_val2_test0 in (1));""", cached=False)


df_val_test = DBReader.tcp_model_cached_read("",  # development_path
                                    """
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
      from sp.eval_data_relish_v1_with_content
      where train1_val2_test0 in (0, 2));""", cached=False)

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

# >>>>> Step 2. choose an action in config.py and to do it <<<<<<
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
