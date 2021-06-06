import nni
import pandas as pd

from config import *
from model.action_processing import ActionProcessor
from myio.data_reader import DBReader

params = nni.get_next_parameter()
if params is not None and len(params) > 0:
    params = ModelConfig.from_dict(params)
    print('use parameters from NNI')
else:
    params = ModelConfig
    print('use default parameters')

model_param_spec = params.one_line_string_config()
print(model_param_spec)

# >>>>> Step 1. load dataset <<<<<<
df = DBReader.tcp_model_cached_read("",  # development_path
                                    """select concat(clean_title, ' ', clean_abstract)         as content1,
                                           concat(rcm_clean_title, ' ', rcm_clean_abstract) as content2,
                                           score                                            as label_or_score,
                                           train1_test0_val2
                                    from sp.train_val_set_pairwise_sim_score
                                    order by rand();""", cached=False)

use_columns = ['content1', 'content2', 'label_or_score']
df_train, df_val, df_test = df[df['train1_test0_val2'] == 1][use_columns], df[df['train1_test0_val2'] == 2][
    use_columns], df[
                                df['train1_test0_val2'] == 0][use_columns]
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
        elif to_do_what == Action.COMPRESS_VECTOR_FINE_TUNE_EVALUATE:
            processor.model.max_seq_length = params.max_seq_length
            print('updated max_seq_length: ', processor.model.max_seq_length)
            res = processor.rebuild_model(concise_vector_len=params.concise_vector_len).fine_tune(
                save_model_path=os.path.join(save_model_dir, 'compressed_' + model_param_spec),
                model_config=params).evaluate()
            print(res)

        elif to_do_what == Action.INFER:
            for remain_data in range(0, 10):
                sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as content from fp.paper_clean_content where pm_id % 10 == " + str(
                    remain_data)
                print(sql)
                # sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as content from fp.paper_clean_content limit 60000"
                df = DBReader.tcp_model_cached_read("cached/XXXX", sql, cached=False)
                print('loaded data partition: %d' % remain_data)
                print('df.shape', df.shape)
                sent_eb = processor.reload_model(best_model_used_to_infer_entire_pubmed).infer(
                    df[['pm_id', 'content']].tolist(), infer_batch_size=320000, batch_size=640)
                pd.DataFrame(sent_eb, index=None, columns=['pm_id', 'embedding']).to_csv(pubmed_infer_embedding_file,
                                                                                         index=False, header=False,
                                                                                         sep='\t', mode='a')
    except Exception as e:
        print(e)
