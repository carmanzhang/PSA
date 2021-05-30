import pandas as pd

from config import *
from model.action_processing import ActionProcessor
from myio.data_reader import DBReader

# >>>>> Step 1. load dataset <<<<<<
df = DBReader.tcp_model_cached_read("cached/XXXX",
                                    """select concat(clean_title, ' ', clean_abstract)         as content1,
                                           concat(rcm_clean_title, ' ', rcm_clean_abstract) as content2,
                                           score                                            as label_or_score,
                                           train1_test0_val2
                                    from sp.pubmed_similar_paper_dataset_for_cosin_similarity_loss_evaluation
                                    order by rand();""", cached=False)

use_columns = ['content1', 'content2', 'label_or_score']
df_train, df_val, df_test = df[df['train1_test0_val2'] == 1][use_columns], df[df['train1_test0_val2'] == 2][
    use_columns], df[
                                df['train1_test0_val2'] == 0][use_columns]
print('load train/val/test data', df_train.shape, df_val.shape, df_test.shape)

# >>>>> Step 2. choose an action in config.py and to do it <<<<<<
print('available models are: ', available_models)
print()
for idx, model_name_or_path in enumerate(available_models):
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

        if to_do_what == 'evaluate':
            print('evaluation metrics: ')
            res = processor.evaluate()
            print(res)

        elif to_do_what == 'fine_tune_evaluate':
            res = processor.fine_tune(
                save_model_path=os.path.join(save_model_dir, 'tuned'),
                optimizer_params=optimizer_params,
                epochs=epochs,
                batch_size=batch_size,
                warmup_steps=warmup_steps,
                evaluation_steps=evaluation_steps).evaluate()
            print(res)
            print()
        elif to_do_what == 'compress_vector_fine_tune_evaluate':
            res = processor.rebuild_model(concise_vector_len=concise_vector_len).fine_tune(
                save_model_path=os.path.join(save_model_dir, 'compressed_%d' % concise_vector_len),
                optimizer_params=optimizer_params,
                epochs=epochs,
                batch_size=batch_size,
                warmup_steps=warmup_steps,
                evaluation_steps=evaluation_steps).evaluate()
            print(res)

        elif to_do_what == 'infer':
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