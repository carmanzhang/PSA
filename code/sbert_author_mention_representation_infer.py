import pandas as pd
from sentence_transformers import SentenceTransformer
from sentence_transformers import models

from myio.data_reader import DBReader

# gc.collect()
# torch.cuda.empty_cache()
# torch.multiprocessing.set_start_method('spawn')
# Semantic Textual Similarity
# The following models were optimized for Semantic Textual Similarity (STS). They were trained on SNLI+MultiNLI and then fine-tuned on the STS benchmark train set.
# The best available models for STS are:
# stsb-roberta-large - STSb performance: 86.39
# model = SentenceTransformer('bert-base-nli-stsb-mean-tokens')
# SPECTER is a model trained on scientific citations and can be used to estimate the similarity of two publications. We can use it to find similar papers.
# allenai-specter - Semantic Search Python Example / Semantic Search Colab Example
candidate_models = ['allenai-specter',
                    'stsb-roberta-base',
                    '/home/zhangli/pre-trained-models/biobert_v1.1_pubmed_pytorch/biobert_v1.1_pubmed']


def get_model(name_or_path):
    if not '/' in name_or_path:
        # 'saved_model0' 0.941366043201236 0.863125 0.8617424242424243 pearsonr: (0.7753791275957671, 0.0) spearmanr:  SpearmanrResult(correlation=0.7611997669339656, pvalue=0.0)
        # 'saved_model1' 0.948047280958716 0.8769375 0.8732212993368103 pearsonr: (0.7828897414509616, 0.0) spearmanr:  SpearmanrResult(correlation=0.7728296382912612, pvalue=0.0)
        print('SentenceTransformer')
        model = SentenceTransformer('../saved_model1')
    else:
        # BERT_BASE=/home/zhangli/pre-trained-models/biobert_v1.1_pubmed
        # pytorch_pretrained_bert convert_tf_checkpoint_to_pytorch $BERT_BASE/model.ckpt-1000000 $BERT_BASE/bert_config.json $BERT_BASE/pytorch_model.bin
        word_embedding_model = models.Transformer(name_or_path, max_seq_length=400)
        pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
        model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
    return model


which_model_in_use = 0  # 0.9126434206525741 0.698 0.7644305772230889 pearsonr: (0.7035041423855313, 6.951090610889304e-76) spearmanr:  SpearmanrResult(correlation=0.7146693548241881, pvalue=2.4594977665298487e-79)
# which_model_in_use = 1 # 0.7767372187171221 0.7174 0.6990415335463259 pearsonr: (0.46265635544012074, 9.497228333197123e-264) spearmanr:  SpearmanrResult(correlation=0.47908355663466823, pvalue=2.4377890009929885e-285)
# which_model_in_use = 2 # 0.7272645890583562 0.4988 0.6655098772023492 pearsonr: (0.30175911668769256, 8.978748705096124e-106) spearmanr:  SpearmanrResult(correlation=0.3936330356618144, pvalue=5.495214509282289e-185)
model = get_model(candidate_models[which_model_in_use])
print('loaded model')

for remainder in range(0, 10):
    sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as content from fp.paper_clean_content where pm_id % 10 == " + str(
        remainder)
    print(sql)
    # sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as content from fp.paper_clean_content limit 60000"
    df = DBReader.tcp_model_cached_read("cached/XXXX", sql, cached=False)
    print('loaded data partition: %d' % remainder)
    print('df.shape', df.shape)

    infer_batch_size = 320000
    batch_size = 640

    sent_batch = []
    sent_eb = []
    for i, (pm_id, content) in df.iterrows():
        i += 1
        sent_batch.append([pm_id, content])
        if i % infer_batch_size == 0:
            tmp_eb = model.encode([n[1] for n in sent_batch], batch_size=batch_size)
            assert len(tmp_eb) == len(sent_batch)
            sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
            sent_batch = []
    if len(sent_batch) > 0:
        tmp_eb = model.encode([n[1] for n in sent_batch], batch_size=batch_size)
        assert len(tmp_eb) == len(sent_batch)
        sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
    pd.DataFrame(sent_eb, index=None, columns=['pm_id', 'embedding']).to_csv(
        '../resources/pubmed_all_paper_bert_embedding.tsv', index=None, header=None, sep='\t', mode='a')
