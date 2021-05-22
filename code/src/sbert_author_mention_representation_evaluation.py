import numpy as np
from scipy.spatial import distance
from sentence_transformers import SentenceTransformer
from sentence_transformers import models
from sklearn import metrics
from scipy import stats
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
        print('Transformer')
    return model


which_model_in_use = 0
# which_model_in_use = 0 # 0.9126434206525741 0.698 0.7644305772230889 pearsonr: (0.7035041423855313, 6.951090610889304e-76) spearmanr:  SpearmanrResult(correlation=0.7146693548241881, pvalue=2.4594977665298487e-79)
# which_model_in_use = 1 # 0.7767372187171221 0.7174 0.6990415335463259 pearsonr: (0.46265635544012074, 9.497228333197123e-264) spearmanr:  SpearmanrResult(correlation=0.47908355663466823, pvalue=2.4377890009929885e-285)
# which_model_in_use = 2 # 0.7272645890583562 0.4988 0.6655098772023492 pearsonr: (0.30175911668769256, 8.978748705096124e-106) spearmanr:  SpearmanrResult(correlation=0.3936330356618144, pvalue=5.495214509282289e-185)
model = get_model(candidate_models[which_model_in_use])
print('loaded model')

df = DBReader.tcp_model_cached_read("cached/XXXX",
                                    "select lowerUTF8(concat(paper_title1, ' ', abstract1)) as content1, lowerUTF8(concat(paper_title2, ' ', abstract2)) as content2, same_author, train1_test0_val2 from and_ds.our_and_dataset_pairwise order by rand();",
                                    cached=False)
df = df[df['train1_test0_val2'] == 0]
print('df.shape', df.shape)


# sent_eb1 = model.encode(df['content1'].values, batch_size=batch_size)
# sent_eb2 = model.encode(df['content2'].values, batch_size=batch_size)
def report_metric(sent_eb1, sent_eb2, label):
    assert len(sent_eb1) == len(sent_eb2)
    l = len(sent_eb1)

    sim = np.array([1 - distance.cosine(sent_eb1[i], sent_eb2[i]) for i in range(l)])
    # label = df['same_author'].values
    roc_auc = metrics.roc_auc_score(label, sim)
    p = stats.pearsonr(sim, label)
    sp = stats.spearmanr(sim, label)
    pred = [1 if n >= 0.5 else 0 for n in sim]
    acc = metrics.accuracy_score(label, pred)
    f1 = metrics.f1_score(label, pred)
    print(roc_auc, acc, f1, 'pearsonr:', p, 'spearmanr: ', sp)


infer_batch_size = 3200
batch_size = 64

cnt = 0
sent_batch1, sent_batch2 = [], []
sent_eb = None
sent_eb1, sent_eb2, batch_label = [], [], []
label = []
for i, (content1, content2, same_author, _) in df.iterrows():
    cnt += 1
    sent_batch1.append(content1)
    sent_batch2.append(content2)
    batch_label.append(same_author)
    if cnt % infer_batch_size == 0:
        tmp_eb1 = model.encode(sent_batch1, batch_size=batch_size)
        tmp_eb2 = model.encode(sent_batch2, batch_size=batch_size)

        sent_eb1.extend(tmp_eb1)
        sent_eb2.extend(tmp_eb2)
        label.extend(batch_label)

        report_metric(tmp_eb1, tmp_eb2, batch_label)
        batch_label, sent_batch1, sent_batch2 = [], [], []

print('final metrics: ')
report_metric(sent_eb1, sent_eb2, label)
