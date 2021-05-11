import numpy as np
import torch
from scipy.spatial import distance
from sentence_transformers import SentenceTransformer, InputExample, losses
from sentence_transformers import evaluation
from sentence_transformers import models
from sklearn import metrics
from sklearn.decomposition import PCA
from torch.utils.data import DataLoader

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
candidate_models = ['allenai-specter', 'stsb-roberta-base',
                    '/home/zhangli/pre-trained-models/biobert_v1.1_pubmed_pytorch/biobert_v1.1_pubmed']


def get_model(name_or_path):
    if not '/' in name_or_path:
        model = SentenceTransformer(name_or_path)
    else:
        # BERT_BASE=/home/zhangli/pre-trained-models/biobert_v1.1_pubmed
        # pytorch_pretrained_bert convert_tf_checkpoint_to_pytorch $BERT_BASE/model.ckpt-1000000 $BERT_BASE/bert_config.json $BERT_BASE/pytorch_model.bin
        # max_seq_length specifies the maximum number of tokens of the input. The number of token is superior or equal to the number of words of an input.
        word_embedding_model = models.Transformer(name_or_path, max_seq_length=300)
        pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
        model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
    return model


which_model_in_use = 2  # default is 1
model = get_model(candidate_models[which_model_in_use])
print('loaded model')

df = DBReader.tcp_model_cached_read("cached/XXXX",
                                    """select lowerUTF8(concat(paper_title1, ' ', abstract1)) as content1, 
                                    lowerUTF8(concat(paper_title2, ' ', abstract2)) as content2, 
                                    same_author, train1_test0_val2 
                                    from and_ds.our_and_dataset_pairwise order by rand();""",
                                    cached=False)
print('df.shape', df.shape)

df_train = df[df['train1_test0_val2'] != 0]
df_eval = df[df['train1_test0_val2'] == 0]

#####################################
new_dimension = 10
# To determine the PCA matrix, we need some example sentence embeddings.
# Here, we compute the embeddings for 20k random sentences from the AllNLI dataset
pca_train_sentences = df_train['content1'].values[0:200000]
train_embeddings = model.encode(pca_train_sentences, convert_to_numpy=True)

# Compute PCA on the train embeddings matrix
pca = PCA(n_components=new_dimension)
pca.fit(train_embeddings)
pca_comp = np.asarray(pca.components_)

# We add a dense layer to the model, so that it will produce directly embeddings with the new size
dense = models.Dense(in_features=model.get_sentence_embedding_dimension(), out_features=new_dimension, bias=False,
                     activation_function=torch.nn.Identity())
dense.linear.weight = torch.nn.Parameter(torch.tensor(pca_comp))
model.add_module('dense', dense)
print('rebuild the model')

#####################################
# prepare train data for tuning
train_examples = [InputExample(texts=[content1, content2], label=float(same_author)) for
                  i, (content1, content2, same_author, _) in df_train.iterrows()]

# prepare evaluation data
evaluator = evaluation.EmbeddingSimilarityEvaluator(df_eval['content1'].values, df_eval['content2'].values,
                                                    df_eval['same_author'].values.astype('float'))

print('load train/val data')

# Define your train dataset, the dataloader and the train loss
train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=8)
train_loss = losses.CosineSimilarityLoss(model)

# Tune the model
model.fit(train_objectives=[(train_dataloader, train_loss)], epochs=12, warmup_steps=100, evaluator=evaluator,
          evaluation_steps=5000, save_best_model=True, output_path="../saved_model%d" % which_model_in_use,
          show_progress_bar=False)


def report_metric(sent_eb1, sent_eb2, label):
    assert len(sent_eb1) == len(sent_eb2)
    l = len(sent_eb1)

    sim = np.array([1 - distance.cosine(sent_eb1[i], sent_eb2[i]) for i in range(l)])
    # label = df['same_author'].values
    roc_auc = metrics.roc_auc_score(label, sim)
    pred = [1 if n >= 0.5 else 0 for n in sim]
    acc = metrics.accuracy_score(label, pred)
    f1 = metrics.f1_score(label, pred)
    print(roc_auc, acc, f1)
