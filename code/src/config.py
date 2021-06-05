import os

# database config
db_host = '127.0.0.1'
db_tcp_port = '9001'
db_http_port = '8124'
db_name = 'sp'
db_passwd = 'root'

# resource config
src_base_path = os.path.dirname(os.path.abspath(__file__))
proj_base_path = os.path.abspath(os.path.join(src_base_path, os.pardir))
# print(src_base_path)
saved_result_path = os.path.join(src_base_path, 'result')
# print(proj_base_path)
res_dir = os.path.join(proj_base_path, 'data')
development_path = os.path.join(res_dir, 'pubmed_similar_paper_development_dataset.pkl')
saved_model_base_path = os.path.join(res_dir, 'saved_best_models')
pubmed_infer_embedding_file = os.path.join(res_dir, 'pubmed_all_paper_bert_embedding.tsv')

# model config
concise_vector_len = 10
epochs = 12
batch_size = 8
max_seq_length = 300
warmup_steps = 3000
evaluation_steps = 5000
optimizer_params = {'lr': 2e-5}

"""
which pre-trained model can we use?, please refer to https://www.sbert.net/docs/pretrained_models.html
- Average Word Embeddings Models
    The following models apply compute the average word embedding for some well-known word embedding methods. Their computation speed is much higher than the transformer based models, but the quality of the embeddings are worse.
    average_word_embeddings_glove.6B.300d
    average_word_embeddings_komninos
    average_word_embeddings_levy_dependency
    average_word_embeddings_glove.840B.300d

- Semantic Textual Similarity https://github.com/UKPLab/sentence-transformers
    Sentence Transformer Models (NLI + STS benchmark). The following models were optimized for Semantic Textual Similarity (STS). They were trained on SNLI+MultiNLI and then fine-tuned on the STS benchmark train set.
    stsb-bert-base                                 21-Dec-2020 20:24           405233603
    stsb-distilbert-base                           21-Dec-2020 20:26           244715968

    stsb-roberta-large 86.39
    stsb-distilroberta-base-v2	86.41
    stsb-roberta-base-v2	87.21
    stsb-mpnet-base-v2	88.57

- SPECTER is a model trained on scientific citations and can be used to estimate the similarity of two publications. We can use it to find similar papers.
    allenai-specter - Semantic Search Python Example / Semantic Search Colab Example

- SciBert https://huggingface.co/allenai/
    biomed_roberta_base
    https://huggingface.co/allenai/scibert_scivocab_uncased
    
- BioBert https://github.com/dmis-lab/biobert
    BioBERT-Base v1.1 (+ PubMed 1M) - based on BERT-base-Cased (same vocabulary)
    BioBERT-Large v1.1 (+ PubMed 1M) - based on BERT-large-Cased (custom 30k vocabulary), NER/QA Results
    BioBERT-Base v1.0 (+ PubMed 200K) - based on BERT-base-Cased (same vocabulary)
    BioBERT-Base v1.0 (+ PMC 270K) - based on BERT-base-Cased (same vocabulary)
    BioBERT-Base v1.0 (+ PubMed 200K + PMC 270K) - based on BERT-base-Cased (same vocabulary)
"""

# TODO available models
ukplab_archived_model = [
    # 'average_word_embeddings_glove.6B.300d',
    # 'average_word_embeddings_komninos',
    # 'average_word_embeddings_levy_dependency',
    # 'average_word_embeddings_glove.840B.300d',

    'stsb-bert-base',
    'stsb-distilbert-base',
    'stsb-roberta-large',
    'stsb-distilroberta-base-v2',
    'stsb-roberta-base',
    'stsb-roberta-base-v2', # batch8
    'stsb-mpnet-base-v2', # batch8

    'allenai-specter'
]

supply_models = [
    '/home/zhangli/pre-trained-models/scibert_scivocab_uncased',  # batch8
    '/home/zhangli/pre-trained-models/biobert-v1.1'
]

pretrained_models = ukplab_archived_model + supply_models

# TODO
saved_tuned_models = []
for d in os.listdir(saved_model_base_path):
    subdir = os.path.join(saved_model_base_path, d)
    for sd in os.listdir(subdir):
        model_path = os.path.join(subdir, sd)
        if len(os.listdir(model_path)) > 0:
            # print('1', model_path)
            saved_tuned_models.append(model_path)
        else:
            # print('0', model_path)
            pass

# TODO use which to evaluate
# models_in_use = saved_tuned_models
models_in_use = pretrained_models

best_model_used_to_infer_entire_pubmed = ''

# metrics config
set_list_length = 60

# action config
action_availables = ['evaluate', 'fine_tune_evaluate', 'compress_vector_fine_tune_evaluate', 'infer']
to_do_what = action_availables[1]

if 'tune' in to_do_what:
    assert models_in_use != saved_tuned_models
