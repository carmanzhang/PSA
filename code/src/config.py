import os

# database config
from enum import Enum


class DBConfig:
    db_user = 'default'
    db_passwd = 'root'
    db_host = '127.0.0.1'
    db_tcp_port = '9001'
    db_http_port = '8124'
    db_in_use = 'sp'


# Note available datasets for evaluation

# action config
class AvailableDataset(Enum):
    RELISHV1 = 'relish_v1'
    TREC2005 = 'trec_genomic_2005'
    TREC2014 = 'trec_cds_2014'

    @classmethod
    def aslist(cls):
        return [cls.RELISHV1, cls.TREC2005, cls.TREC2014]


which_datasets = AvailableDataset.aslist()

# resource config
src_base_path = os.path.dirname(os.path.abspath(__file__))
proj_base_path = os.path.abspath(os.path.join(src_base_path, os.pardir))
# print(src_base_path)
saved_result_path = os.path.join(src_base_path, 'result')
# print(proj_base_path)
res_dir = os.path.join(proj_base_path, 'data')
cached_dir = os.path.join(res_dir, 'cached')
model_dir = os.path.join(proj_base_path, 'model')
eval_data_dir = os.path.join(res_dir, 'evaluation-datasets')
development_path = os.path.join(res_dir, 'pubmed_similar_paper_development_dataset.pkl')
saved_model_base_path = os.path.join(res_dir, 'saved_best_models')
pubmed_infer_embedding_file = os.path.join(res_dir, 'pubmed_all_paper_bert_embedding.tsv')
pmra_cached_data_dir = os.path.join(res_dir, 'pmra-cached-data')


# Note simple model config

pretrained_model_path = proj_base_path = os.path.abspath('/home/zhangli/pre-trained-models/')
glove840b300d_path = os.path.join(pretrained_model_path, 'GloVe/glove.840B.300d.txt')
fasttextcrawl300d2m_path = os.path.join(pretrained_model_path, 'fastText/crawl-300d-2M.vec')
infersent_based_path  = os.path.join(pretrained_model_path, 'infersent')

num_lda_topics = 64
lda_based_path = os.path.join(model_dir, 'lda')
word2vec_based_path = os.path.join(model_dir, 'word2vec.model')
doc2vec_based_path = os.path.join(model_dir, 'doc2vec.model')

# Note model config

class ModelConfig:
    loss = ['COSIN', 'TRIPLET'][1]
    epoch = 4
    batch_size = 8
    optimizer_params = {'lr': 2e-5}
    max_seq_length = 200
    concise_vector_len = 10
    warmup_steps = 1500
    evaluation_steps = 3000

    @staticmethod
    def from_dict(d: dict):
        ModelConfig.loss = d['loss'] if 'loss' in d else ModelConfig.loss
        ModelConfig.concise_vector_len = d[
            'concise_vector_len'] if 'concise_vector_len' in d else ModelConfig.concise_vector_len
        ModelConfig.epoch = d['epoch'] if 'epoch' in d else ModelConfig.epoch
        ModelConfig.batch_size = d['batch_size'] if 'batch_size' in d else ModelConfig.batch_size
        ModelConfig.max_seq_length = d['max_seq_length'] if 'max_seq_length' in d else ModelConfig.max_seq_length
        ModelConfig.warmup_steps = d['warmup_steps'] if 'warmup_steps' in d else ModelConfig.warmup_steps
        ModelConfig.evaluation_steps = d[
            'evaluation_steps'] if 'evaluation_steps' in d else ModelConfig.evaluation_steps
        ModelConfig.optimizer_params = {'lr': d['lr']} if 'lr' in d else ModelConfig.optimizer_params
        return ModelConfig

    @staticmethod
    def one_line_string_config():
        s = 'ls%s-ep%d-bs%d-lr%f-vl%d-sl%d' % (
            ModelConfig.loss, ModelConfig.epoch, ModelConfig.batch_size, ModelConfig.optimizer_params['lr'],
            ModelConfig.concise_vector_len, ModelConfig.max_seq_length)
        return s


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
    # TODO include bert-base in for comparison to increase the diversity of baselines
    # TODO add sota unspervised learning model TSDAE
    'stsb-bert-base',
    'stsb-distilbert-base',
    'stsb-roberta-large',
    'stsb-distilroberta-base-v2',
    'stsb-roberta-base',
    'stsb-roberta-base-v2',  # batch8
    'stsb-mpnet-base-v2',  # batch8

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
# models_in_use = pretrained_models
models_in_use =  ['allenai-specter'] + supply_models

best_model_used_to_infer_entire_pubmed = ''

# metrics config
top_n_interval = 5
set_list_length = 50

# action config
class Action(Enum):
    EVALUATE = 'evaluate'
    FINE_TUNE_EVALUATE = 'fine_tune_evaluate'
    COMPRESS_VECTOR_FINE_TUNE_EVALUATE = 'compress_vector_fine_tune_evaluate'
    INFER = 'infer'


to_do_what = Action.FINE_TUNE_EVALUATE

if 'tune' in to_do_what.value:
    assert models_in_use != saved_tuned_models
