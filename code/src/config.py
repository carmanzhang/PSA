# database config
from enum import Enum

import os

# Note device config
# 0 _CudaDeviceProperties(name='GeForce RTX 3090', major=8, minor=6, total_memory=24268MB, multi_processor_count=82)
# 1 _CudaDeviceProperties(name='GeForce RTX 3090', major=8, minor=6, total_memory=24268MB, multi_processor_count=82)
# 2 _CudaDeviceProperties(name='Tesla V100-PCIE-16GB', major=7, minor=0, total_memory=16160MB, multi_processor_count=80)
# 3 _CudaDeviceProperties(name='GeForce RTX 2080 Ti', major=7, minor=5, total_memory=11019MB, multi_processor_count=68)
# 4 _CudaDeviceProperties(name='GeForce RTX 2080 Ti', major=7, minor=5, total_memory=11019MB, multi_processor_count=68)

gpu_id = 0
device = "cuda:%d" % gpu_id


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
        # return [cls.RELISHV1, cls.TREC2005, cls.TREC2014]
        return [cls.RELISHV1, cls.TREC2005]


which_datasets = AvailableDataset.aslist()

# resource config
latex_doc_base_dir = '/home/zhangli/mydisk-2t/repo/manuscripts/ongoning-works/similar-article-recommendation-evaluation/src/'
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
saved_model_parameter_ablation_base_path = os.path.join(res_dir, 'saved_best_models_parameter_ablation')
pubmed_infer_embedding_file = os.path.join(res_dir, 'pubmed_all_paper_bert_embedding.tsv')
pmra_cached_data_dir = os.path.join(res_dir, 'pmra-cached-data')
ranking_result_dir = os.path.join(res_dir, 'ranking_result')

# Note simple model config

pretrained_model_path = proj_base_path = os.path.abspath('/home/zhangli/pre-trained-models/')
glove840b300d_path = os.path.join(pretrained_model_path, 'glove.840B/glove.840B.300d.txt')
fasttextcrawl300d2m_path = os.path.join(pretrained_model_path, 'fastText/crawl-300d-2M.vec')
infersent_based_path = os.path.join(pretrained_model_path, 'infersent')

num_lda_topics = 64
lda_based_path = os.path.join(model_dir, 'lda')
doc2vec_based_path = os.path.join(model_dir, 'doc2vec')


# Note model config

class ModelConfig:
    loss = ['COSIN', 'TRIPLET', 'CONTRASTIVE'][1]
    epoch = 2
    batch_size = 16
    optimizer_params = {'lr': 1e-5}
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

# TODO use which to evaluate
# models_in_use = saved_tuned_models
# models_in_use = pretrained_models
models_in_use = ['allenai-specter'] + supply_models

# metrics config
top_n_interval = 5
set_list_length = 50
