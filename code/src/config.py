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
saved_model_base_path = os.path.join(res_dir, 'saved_best_models')
pubmed_infer_embedding_file = os.path.join(res_dir, 'pubmed_all_paper_bert_embedding.tsv')

# model config
import torch

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# device = torch.device("cpu")
concise_vector_len = 10
epochs = 12
batch_size = 64
warmup_steps = 100
evaluation_steps = 5000
# TODO
best_model_used_to_infer_entire_pubmed = ''

# metrics config
set_list_length = 60

# action config
action_availables = ['evaluate', 'fine_tune_evaluate', 'compress_vector_fine_tune_evaluate', 'infer']
to_do_what = action_availables[1]
