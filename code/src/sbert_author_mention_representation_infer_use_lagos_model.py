import pandas as pd
import torch
from torchtext import vocab

from model.nn import MatchGRU
from myio.data_reader import DBReader
import numpy as np
glove = vocab.GloVe(name='6B', dim=100)
pad_idx = 0
max_sql_len = 550
print(max_sql_len)

device = torch.device('cuda') if torch.cuda.is_available() else torch.device('cpu')
print('use device: ', device)

model = MatchGRU(glove, hidden_dim=64, num_layers=2,
                 # num_hand_craft_feature=len(train_set.num_hand_craft_feature_set),
                 bidirectional=True, output_dim=2).to(device)
print(model)

checkpoint = torch.load('cached/match-checkpoint.pkl')
model.load_state_dict(checkpoint['model_state_dict'])

print('loaded model')
print('Epoch:', checkpoint['epoch'])
print('losst:', checkpoint['losst'])
print('lossv:', checkpoint['lossv'])


def word_token(txt):
    words = txt.lower().split()
    tokens = [glove.stoi[word] for word in words if word in glove.stoi]
    tokens = tokens[:max_sql_len] if len(tokens) >= max_sql_len else tokens + [pad_idx] * (
            max_sql_len - len(tokens))
    return tokens

model.eval()
with torch.no_grad():
    for remainder in range(0, 10):
        # sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as content from fp.paper_clean_content where pm_id % 10 == " + str(remainder)
        sql = "select pm_id, concat(clean_title, ' ', clean_abstract) as content from fp.paper_clean_content limit 3000"
        print(sql)
        df = DBReader.tcp_model_cached_read("cached/XXXX", sql, cached=False)
        print('loaded data partition: %d' % remainder)
        print('df.shape', df.shape)

        infer_batch_size = 320
        batch_size = 32

        sent_batch = []
        sent_eb = []
        for i, (pm_id, content) in df.iterrows():
            i += 1
            sent_batch.append([pm_id, content])
            if i % infer_batch_size == 0:
                batch_tensor = torch.from_numpy(np.array([word_token(n[1]) for n in sent_batch])).to(device)
                tmp_eb = [n[1] for n in model([batch_tensor, batch_tensor]).sigmoid().cpu().numpy()]
                assert len(tmp_eb) == len(sent_batch)
                sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
                sent_batch = []
        if len(sent_batch) > 0:
            batch_tensor = torch.from_numpy(np.array([word_token(n[1]) for n in sent_batch])).to(device)
            tmp_eb = [n[1] for n in model([batch_tensor, batch_tensor]).sigmoid().cpu().numpy()]
            assert len(tmp_eb) == len(sent_batch)
            sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
        print(sent_eb)
        # pd.DataFrame(sent_eb, index=None, columns=['pm_id', 'embedding']).to_csv(
        #     '../resources/pubmed_all_paper_bert_embedding.tsv', index=None, header=None, sep='\t', mode='a')
