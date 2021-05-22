# crawler PubMed paper information from PubMed interface

import json
import time

from Bio import Entrez

from myio.data_reader import DBReader

Entrez.email = "13156518189@163.com"
existing_pm_ids = [json.loads(line.strip())['pm_id'] for line in open('data/pubmed_official_similar_paper_bulk.tsv')]
existing_pm_ids = set(existing_pm_ids)
print(len(existing_pm_ids))
df = DBReader.tcp_model_cached_read(cached_file_path='',
                                    sql='select pm_id from sp.pubmed_randomly_selected_papers order by function desc',
                                    cached=False)

pm_id_arr = df['pm_id'].values
print(len(pm_id_arr))
# filter out existing pm_ids
pm_id_arr = [n for n in pm_id_arr if n not in existing_pm_ids]
print(len(pm_id_arr))

# import pandas as pd
# pd.DataFrame(pm_id_arr, columns=['pm_id']).to_csv('data/pm_id_temp.tsv', sep='\t', index=None)

chunk_size = 500
pm_id_arr_chunks = [pm_id_arr[x:x + chunk_size] for x in range(0, len(pm_id_arr), chunk_size)]

fw = open('data/pubmed_official_similar_paper_bulk.tsv', 'a')
for i, pm_ids in enumerate(pm_id_arr_chunks):
    if i % 2 == 0:
        print(i)
    try:
        handle = Entrez.elink(db="pubmed", id=pm_ids, cmd="neighbor_score", rettype="json")
        records = Entrez.read(handle)
        if len(pm_ids) != len(records):
            print('error: ', pm_ids)
            continue

        errors = [n['ERROR'] for n in records if len(n['ERROR']) > 0]
        if len(errors) > 0:
            print('error occur, sleep')
            time.sleep(100)
            continue

        for j, pm_id in enumerate(pm_ids):
            fw.write(json.dumps({'pm_id': pm_id, 'record': records[j]}) + '\n')

        time.sleep(3)
    except Exception as e:
        print(e)
        time.sleep(20 * 60)

# scores = sorted(records[0]['LinkSetDb'][0]['Link'], key=lambda k: int(k['Score']))
# print(scores)
# show the top 5 results
# for i in range(1, 6):
#     handle = Entrez.efetch(db="pubmed", id=scores[-i]['Id'], rettype="xml")
#     record = Entrez.read(handle)
#     print(record)
