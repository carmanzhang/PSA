# crawler PubMed paper information from PubMed interface

import json
import time

import requests

from myio.data_reader import DBReader

fw = open('data/pubmed_official_similar_paper.tsv', 'a')
df = DBReader.tcp_model_cached_read(cached_file_path='',
                                    sql='select pm_id from sp.pubmed_randomly_selected_papers',
                                    cached=False)
pm_id_arr = df['pm_id'].values
for i, pm_id in enumerate(pm_id_arr):
    if i % 100 == 0:
        print(i)
    url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&db=pubmed&id=%s&cmd=neighbor_score' % pm_id
    # url = 'https://pubmed.ncbi.nlm.nih.gov/?format=pmid&linkname=pubmed_pubmed&from_uid=%s&page=1&size=100' % pm_id
    r = requests.get(url)
    if r.status_code != 200:
        print('error occur, sleep')
        time.sleep(100)
    else:
        res = str(r.text).replace('\n', ' ')
        fw.write(pm_id + '\t' + res + '\n')
        time.sleep(1)

# scores = sorted(records[0]['LinkSetDb'][0]['Link'], key=lambda k: int(k['Score']))
# print(scores)
# show the top 5 results
# for i in range(1, 6):
#     handle = Entrez.efetch(db="pubmed", id=scores[-i]['Id'], rettype="xml")
#     record = Entrez.read(handle)
#     print(record)
