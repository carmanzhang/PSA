import json
import os
import pandas as pd

from config import eval_data_dir

relish_data = os.path.join(eval_data_dir, 'RELISH_v1.json')
format_relish_data = os.path.join(eval_data_dir, 'RELISH_v1.tsv')

lines = open(relish_data).readlines()
content = ' '.join(lines)
content = json.loads(content)
print(len(content), content[0])

df = pd.json_normalize(content)

columns = df.columns.values
print(columns)
columns = [c.replace('.', '_') for c in columns]

print(df.head())

# response.relevant	response.partial	response.irrelevant
df.rename({'response.relevant': 'relevant',
           'response.partial': 'partial',
           'response.irrelevant': 'irrelevant',
           'pmid': 'pm_id'},
          axis='columns', inplace=True)

df.to_csv(format_relish_data, sep='\t', header=True, index=False)
