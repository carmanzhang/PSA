#!/usr/bin/env python
# from Bio import Entrez
#
# Entrez.email = "zhangli@whu.edu.cn"
# handle = Entrez.esearch(db="pmc", term="""One-quarter of patients who received cisplatin experienced a grade or increase in serum creatinine on day;
# """)
# record = Entrez.read(handle)
#
# print(record)
# print(record["IdList"])


# from Bio import Entrez
# Entrez.email = "A.N.Other@example.com"     # Always tell NCBI who you are
# handle = Entrez.esearch(db="pubmed", term="A Review of the Literature Organized Into a New Database: RHeference")
# record = Entrez.read(handle)
# print(record)
#
# print(record["IdList"])
#
# print("19304878" in record["IdList"])


from Bio import Entrez
Entrez.email = "zhangli@whu.edu.cn"
handle = Entrez.esearch(db="pmc", retmax=100, term="One-quarter of patients who received cisplatin experienced a grade or increase in serum creatinine on day")

record = Entrez.read(handle)
handle.close()
print(record)
for row in record["eGQueryResult"]:
    if "pmc" in row["DbName"]:
        print(int(row["Count"]) > 60)