from typing import List, Union

import string
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from scipy.stats import mannwhitneyu
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import SVR
from tqdm import tqdm

from config import AvailableDataset
from metric.all_metric import eval_metrics
from myio.data_reader import DBReader
from scorer.scorer import NoQueryScorer

stop_words = set(stopwords.words('english'))
punctuation = set(string.punctuation)
ps = PorterStemmer()


def format_content(content: str) -> str:
    tokens = content.lower().split()
    tokens = [n for n in tokens if n not in stop_words and n not in punctuation and not n.isnumeric() and len(n) > 0]
    #  stemDocument and stripWhitespace
    res = ' '.join([ps.stem(n) for n in tokens])
    return res


class BioreaderScorer(NoQueryScorer):
    def __init__(self):
        super().__init__('bioreader-no-query')

    def score(self, train_id: List[str], train_contents: List[str], train_orders: List[int], test_id: List[str],
              test_contents: List[str]) -> Union[List[float], None]:
        contents = train_contents + test_contents
        ids = train_id + test_id

        # Note apply these transformations to content
        # Note step 1. lowcase, removeWords， removePunctuation, removeNumbers, stemDocument, stripWhitespace
        contents = [format_content(n[0]) for n in contents]

        # Note step 2. building  Document Term Matrix (DTM) and weighted by TFIDF
        #  similar blog to this paper please refer to https://edumunozsala.github.io/BlogEms/jupyter/nlp/classification/python/2020/07/31/Intro_NLP_1_TFIDF_Text_Classification.html#Tokenization,-Term-Document-Matrix,-TF-IDF-and-Text-classification
        tfidf = TfidfVectorizer()
        c_contents_dtm = tfidf.fit_transform(contents)
        c_contents_dtm = c_contents_dtm.toarray()

        # Note step 3. split the dataset into train/test set
        train_dtm = c_contents_dtm[:len(train_contents), ]
        test_dtm = c_contents_dtm[len(train_contents):, ]
        assert train_dtm.shape[0] + test_dtm.shape[0] == len(contents)

        # Note step 4. Reduce terms using Mann-Whitney and apply it to test set
        pos_order_idx = [i for i, n in enumerate(train_orders) if n > 0]
        neg_order_idx = [i for i, n in enumerate(train_orders) if n == 0]

        n_doc, n_terms = train_dtm.shape
        pvals = []
        for i in range(n_terms):
            pos_weights, neg_weights = train_dtm[pos_order_idx, i], train_dtm[neg_order_idx, i]
            try:
                U1, p = mannwhitneyu(pos_weights, neg_weights)
            except Exception as e:
                p = 1
            pvals.append([i, p])

        pvals = sorted(pvals, key=lambda x: x[1], reverse=False)
        num_sig_pvals = len([p for _, p in pvals if p < 0.05])
        selected_num_cols = 100 if num_sig_pvals > 100 else min(n_terms, 100)
        selected_sig_idx = [i for i, p in pvals[:selected_num_cols]]

        x_train = train_dtm[:, selected_sig_idx]
        y_train = [1 if n > 0 else 0 for n in train_orders]

        x_test = test_dtm[:, selected_sig_idx]

        # print(train_data.shape)

        # Note step 4. classification
        try:
            model = SVR(kernel='rbf')
            model.fit(x_train, y_train)
            # Predicting the Test set results
            # y_pred = model.predict(x_train)
            y_pred = model.predict(x_test)
            # print(y_pred)
        except Exception as e:
            print(e)
            return None

        return y_pred


# available_datasets = AvailableDataset.aslist()
# for ds_name in available_datasets:
#     ds_name = ds_name.value
#     running_desc = 'bioreader-no-query' + '_' + ds_name
#     print(ds_name + '...')
#     #  Note used information: title + abstract + MeSH
#     sql = sql_template % ds_name
#     print(sql)
#     df = DBReader.tcp_model_cached_read("XXXX", sql=sql, cached=False)
#     all_query_ranks = []
#     for i, row in tqdm(df.iterrows(), total=df.shape[0]):
#         id, train_data, val_data, test_data = row
#
#         # Note apply these transformations to content
#         # Note step 1. lowcase, removeWords， removePunctuation, removeNumbers, stemDocument, stripWhitespace
#
#         train_contents = [n[1] for n in train_data]
#         test_contents = [n[1] for n in test_data]
#         # title_abstract, mesh heading, mesh qualifier, journal name
#         train_contents = [format_content(n[0]) for n in train_contents]
#         test_contents = [format_content(n[0]) for n in test_contents]
#
#         contents = train_contents + test_contents
#
#         train_orders = [n[2] for n in train_data]
#         test_orders = [n[2] for n in test_data]
#
#         # Note step 2. building  Document Term Matrix (DTM) and weighted by TFIDF
#         #  similar blog to this paper please refer to https://edumunozsala.github.io/BlogEms/jupyter/nlp/classification/python/2020/07/31/Intro_NLP_1_TFIDF_Text_Classification.html#Tokenization,-Term-Document-Matrix,-TF-IDF-and-Text-classification
#         tfidf = TfidfVectorizer()
#         c_contents_dtm = tfidf.fit_transform(contents)
#         c_contents_dtm = c_contents_dtm.toarray()
#
#         # Note step 3. split the dataset into train/test set
#         train_dtm = c_contents_dtm[:len(train_contents), ]
#         test_dtm = c_contents_dtm[len(train_contents):, ]
#         assert train_dtm.shape[0] + test_dtm.shape[0] == len(contents)
#
#         # Note step 4. Reduce terms using Mann-Whitney and apply it to test set
#         pos_order_idx = [i for i, n in enumerate(train_orders) if n > 0]
#         neg_order_idx = [i for i, n in enumerate(train_orders) if n == 0]
#
#         n_doc, n_terms = train_dtm.shape
#         pvals = []
#         for i in range(n_terms):
#             pos_weights, neg_weights = train_dtm[pos_order_idx, i], train_dtm[neg_order_idx, i]
#             try:
#                 U1, p = mannwhitneyu(pos_weights, neg_weights)
#             except Exception as e:
#                 p = 1
#             pvals.append([i, p])
#
#         pvals = sorted(pvals, key=lambda x: x[1], reverse=False)
#         num_sig_pvals = len([p for _, p in pvals if p < 0.05])
#         selected_num_cols = 100 if num_sig_pvals > 100 else min(n_terms, 100)
#         selected_sig_idx = [i for i, p in pvals[:selected_num_cols]]
#
#         x_train = train_dtm[:, selected_sig_idx]
#         y_train = [1 if n > 0 else 0 for n in train_orders]
#
#         x_test = test_dtm[:, selected_sig_idx]
#         y_test = [1 if n > 0 else 0 for n in test_orders]
#
#         # print(train_data.shape)
#
#         # Note step 4. classification
#         try:
#             model = SVR(kernel='rbf')
#             model.fit(x_train, y_train)
#             # Predicting the Test set results
#             # y_pred = model.predict(x_train)
#             y_pred = model.predict(x_test)
#             # print(y_pred)
#         except Exception as e:
#             print(e)
#             continue
#         assert len(test_orders) == len(y_pred)
#
#         query_rank = sorted(zip(y_pred, test_orders), key=lambda x: x[0], reverse=True)
#         # query_rank = [n for n, _ in query_rank]
#         all_query_ranks.append(query_rank)
#
#     eval_metrics(all_query_ranks, running_desc)
