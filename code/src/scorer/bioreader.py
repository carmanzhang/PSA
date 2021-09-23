import string
from nltk.corpus import stopwords
from scipy.stats import mannwhitneyu
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import SVR
from typing import List, Union

from helper.word_helper import Stemmer
from scorer.scorer import NoQueryScorer

stop_words = set(stopwords.words('english'))
punctuation = set(string.punctuation)


def format_content(content: str) -> str:
    tokens = content.lower().split()
    tokens = [n for n in tokens if n not in stop_words and n not in punctuation and not n.isnumeric() and len(n) > 0]
    res = ' '.join([Stemmer.stem(n) for n in tokens])
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
