import string
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from scipy.stats import mannwhitneyu
from sklearn import metrics
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import SVC

from myio.data_reader import DBReader

stop_words = set(stopwords.words('english'))
punctuation = set(string.punctuation)
ps = PorterStemmer()

# more than 2 million PMC papers
ds_name = 'trec_genomic_2005'
# ds_name = 'relish_v1'

#  Note used information: title + absract + MeSH
df = DBReader.tcp_model_cached_read("XXXX",
                                    '''select q_id,
   q_pm_id,
   concat(q_content[1], ' ', q_content[2], ' ', q_content[3]) as q_content,
   arrayMap(x->
                (tupleElement(x, 1),
                 concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2], ' ', tupleElement(x, 2)[3]),
                 tupleElement(x, 3))
       , c_tuples)                                            as c_tuples
from sp.eval_data_%s_with_content where rand()%%100 <1 ;''' % ds_name,
                                    cached=False)


def format_content(content: str) -> str:
    tokens = content.lower().split()
    tokens = [n for n in tokens if n not in stop_words and n not in punctuation and not n.isnumeric() and len(n) > 0]
    #  stemDocument and stripWhitespace
    res = ' '.join([ps.stem(n) for n in tokens])
    return res


for i, row in df.iterrows():
    q_id, q_pm_id, q_content, c_tuples = row
    # q_content = format_content(q_content)

    # apply these transformations to content
    # Note step 1. lowcase, removeWords， removePunctuation, removeNumbers, stemDocument, stripWhitespace
    q_ids = [q_id] * len(c_tuples)
    c_ids = [n[0] for n in c_tuples]
    orders = [n[-1] for n in c_tuples]
    c_pos_ids = [n[0] for n in c_tuples if n[-1] > 0]
    c_neg_ids = [n[0] for n in c_tuples if n[-1] == 0]
    c_contents = [format_content(n[1]) for n in c_tuples]

    # Note step 2. building  Document Term Matrix (DTM) and weighted by TFIDF
    #  similar blog to this paper please refer to https://edumunozsala.github.io/BlogEms/jupyter/nlp/classification/python/2020/07/31/Intro_NLP_1_TFIDF_Text_Classification.html#Tokenization,-Term-Document-Matrix,-TF-IDF-and-Text-classification
    tfidf = TfidfVectorizer()
    c_contents_dtm = tfidf.fit_transform(c_contents)
    c_contents_dtm = c_contents_dtm.toarray()

    # Note step 3. split the dataset into train/test set
    # c_pos_contents = [n for i, n in enumerate(c_contents) if orders[i] > 0]
    # c_neg_contents = [n for i, n in enumerate(c_contents) if orders[i] == 0]
    # c_pos_contents_dtm = tfidf.transform(c_pos_contents).toarray()
    # c_neg_contents_dtm = tfidf.transform(c_neg_contents).toarray()

    # Note step 4. Reduce terms using Mann-Whitney and apply it to test set
    pos_order_idx = [i for i, n in enumerate(orders) if n > 0]
    neg_order_idx = [i for i, n in enumerate(orders) if n == 0]

    n_doc, n_terms = c_contents_dtm.shape
    pvals = []
    for i in range(n_terms):
        pos_weights, neg_weights = c_contents_dtm[pos_order_idx, i], c_contents_dtm[neg_order_idx, i]
        U1, p = mannwhitneyu(pos_weights, neg_weights)
        pvals.append([i, p])

    pvals = sorted(pvals, key=lambda x: x[1], reverse=False)
    num_sig_pvals = len([p for _, p in pvals if p < 0.05])
    selected_num_cols = 100 if num_sig_pvals > 100 else min(n_terms, 100)
    selected_sig_idx = [i for i, p in pvals[:selected_num_cols]]

    # print(c_contents_dtm.shape)
    x_train = c_contents_dtm[:, selected_sig_idx]
    # print(train_data.shape)
    y_train = [1 if n > 0 else 0 for n in orders]

    # Note step 4. classification
    model = SVC(kernel='rbf')
    model.fit(x_train, y_train)
    # Predicting the Test set results
    # y_pred = model.predict(x_train)
    y_pred = model.predict(x_train)
    # print(metrics.classification_report(y_test, y_pred,  digits=5))
    print(metrics.classification_report(y_train, y_pred, digits=5))
