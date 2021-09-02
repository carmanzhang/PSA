from nltk.corpus import stopwords
from sentence2vec.word2vec import Word2Vec, Sent2Vec
import pickle

from config import doc2vec_based_path, word2vec_based_path
from myio.data_reader import DBReader

#  eval_data_related_pubmed_article_clean_metadata contains more than 20K abstract
df = DBReader.tcp_model_cached_read("XXXX",
                                    """select pm_id, concat(clean_title, ' ', clean_abstract) as content
                                    from sp.eval_data_related_pubmed_article_clean_metadata
                                    order by pm_id
                                    """,
                                    cached=False)

doc_id_mapping = {pm_id: i for i, pm_id in enumerate(df['pm_id'].values)}
pickle.dump(doc_id_mapping, open(doc2vec_based_path + '-pm-id-mapping.pkl', 'wb'))

corpus = df['content'].values

stop_words = set(stopwords.words('english'))

corpus = [' '.join([m for m in n.split() if len(m) > 0 and m not in stop_words]) for n in corpus]
print('number of corpus: %d' % len(corpus), corpus[:5])

# size` is the dimensionality of the feature vectors.
model = Word2Vec(corpus, size=100, window=5, sg=0, min_count=5, workers=12)
model.save(word2vec_based_path + '.model')
model = Sent2Vec(corpus, model_file=word2vec_based_path + '.model')
model.save(doc2vec_based_path + '.model')
