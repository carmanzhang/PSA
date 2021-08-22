from nltk.corpus import stopwords
from sentence2vec.word2vec import Word2Vec, Sent2Vec

from config import doc2vec_based_path, word2vec_based_path
from myio.data_reader import DBReader

corpus = DBReader.tcp_model_cached_read("XXXX",
                                        """select concat(clean_title, ' ', clean_abstract,  ' ', mesh_keywords) as content 
                                        from pr.pmc_paper_clean_merge_content
                                        where rand() % 100 < 1
                                        """,
                                        cached=False)['content'].values

stop_words = set(stopwords.words('english'))

corpus = [' '.join([m for m in n.split() if len(m) > 0 and m not in stop_words]) for n in corpus]
print('number of corpus: %d' % len(corpus), corpus[:5])

model = Word2Vec(corpus, size=100, window=5, sg=0, min_count=5, workers=8)
model.save(word2vec_based_path + '.model')
model = Sent2Vec(corpus, model_file=word2vec_based_path + '.model')
model.save(doc2vec_based_path + '.model')
