import joblib
import multiprocessing
import os
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.models.doc2vec import TaggedDocument, Doc2Vec
from nltk.corpus import stopwords

from config import doc2vec_based_path
from myio.data_reader import DBReader

low_tfidf_value = 0.002
cpu_cnt = multiprocessing.cpu_count()
dictionary_path = os.path.join(doc2vec_based_path, "pmc_doc2vec_dictionary.pkl")
model_path = os.path.join(doc2vec_based_path, "pmc_doc2vec_model.pkl")
print('model_path: %s' % model_path)

documents = DBReader.tcp_model_cached_read("XXXX",
                                           """select concat(clean_title, ' ', clean_abstract,  ' ', mesh_keywords) as content 
                                           from pr.pmc_paper_clean_merge_content
                                           where rand() % 100 < 3
                                           """,
                                           cached=False)['content'].values

stop_words = set(stopwords.words('english'))

documents = [[m for m in n.split() if len(m) > 0 and m not in stop_words] for n in documents]
print('number of corpus: %d' % len(documents), documents[:5])

# Create a corpus from a list of texts
dictionary = Dictionary(documents)
corpus = [dictionary.doc2bow(text) for text in documents]
tfidf = TfidfModel(corpus, id2word=dictionary)
low_value_words = []
for bow in corpus:
    low_value_words += [id for id, value in tfidf[bow] if value < low_tfidf_value]
dictionary.filter_tokens(bad_ids=low_value_words)
print('remove %d/%d words with lower tfidf score' % (len(low_value_words), len(dictionary.items())))
dictionary = dictionary.token2id
joblib.dump(dictionary, dictionary_path)

corpus = []
for i, doc in enumerate(documents):
    word_list = [w for w in doc if w in dictionary]
    corpus.append(TaggedDocument(words=word_list, tags=[i]))

model = Doc2Vec(epochs=10, vector_size=64, workers=cpu_cnt - 2)
model.build_vocab(corpus)
model.train(corpus, epochs=10, total_examples=len(corpus))

model.save(model_path)

test_doc = '''p for an understanding of the effect of climate change 
on animal population dynamics it is crucial to be able to identify 
which climatologic parameters affect which demographic rate and 
what the underlying mechanistic links are an important reason for 
why the interactions between demography and climate still are 
poorly understood is that the effects of climate vary both 
geographically and taxonomically
'''

inferred_vector = model.infer_vector(test_doc.split())
print(inferred_vector)
sims = model.docvecs.most_similar([inferred_vector], topn=3)
print(sims)
