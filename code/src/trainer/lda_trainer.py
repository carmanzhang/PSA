import joblib
import multiprocessing
import os
from gensim.corpora.dictionary import Dictionary
from gensim.models import TfidfModel
from gensim.models.ldamulticore import LdaMulticore
from gensim.test.utils import datapath
from nltk.corpus import stopwords

from config import num_lda_topics, lda_based_path
from myio.data_reader import DBReader

low_tfidf_value = 0.002
cpu_cnt = multiprocessing.cpu_count()
dictionary_path = os.path.join(lda_based_path, "pmc_lda_dictionary.pkl")
model_path = os.path.join(lda_based_path, "pmc_lda_t%d.lda" % num_lda_topics)
print('model_path: %s' % model_path)

# Note Load or train the model
# Save a model to disk, or reload a pre-trained model
# Save model to disk.

# Note prepare corpus
temp_file = datapath(model_path)

# more than 2 million PMC papers
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
joblib.dump(dictionary, dictionary_path)
corpus = [dictionary.doc2bow(text) for text in documents]

# ldalda = LdaModel(corpus, num_topics=num_topics, id2word=dictionary)
# Train the model on the corpus.
# None If workers=None all available cores (as estimated by `workers=cpu_count()-1` will be used.
lda = LdaMulticore(passes=10, corpus=corpus, num_topics=num_lda_topics, id2word=dictionary, iterations=20,
                   workers=cpu_cnt - 2)
lda.save(temp_file)
print('persistent lda model')

topic_list = lda.print_topics(num_lda_topics)
for topic in topic_list:
    print(topic)
