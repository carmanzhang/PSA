import joblib
import multiprocessing
import os
from gensim.corpora.dictionary import Dictionary
from gensim.models.ldamulticore import LdaMulticore
from gensim.test.utils import datapath
from nltk.corpus import stopwords

from config import num_lda_topics, lda_based_path
from myio.data_reader import DBReader

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
corpus = DBReader.tcp_model_cached_read("XXXX",
                                        """select concat(clean_title, ' ', clean_abstract,  ' ', mesh_keywords) as content 
                                        from pr.pmc_paper_clean_merge_content
                                        where rand() % 100 < 1
                                        """,
                                        cached=False)['content'].values

stop_words = set(stopwords.words('english'))

corpus = [[m for m in n.split() if len(m) > 0 and m not in stop_words] for n in corpus]
print('number of corpus: %d' % len(corpus), corpus[:5])

# Create a corpus from a list of texts
dictionary = Dictionary(corpus)
joblib.dump(dictionary, dictionary_path)

corpus = [dictionary.doc2bow(text) for text in corpus]
# ldalda = LdaModel(corpus, num_topics=num_topics, id2word=dictionary)
# Train the model on the corpus.
# None If workers=None all available cores (as estimated by `workers=cpu_count()-1` will be used.
lda = LdaMulticore(iterations=20, corpus=corpus, num_topics=num_lda_topics, id2word=dictionary, workers=cpu_cnt - 2)
lda.save(temp_file)
print('persistent lda model')

topic_list = lda.print_topics(num_lda_topics)
for topic in topic_list:
    print(topic)
