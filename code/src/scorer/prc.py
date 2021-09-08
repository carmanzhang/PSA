from collections import Counter

import numpy as np
import os
import pickle
import re
import string
from hashlib import sha1
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, TreebankWordTokenizer
from sklearn.feature_extraction.text import CountVectorizer
from typing import List

from config import pmra_cached_data_dir
from scorer.scorer import SimpleScorer


class PMRAScorer(SimpleScorer):
    def __init__(self):
        super().__init__('pmra')  # Note pmra is equivalent to prc
        pass

    def score(self, q_content: str, c_contents: List[str], q_pm_id=None, c_pm_ids=None) -> List[float]:
        c_contents = [q_content] + c_contents
        c_pm_ids = [sha1(n.encode('utf-8')).hexdigest() for n in c_contents]
        q_pm_id = c_pm_ids[0]

        prc = PRC(q_pm_id=q_pm_id, c_pm_ids=c_pm_ids, c_contents=c_contents)
        scores = prc.run_PRC()
        assert len(scores) == len(c_contents) - 1
        return scores


class PRC(object):
    '''This class re-rank the BM25 results using Lin and Wilbur's PRC algorithm.'''

    def __init__(self, q_pm_id, c_pm_ids, c_contents):
        self.vocabDir = os.path.join(pmra_cached_data_dir, 'vocab')
        self.stemmed_corpus_dir = os.path.join(pmra_cached_data_dir, 'stem')
        self.q_pm_id = q_pm_id
        self.pmidList = c_pm_ids
        self.vocab = []
        self.corpus = c_contents
        self.stemmed_corpus = []
        self.df = None  # doc freq vector
        self.doclen = None  # doc len vector
        self.doc_term_matrix = None  # doc-term count matrix
        self.prc_matrix = None  # PRC score matrix
        self.sim_matrix = None  # Similarity score matrix
        self.prc_rankedHits = []

    def run_PRC(self):
        '''run the experiment to get PRC top hits'''
        self.getVocab()
        self.vectorizeText()
        self.buildDocFreq()
        self.calPRCscores()
        return self.cal_PRC_Similarity()

    def getVocab(self):
        try:
            self.vocab = pickle.load(open(os.path.join(self.vocabDir, self.q_pm_id), 'rb'))
            self.stemmed_corpus = pickle.load(open(os.path.join(self.stemmed_corpus_dir, str(self.q_pm_id)), 'rb'))
        except Exception as e:
            # print(e)
            self.buildVocab()  # including both self.vocab and self.stemmed_corpus

    def buildVocab(self):
        '''Build a vocabulary for the selected documents (from dir database).'''
        ## Note: The source of text should be Lucene processed field values. Lucene tokenized the text, remove stop words, and may take other unknown steps.
        ## Right now the vocabulary is built on the raw text with NLTK based stopwords removal, and tokenization. This should be improved.
        # collect contents from /database/ for each of these doc
        # for pmid in self.pmidList:  # self.pmidList includes the query and the 99 most similar articles selected by BM25
        #     #  Note content of this PMID
        #     self.corpus.append(   )  # corpus contains raw text (MH, title*2, abstract)
        for text in self.corpus:
            sent_tokenize_list = sent_tokenize(text.strip().lower(), "english")  # tokenize an article text
            stemmed_text = []
            if sent_tokenize_list:  # if sent_tokenize_list is not empty
                porter_stemmer = PorterStemmer()
                for sent in sent_tokenize_list:
                    words = TreebankWordTokenizer().tokenize(sent)  # tokenize the sentence
                    words = [word.strip(string.punctuation) for word in words]
                    words = [word for word in words if not word in stopwords.words("english")]
                    words = [word for word in words if
                             len(word) > 1]  # remove single letters and non alphabetic characters
                    words = [word for word in words if re.search('[a-zA-Z]', word)]
                    # Note TODO stemmer will result in many OOV words
                    words = [porter_stemmer.stem(word) for word in words]  # apply Porter stemmer
                    stemmed_text.append(" ".join(words))
                    self.vocab += words
            self.stemmed_corpus.append(". ".join(stemmed_text))  # append a stemmed article text
        # save stemmed corpus
        # pickle.dump(self.stemmed_corpus, open(os.path.join(self.stemmed_corpus_dir, str(self.q_pm_id)), "wb"))
        # remove low frequency tokens and redundant tokens
        tokenDist = Counter(self.vocab)
        lowFreqList = []
        for token, count in tokenDist.items():
            if count < 2:
                lowFreqList.append(token)
        self.vocab = list(set(self.vocab) - set(lowFreqList))
        # save vocabulary
        # pickle.dump(self.vocab, open(os.path.join(self.vocabDir, str(self.q_pm_id)), "wb"))

    def vectorizeText(self):
        '''This function converts every article (title and abstract) into a list of vocabulary count'''
        vectorizer = CountVectorizer(analyzer='word', vocabulary=self.vocab,
                                     dtype=np.float64)  # CountVectorizer cannot deal with terms with hyphen inside, e.g. k-ras. CountVectorizer will not count such terms.
        self.doc_term_matrix = vectorizer.fit_transform(self.stemmed_corpus)  # for Porter stemmer
        #         self.doc_term_matrix = vectorizer.fit_transform(self.corpus) # for Standard analyzer, no stemmer
        #         self.doclen = self.doc_term_matrix.sum(1) # self.doclen format is CSC, not numpy
        self.doc_term_matrix = self.doc_term_matrix.A
        self.doclen = np.sum(self.doc_term_matrix, 1)
        self.doclen = self.doclen.reshape((len(self.doclen), 1))

    def buildDocFreq(self):
        '''Count documents that contain particular words'''
        vectorizer = CountVectorizer(analyzer='word', vocabulary=self.vocab, binary=True)
        #         doc_term_bin_matrix = vectorizer.fit_transform(self.corpus) # for standard analyzer, no stemmer
        doc_term_bin_matrix = vectorizer.fit_transform(self.stemmed_corpus)  # for Porter stemmer
        self.df = doc_term_bin_matrix.sum(0)

    def calPRCscores(self):
        '''Calculate the weight of every term per document, using PubMed Related Citation (PRC) algorithm, Jimmy Lin and John Wilbur 2007.
           input: idf vector, docLen vector, occurrence count matrix (n documents, all terms in the vocabulary)
           output: a matrix of PRC scores.
        '''
        la = 0.022
        mu = 0.013
        div = mu / la
        ## generate m1
        reciSqrtIdf = np.reciprocal(
            np.sqrt(np.log(len(self.stemmed_corpus) * 2.0 / (self.df + 1))))  # dim 1*19, conversion verified
        expDoclen = np.exp(self.doclen * (la - mu))  # dim 10*1, conversion verified
        m1 = np.dot(expDoclen, reciSqrtIdf)  # dim 10*19, product verified
        ## generate m2: matrix
        matrix = np.power(div, self.doc_term_matrix) / div
        ## Hadamard product
        matrix = np.multiply(matrix, m1)
        ## offset
        offset = np.dot(np.ones((matrix.shape[0], 1)), reciSqrtIdf)
        ## matrix+offset
        matrix = matrix + offset
        ## reciprocal of recWt
        raw_prc_matrix = np.reciprocal(matrix)
        ## reset scores for the terms that do not occur
        label = (self.doc_term_matrix > 0)
        self.prc_matrix = np.multiply(label, raw_prc_matrix)

    def cal_PRC_Similarity(self):
        '''Measure the similarity between every pair of articles using PRC scores of terms in common between documents'''
        self.sim_matrix = np.dot(self.prc_matrix, self.prc_matrix.T)
        ## get a ranked similar doc list
        scoreList = self.sim_matrix[0, :].tolist()[0]
        self.prc_rankedHits = [pmid for (score, pmid) in sorted(zip(scoreList, self.pmidList), reverse=True)]
        scoreList_exlcude_query = [score for (score, pmid) in zip(scoreList, self.pmidList) if pmid != self.q_pm_id]
        return scoreList_exlcude_query

        # if self.prc_rankedHits[0] != self.q_pm_id:
        #     print(len(self.prc_rankedHits), self.prc_rankedHits)

        # ## save results
        # outDir = self.resDir  # os.path.join(self.baseDir,"prc_ranks","PorterStemmer")
        # if not os.path.exists(outDir):
        #     os.makedirs(outDir)
        # outFile1 = os.path.join(outDir, self.q_pm_id + ".txt")
        # fout = open(outFile1, "wb")
        # fout.write("\n".join(self.prc_rankedHits))
        #
        # outFile2 = os.path.join(outDir, self.q_pm_id + "_" + "score.pkl")
        # fout = open(outFile2, "wb")
        # pmidwScore = sorted(zip(scoreList, self.pmidList), reverse=True)
        # pickle.dump(pmidwScore, fout)
