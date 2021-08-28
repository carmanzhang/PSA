import numpy as np
import pickle
from Bio import Entrez
from gensim import models
from gensim.summarization.bm25 import BM25
from tqdm import tqdm

from ParameterSetting import *
from myio.data_reader import DBReader
from prc import PRC

Entrez.email = "granitedewint@gmail.com"


#     def __init__(self, q_pm_id, c_pm_ids, c_contents):
#         self.vocabDir = 'vocab'
#         self.stemmed_corpus_dir = 'stem'
#         self.q_pm_id = q_pm_id
#         self.pmidList = c_pm_ids
#         self.vocab = []
#         self.corpus = c_contents
#         self.stemmed_corpus = []
#         self.df = None  # doc freq vector
#         self.doclen = None  # doc len vector
#         self.doc_term_matrix = None  # doc-term count matrix
#         self.prc_matrix = None  # PRC score matrix
#         self.sim_matrix = None  # Similarity score matrix
#         self.prc_rankedHits = []

class MPRC(PRC):
    '''This class re-rank the BM25 results using the MPRC algorithm.'''

    def __init__(self, q_pm_id, c_pm_ids, c_contents):
        super(MPRC, self).__init__(q_pm_id, c_pm_ids, c_contents)
        self.knntermDir = 'knn'
        self.model = models.KeyedVectors.load_word2vec_format(
            '/home/zhangli/pre-trained-models/BioWordVec/bio_embedding_intrinsic', binary=True)
        self.knnTermDict = {}
        self.mprc_rankedHits = []
        self.mprc_matrix = None  # MPRC score matrix
        self.resDir = 'result'
    def run_MPRC(self):
        '''run the experiment to get MPRC top hits'''
        self.getVocab()
        self.vectorizeText()
        self.buildDocFreq()
        self.getKNNterms()
        self.calMPRCscores()
        self.cal_MPRC1_similarity()

    def getKNNterms(self):
        '''prepare knn terms of the QUERY TEXT VOCABULARY from the trained word2vec model. '''
        try:
            if "PorterStemmer" in self.resDir:  # if Word2Vec model was trained on stemmed texts
                self.knnTermDict = pickle.load(
                    open(os.path.join(self.knntermDir, "knnTermDict_PorterStemmer_%s.pkl" % self.pmidList[0]), 'rb'))
            if "Standard" in self.resDir:  # if Word2Vec model was trained on the raw texts
                self.knnTermDict = pickle.load(
                    open(os.path.join(self.knntermDir, "knnTermDict_Standard_%s.pkl" % self.pmidList[0]), 'rb'))
        except:
            for term in self.vocab:
                try:
                    knnTerms = self.model.most_similar(term, topn=5)
                    knnTerms = [t[0] for t in knnTerms]
                    self.knnTermDict[term] = knnTerms
                except:
                    pass
            if "PorterStemmer" in self.resDir:  # if Word2Vec model was trained on stemmed texts
                print(os.path.join(self.knntermDir, "knnTermDict_PorterStemmer_%s.pkl" % self.pmidList[0]))
                pickle.dump(self.knnTermDict,
                            open(os.path.join(self.knntermDir, "knnTermDict_PorterStemmer_%s.pkl" % self.pmidList[0]),
                                 "wb"))
            if "Standard" in self.resDir:  # if Word2Vec model was trained on the raw texts
                print(os.path.join(self.knntermDir, "knnTermDict_PorterStemmer_%s.pkl" % self.pmidList[0]))
                pickle.dump(self.knnTermDict,
                            open(os.path.join(self.knntermDir, "knnTermDict_Standard_%s.pkl" % self.pmidList[0]), "wb"))

    def calMPRCscores(self):
        '''Calculate Modified PRC score matrix'''
        ## count matrix
        d1_vocab = np.where(self.doc_term_matrix[0, :] > 0)[
            0].tolist()  # d1_vocab is a list of index, not acutal terms. These terms verified.
        #         query_vocab_index = np.where(self.doc_term_matrix[self.pmidList.index(self.query),:]>0)[0].tolist() # query_vocab is a list of index, not acutal terms
        curr_doclen = np.sum(self.doc_term_matrix[0, :])  # the length of the query text
        newMx = self.doc_term_matrix  # a numpy matrix
        for ind in d1_vocab:
            ori_t = self.vocab[ind]
            if ori_t in list(self.knnTermDict.keys()):
                knn_t = self.knnTermDict[ori_t]
                knn_t = [t for t in knn_t if t in self.vocab]
                knn_index = [self.vocab.index(t) for t in knn_t]
                t_index = [ind] + knn_index
                subMx = self.doc_term_matrix[:,
                        t_index]  # The columns of subMx include the original terms in d0 and their similar terms
                newMx[1:, ind] = np.sum(subMx, axis=1)[
                                 1:]  # Count the occurrence of the original term and its associated similar terms in all other documents
                newMx[:, ind] = newMx[:, ind] * (newMx[
                                                     0, ind] / curr_doclen)  # weight every term by the percentage of this term in the original text
        newMx = newMx[:, d1_vocab]  # the new count matrix weighted by percentage
        self.doc_term_matrix = newMx

        ## MPRC
        la = 0.022
        mu = 0.013
        div = mu / la
        ## generate m1
        reciSqrtIdf = np.reciprocal(
            np.sqrt(np.log(len(self.stemmed_corpus) * 2.0 / (self.df + 1))))  # dim 1*19, conversion verified
        reciSqrtIdf = reciSqrtIdf[0, d1_vocab]
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
        self.mprc_matrix = np.multiply(label, raw_prc_matrix)

    def cal_MPRC1_similarity(self):
        '''MPRC 1 keeps word count k the same, but increase the number of words in common N'''
        self.sim_matrix = np.dot(self.mprc_matrix, self.mprc_matrix.T)
        ## get a ranked similar doc list
        scoreList = self.sim_matrix[0, :].tolist()[0]
        self.mprc_rankedHits = [pmid for (score, pmid) in sorted(zip(scoreList, self.pmidList), reverse=True)]
        if self.mprc_rankedHits[0] != self.pmidList[0]:
            print("Similarity metric error: The most similar article to PMID %s is not itself." % self.pmidList[0],
                  self.mprc_rankedHits[:10])
        ## save results
        outDir = self.resDir  # os.path.join(self.baseDir,"mprc1_ranks")
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        outFile1 = os.path.join(outDir, self.pmidList[0] + ".txt")
        fout = open(outFile1, "w")
        fout.write("\n".join(self.mprc_rankedHits))
        outFile2 = os.path.join(outDir, self.pmidList[0] + "_" + "score.pkl")
        fout = open(outFile2, "wb")
        pmidwScore = sorted(zip(scoreList, self.pmidList), reverse=True)
        pickle.dump(pmidwScore, fout)


if __name__ == '__main__':
    ds_name = 'relish_v1'
    df = DBReader.tcp_model_cached_read('vdsvfn',
                                        sql='''select  q_id,
                                                       concat(q_content[1], ' ', q_content[2]) as q_content,
                                                       arrayMap(x->
                                                                    (tupleElement(x, 1),
                                                                     concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                                                     tupleElement(x, 3))
                                                           , c_tuples)                         as c_tuples
                                                from sp.eval_data_%s_with_content order by q_id limit 20;''' % ds_name,
                                        cached=False)
    df_test = df
    # Note there is no need training
    print('shape test dataframes: ', df_test.shape)

    items = df_test[['q_id', 'q_content', 'c_tuples']].values
    all_query_ranks = []
    for item in tqdm(items):
        q_id, q_content, c_tuples = item
        # rcm_id, c_content, label
        rcm_ids = [rcm_id for rcm_id, c_content, label in c_tuples]
        c_contents = [c_content for rcm_id, c_content, label in c_tuples]
        orders = [label for rcm_id, c_content, label in c_tuples]

        query = q_content.split()
        tok_corpus = [c_content.split() for c_content in c_contents]
        bm25 = BM25(tok_corpus)
        scores = bm25.get_scores(query)

        similars = zip(tok_corpus, scores)

        # print(query)
        mprc = MPRC(q_pm_id=str(q_id), c_pm_ids=[str(n) for n in rcm_ids], c_contents=c_contents)
        mprc.run_MPRC()
