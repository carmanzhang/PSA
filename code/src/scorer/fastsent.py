# from model.fastsent import FastSent
# from model.infersent import InferSent
# from random import randint
# from typing import List
#
#
# import numpy as np
# import torch
#
# from scorer.scorer import SimpleScorer
#
# import os
# import logging
# import re
# import numpy as np
# import pickle
# import gensim
# import multiprocessing
# from gensim.models import Doc2Vec
# from utility.Utility import Utility
#
# from log_manager.log_config import Logger
# from baselineRunner.BaselineRunner import BaselineRunner
# from summaryGenerator.SummaryGenerator import SummaryGenerator
#
# class FastSentScorer(SimpleScorer):
#     def __init__(self):
#         # Note add a method_signature
#         super().__init__('random')
#
#     def score(self, q_content: str, c_contents: List[str]) -> List[float]:
#
#
#
# class MySentences(object):
#     def __init__(self, sentenceList):
#         self.sentenceList = sentenceList
#
#     def __iter__(self):
#         for line in self.sentenceList:
#             yield line.split()
#
#
# class FastSentFHVersionRunner(BaselineRunner):
#
#     def __init__(self, dbstring, autoencode, **kwargs):
#         BaselineRunner.__init__(self, dbstring, **kwargs)
#         self.sentIDList = list()
#         self.sentenceList = list()
#         self.cores = multiprocessing.cpu_count()
#         self.window = str(10)
#         self.autoencode = autoencode
#         self.dataDir = os.environ['TRTESTFOLDER']
#         self.latReprName = 'felixhillfastsent'
#         self.utFunction = Utility("Text Utility")
#         self.postgresConnection.connectDatabase()
#         self.system_id = 84
#
#         if self.autoencode == True:
#             self.latReprName = "%s_(AE)"%self.latReprName
#
#     def prepareData(self, pd):
#         Logger.logr.info ("Preparing Data for FastSent")
#         for doc_result in self.postgresConnection.memoryEfficientSelect(["id"], \
#                                                                         ["document"], [], [], ["id"]):
#             for row_id in range(0,len(doc_result)):
#                 document_id = doc_result[row_id][0]
#                 for sentence_result in self.postgresConnection.memoryEfficientSelect( \
#                         ['id','content'],['sentence'],[["doc_id","=",document_id]],[],['id']):
#                     for inrow_id in range(0, len(sentence_result)):
#                         sentence_id = int(sentence_result[inrow_id][0])
#                         sentence = sentence_result[inrow_id][1]
#                         content = gensim.utils.to_unicode(sentence)
#                         content = self.utFunction.normalizeText(content, remove_stopwords=0)
#                         self.sentenceList.append(' '.join(content))
#                         self.sentIDList.append(sentence_id)
#
#         Logger.logr.info ("Total sentences loaded = %i"%len(self.sentenceList))
#
#     def runTheBaseline(self, rbase, latent_space_size):
#         # What is auto-encode +AE version, AE = 0
#         # AutoEncode will enable us to perform experiment
#         # on both the simple and +AE version of Fastsent
#         sentences = MySentences(self.sentenceList)
#
#         model = FastSent(sentences, size=latent_space_size, \
#                          window=self.window, min_count=0, workers=self.cores*2, \
#                          sample=1e-4, autoencode=self.autoencode)
#         model.build_vocab(sentences)
#         model.train(sentences, chunksize=1000)
#         outFile = os.path.join(self.dataDir,"%s_repr"%self.latReprName)
#         model.save_fastsent_format(outFile, binary=False)
#
#         fhvecModel = Doc2Vec.load_word2vec_format(outFile, binary=False)
#
#         fhvecFile = open("%s.p"%(outFile),"wb")
#         fhvec_dict = {}
#
#         fhvecFile_raw = open("%s_raw.p"%(outFile),"wb")
#         fhvec_raw_dict = {}
#
#         for result in self.postgresConnection.memoryEfficientSelect(["id", "content"], \
#                                                                     ["sentence"], [], [], ["id"]):
#             for row_id in range(0,len(result)):
#                 id_ = result[row_id][0]
#                 sentence = result[row_id][1]
#
#                 content = gensim.utils.to_unicode(sentence)
#                 content = self.utFunction.normalizeText(content, remove_stopwords=0)
#
#                 if len(content) == 0: continue
#                 vec = np.zeros(latent_space_size)
#                 for word in content:
#                     vec += fhvecModel[word]
#
#                 fhvec_raw_dict[id_] = vec
#                 fhvec_dict[id_] = vec /  ( np.linalg.norm(vec) +  1e-6)
#
#         Logger.logr.info("Total Number of Sentences written=%i", len(fhvec_dict))
#         pickle.dump(fhvec_dict, fhvecFile)
#         pickle.dump(fhvec_raw_dict, fhvecFile_raw)
#
#         fhvecFile_raw.close()
#         fhvecFile.close()
#
#
#
#     def generateSummary(self, gs, methodId, filePrefix, \
#                         lambda_val=1.0, diversity=False):
#
#         if gs <= 0: return 0
#         outFile = os.path.join(self.dataDir, \
#                                "%s_repr"%self.latReprName)
#         vecFile = open("%s.p"%(outFile),"rb")
#         vDict = pickle.load (vecFile)
#
#         summGen = SummaryGenerator (diverse_summ=diversity, \
#                                     postgres_connection = self.postgresConnection, \
#                                     lambda_val = lambda_val)
#
#         summGen.populateSummary(methodId, vDict)
#
#     def runEvaluationTask(self):
#         summaryMethodID = 2
#         what_for =""
#         try:
#             what_for = os.environ['VALID_FOR'].lower()
#         except:
#             what_for = os.environ['TEST_FOR'].lower()
#
#         vDict = {}
#
#         if  "rank" in what_for:
#             vecFile = open("%s.p"%(os.path.join(self.dataDir,"%s_repr"%self.latReprName)),"rb")
#             vDict = pickle.load(vecFile)
#         else:
#             vecFile_raw = open("%s_raw.p"%(os.path.join(self.dataDir,"%s_repr"%self.latReprName)),"rb")
#             vDict = pickle.load(vecFile_raw)
#
#         Logger.logr.info ("Performing evaluation for %s"%what_for)
#         self.performEvaluation(summaryMethodID, self.latReprName, vDict)
#
#
#     def doHouseKeeping(self):
#         self.postgresConnection.disconnectDatabase()
