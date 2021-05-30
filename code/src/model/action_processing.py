import torch
from sentence_transformers import models, InputExample
from sentence_transformers.evaluation import SimilarityFunction
from sklearn.decomposition import PCA
from scipy.spatial import distance
from sentence_transformers import SentenceTransformer, InputExample, losses
from sentence_transformers import evaluation
from sentence_transformers import models
from sklearn import metrics
from sklearn.decomposition import PCA
from torch.utils.data import DataLoader
from metric import all_metric
import numpy as np

from model.transformer import PreTrainedModel


class ActionProcessor:
    def __init__(self, model_name_or_path, data):
        self.model = PreTrainedModel(name_or_path=model_name_or_path).load()
        self.df_train = data[0]
        self.df_val = data[1]
        self.df_test = data[2]

    def reload_model(self, save_model_path):
        """:param
        path the path of the fine tuned model
        """
        self.model = PreTrainedModel(name_or_path=save_model_path).load()
        print('successfully reloaded the model')
        return self

    def rebuild_model(self, concise_vector_len=None):
        """:param
        path the path of the fine tuned model
                """
        if concise_vector_len is None:
            return self

        model = self.model
        df_train = self.df_train

        #####################################
        # To determine the PCA matrix, we need some example sentence embeddings.
        # Here, we compute the embeddings for 20k random sentences from the AllNLI dataset
        print('rebuilding the model, set the output vector length to %d ...' % concise_vector_len)
        pca_train_sentences = df_train['content1'].values[0:200000]
        train_embeddings = model.encode(pca_train_sentences, convert_to_numpy=True)

        # Compute PCA on the train embeddings matrix
        pca = PCA(n_components=concise_vector_len)
        pca.fit(train_embeddings)
        pca_comp = np.asarray(pca.components_)

        # We add a dense layer to the model, so that it will produce directly embeddings with the new size
        dense = models.Dense(in_features=model.get_sentence_embedding_dimension(),
                             out_features=concise_vector_len, bias=False,
                             activation_function=torch.nn.Identity())
        dense.linear.weight = torch.nn.Parameter(torch.tensor(pca_comp))
        model.add_module('dense', dense)
        print('rebuild the model')
        # reset the fine-tuned model to attribute
        self.model = model
        return self

    def fine_tune(self, save_model_path, optimizer_params, epochs, batch_size, warmup_steps=100, evaluation_steps=5000):
        model = self.model
        df_train, df_val = self.df_train, self.df_val
        print(df_train.columns.values)
        # prepare train data for tuning
        train_examples = [InputExample(texts=[content1, content2], label=float(label_or_score)) for
                          i, (content1, content2, label_or_score) in df_train.iterrows()]
        # Define your train dataset, the dataloader and the train loss
        train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=batch_size)
        print('load train/val data')

        # TODO try other losses and evaluators
        loss = losses.CosineSimilarityLoss(model)

        # prepare evaluation data
        evaluator = evaluation.EmbeddingSimilarityEvaluator(df_val['content1'].values, df_val['content2'].values,
                                                            df_val['label_or_score'].values.astype('float'),
                                                            main_similarity=SimilarityFunction.COSINE)

        # Tune the model
        model.fit(train_objectives=[(train_dataloader, loss)],
                  epochs=epochs,
                  optimizer_params=optimizer_params,
                  warmup_steps=warmup_steps,
                  evaluator=evaluator,
                  evaluation_steps=evaluation_steps,
                  save_best_model=True,
                  output_path=save_model_path,
                  show_progress_bar=False)

        # reset the fine-tuned model to attribute
        self.model = model
        return self

    def infer(self, content_list, infer_batch_size=3200, batch_size=64):
        sent_batch = []
        sent_eb = []
        for i, v in enumerate(content_list):
            if type(v) == list and len(v) == 2:
                row_id, content = v
            else:
                row_id, content = i, v
            sent_batch.append([row_id, content])
            if i % infer_batch_size == 0:
                tmp_eb = self.model.encode([n[1] for n in sent_batch], batch_size=batch_size)
                assert len(tmp_eb) == len(sent_batch)
                sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
                sent_batch = []
        if len(sent_batch) > 0:
            tmp_eb = self.model.encode([n[1] for n in sent_batch], batch_size=batch_size)
            assert len(tmp_eb) == len(sent_batch)
            sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
        return sent_eb

    def evaluate(self):
        content_list1 = self.df_test['content1'].values.tolist()
        content_list2 = self.df_test['content2'].values.tolist()
        sent_eb1 = [n[1] for n in self.infer(content_list1)]
        sent_eb2 = [n[1] for n in self.infer(content_list2)]

        label_or_score = self.df_test['label_or_score'].values.tolist()
        cosin_sim_scores = all_metric.batch_cosin_sim_score(sent_eb1, sent_eb2)
        # label
        res = all_metric.report_correlation_metrics(cosin_sim_scores, label_or_score)
        return res
