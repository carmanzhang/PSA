import numpy as np
import torch
from sentence_transformers import InputExample, losses
from sentence_transformers import SentencesDataset
from sentence_transformers import evaluation
from sentence_transformers import models
from sentence_transformers.evaluation import SimilarityFunction
from sklearn.decomposition import PCA
from torch.utils.data import DataLoader

from metric import all_metric
from model.transformer import PreTrainedModel


class ActionProcessor:
    def __init__(self, model_name_or_path, data):
        self.model = PreTrainedModel(name_or_path=model_name_or_path).load()
        if data:
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

    def fine_tune(self, save_model_path, model_config):
        optimizer_params, epoch, batch_size, warmup_steps, evaluation_steps \
            = model_config.optimizer_params, model_config.epoch, model_config.batch_size, model_config.warmup_steps, model_config.evaluation_steps

        model = self.model
        df_train, df_val = self.df_train, self.df_val
        print(df_train.columns.values)
        if model_config.loss == 'COSIN':
            # prepare train data for tuning
            train_examples = [InputExample(texts=[content1, content2], label=float(label_or_score)) for
                              i, (content1, content2, label_or_score) in df_train.iterrows()]
            # Define your train dataset, the dataloader and the train loss
            train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=batch_size)
            loss = losses.CosineSimilarityLoss(model)
        elif model_config.loss == 'TRIPLET':
            train_examples = [InputExample(texts=[q_content, c_pos_content, c_neg_content]) for
                              i, (q_content, c_pos_content, c_neg_content) in df_train.iterrows()]

            train_dataset = SentencesDataset(train_examples, model)
            train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=batch_size)
            loss = losses.TripletLoss(model=model)
            evaluator = evaluation.EmbeddingSimilarityEvaluator(df_val['q_content'].values, df_val['c_content'].values,
                                                                df_val['score'].values.astype('float'),
                                                                main_similarity=SimilarityFunction.COSINE)
        elif model_config.loss == 'CONTRASTIVE':
            # print('using model_config.loss: %s' % model_config.loss)
            # Contrastive loss. Expects as input two texts and a label of either 0 or 1.
            # If the label == 1, then the distance between the two embeddings is reduced.
            # If the label == 0, then the distance between the embeddings is increased.
            train_examples = [InputExample(texts=[content1, content2], label=int(score)) for
                              i, (id, pm_id1, content1, pm_id2, content2, score) in df_train.iterrows()]

            train_dataset = SentencesDataset(train_examples, model)
            train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=batch_size)
            loss = losses.ContrastiveLoss(model=model)
            evaluator = evaluation.EmbeddingSimilarityEvaluator(df_val['content1'].values, df_val['content2'].values,
                                                                df_val['score'].values.astype('float'),
                                                                main_similarity=SimilarityFunction.COSINE)

        # loss = losses.MultipleNegativesRankingLoss
        # loss = losses.TripletLoss
        #
        # loss = losses.BatchHardSoftMarginTripletLoss
        # loss = losses.BatchAllTripletLoss
        # loss = losses.BatchHardTripletLoss
        # loss = losses.ContrastiveLoss

        # prepare evaluation data

        # Tune the model
        model.fit(train_objectives=[(train_dataloader, loss)],
                  epochs=epoch,
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
                tmp_eb = self.model.encode([n[1] for n in sent_batch], batch_size=batch_size, show_progress_bar=False)
                assert len(tmp_eb) == len(sent_batch)
                sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
                sent_batch = []
        if len(sent_batch) > 0:
            tmp_eb = self.model.encode([n[1] for n in sent_batch], batch_size=batch_size, show_progress_bar=False)
            assert len(tmp_eb) == len(sent_batch)
            sent_eb.extend([[n[0], tmp_eb[i]] for i, n in enumerate(sent_batch)])
        return sent_eb

    def evaluate(self):
        content_list1 = self.df_test['content1'].values.tolist()
        content_list2 = self.df_test['content2'].values.tolist()
        sent_eb1 = [n[1] for n in self.infer(content_list1)]
        sent_eb2 = [n[1] for n in self.infer(content_list2)]

        label_or_score = self.df_test['score'].values.tolist()
        cosin_sim_scores = all_metric.batch_cosin_sim_score(sent_eb1, sent_eb2)
        # label
        res = all_metric.report_correlation_metrics(cosin_sim_scores, label_or_score)
        return res
