from sentence_transformers import SentenceTransformer
from sentence_transformers import models

"""
which pre-trained model can we use?, please refer to https://www.sbert.net/docs/pretrained_models.html
- Average Word Embeddings Models
    The following models apply compute the average word embedding for some well-known word embedding methods. Their computation speed is much higher than the transformer based models, but the quality of the embeddings are worse.
    average_word_embeddings_glove.6B.300d
    average_word_embeddings_komninos
    average_word_embeddings_levy_dependency
    average_word_embeddings_glove.840B.300d

- Semantic Textual Similarity https://github.com/UKPLab/sentence-transformers
    Sentence Transformer Models (NLI + STS benchmark). The following models were optimized for Semantic Textual Similarity (STS). They were trained on SNLI+MultiNLI and then fine-tuned on the STS benchmark train set.
    stsb-bert-base                                 21-Dec-2020 20:24           405233603
    stsb-distilbert-base                           21-Dec-2020 20:26           244715968

    stsb-roberta-large 86.39
    stsb-distilroberta-base-v2	86.41
    stsb-roberta-base-v2	87.21
    stsb-mpnet-base-v2	88.57

- SPECTER is a model trained on scientific citations and can be used to estimate the similarity of two publications. We can use it to find similar papers.
    allenai-specter - Semantic Search Python Example / Semantic Search Colab Example

- SciBert https://huggingface.co/allenai/
    biomed_roberta_base
    https://huggingface.co/allenai/scibert_scivocab_uncased
    
- BioBert https://github.com/dmis-lab/biobert
    BioBERT-Base v1.1 (+ PubMed 1M) - based on BERT-base-Cased (same vocabulary)
    BioBERT-Large v1.1 (+ PubMed 1M) - based on BERT-large-Cased (custom 30k vocabulary), NER/QA Results
    BioBERT-Base v1.0 (+ PubMed 200K) - based on BERT-base-Cased (same vocabulary)
    BioBERT-Base v1.0 (+ PMC 270K) - based on BERT-base-Cased (same vocabulary)
    BioBERT-Base v1.0 (+ PubMed 200K + PMC 270K) - based on BERT-base-Cased (same vocabulary)
"""

# TODO available models
upklab_archived_model = [
    # 'average_word_embeddings_glove.6B.300d',
    # 'average_word_embeddings_komninos',
    # 'average_word_embeddings_levy_dependency',
    # 'average_word_embeddings_glove.840B.300d',

    # 'stsb-bert-base',
    'stsb-distilbert-base',
    # 'stsb-roberta-large',
    'stsb-distilroberta-base-v2',
    # 'stsb-roberta-base',
    'stsb-roberta-base-v2',
    'stsb-mpnet-base-v2',

    'allenai-specter'
]

supply_models = ['/home/zhangli/pre-trained-models/scibert_scivocab_uncased',
                 '/home/zhangli/pre-trained-models/biobert-v1.1']

available_models = upklab_archived_model + supply_models


class PreTrainedModel:
    def __init__(self, name_or_path, max_seq_length=300):
        self.name_or_path = name_or_path
        self.max_seq_length = max_seq_length

    def load(self):
        if not '/' in self.name_or_path:
            model = SentenceTransformer(self.name_or_path)
            print('loaded model: %s from UPKLab' % self.name_or_path)
        else:
            # BERT_BASE=/home/zhangli/pre-trained-models/biobert_v1.1_pubmed
            # pytorch_pretrained_bert convert_tf_checkpoint_to_pytorch $BERT_BASE/model.ckpt-1000000 $BERT_BASE/bert_config.json $BERT_BASE/pytorch_model.bin
            # max_seq_length specifies the maximum number of tokens of the input. The number of token is superior or equal to the number of words of an input.
            word_embedding_model = models.Transformer(self.name_or_path, self.max_seq_length)
            pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
            model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
            print('loaded model: %s from local file' % self.name_or_path)
        return model
