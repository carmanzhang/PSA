from sentence_transformers import SentenceTransformer
from sentence_transformers import models


class PreTrainedModel:
    def __init__(self, name_or_path, max_seq_length=300):
        self.name_or_path = name_or_path
        self.max_seq_length = max_seq_length

    def load(self):
        if not '/' in self.name_or_path:
            model = SentenceTransformer(self.name_or_path)
            print('loaded model: %s from UKPLab' % self.name_or_path)
        else:
            # BERT_BASE=/home/zhangli/pre-trained-models/biobert_v1.1_pubmed
            # pytorch_pretrained_bert convert_tf_checkpoint_to_pytorch $BERT_BASE/model.ckpt-1000000 $BERT_BASE/bert_config.json $BERT_BASE/pytorch_model.bin
            # max_seq_length specifies the maximum number of tokens of the input. The number of token is superior or equal to the number of words of an input.
            word_embedding_model = models.Transformer(self.name_or_path, self.max_seq_length)
            pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
            model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
            print('loaded model: %s from local file' % self.name_or_path)
        return model
