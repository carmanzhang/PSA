from sentence_transformers import SentenceTransformer
from sentence_transformers import models


class PreTrainedModel:
    def __init__(self, name_or_path):
        self.name_or_path = name_or_path

    def load_transformer(self):
        # max_seq_length specifies the maximum number of tokens of the input.
        # The number of token is superior or equal to the number of words of an input.
        word_embedding_model = models.Transformer(self.name_or_path)
        pooling_model = models.Pooling(word_embedding_model.get_word_embedding_dimension())
        model = SentenceTransformer(modules=[word_embedding_model, pooling_model])
        return model

    def load_sentence_transformer(self):
        model = SentenceTransformer(self.name_or_path)
        return model

    def load(self):
        if not '/' in self.name_or_path:
            model = self.load_sentence_transformer()
            print('load_sentence_transformer: %s' % self.name_or_path)
        else:
            try:
                model = self.load_transformer()
                print('load_transformer: %s' % self.name_or_path)
            except Exception as e:
                model = self.load_sentence_transformer()
                print('load_sentence_transformer: %s from local file' % self.name_or_path)
        return model
