from sentence_transformers import SentenceTransformer, SentencesDataset, losses
from sentence_transformers.readers import InputExample
from torch.utils.data import DataLoader

train_batch_size = 64
model = SentenceTransformer('distilbert-base-nli-mean-tokens')

# CosineSimilarityLoss
train_examples = [InputExample(texts=['My first sentence', 'My second sentence'], label=0.8),
                  InputExample(texts=['Another pair', 'Unrelated sentence'], label=0.3)]
train_dataset = SentencesDataset(train_examples, model)
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=train_batch_size)
train_loss = losses.CosineSimilarityLoss(model=model)

# MultipleNegativesRankingLoss
"""
You can also provide one or multiple hard negatives per anchor-positive pair by structering the data like this: (a_1, p_1, n_1), (a_2, p_2, n_2)
Here, n_1 is a hard negative for (a_1, p_1). The loss will use for the pair (a_i, p_i) all p_j (j!=i) and all n_j as negatives.
"""

train_examples = [InputExample(texts=['Anchor 1', 'Positive 1']),
                  InputExample(texts=['Anchor 2', 'Positive 2'])]
train_dataset = SentencesDataset(train_examples, model)
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=train_batch_size)
train_loss = losses.MultipleNegativesRankingLoss(model=model)

# TripletLoss
train_examples = [InputExample(texts=['Anchor 1', 'Positive 1', 'Negative 1']),
                  InputExample(texts=['Anchor 2', 'Positive 2', 'Negative 2'])]
train_dataset = SentencesDataset(train_examples, model)
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=train_batch_size)
train_loss = losses.TripletLoss(model=model)

# BatchHardSoftMarginTripletLoss
train_examples = [InputExample(texts=['Sentence from class 0'], label=0),
                  InputExample(texts=['Another sentence from class 0'], label=0),
                  InputExample(texts=['Sentence from class 1'], label=1),
                  InputExample(texts=['Sentence from class 2'], label=2)]
train_dataset = SentencesDataset(train_examples, model)
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=train_batch_size)
train_loss = losses.BatchHardSoftMarginTripletLoss(model=model)

# BatchAllTripletLoss
train_examples = [InputExample(texts=['Sentence from class 0'], label=0),
                  InputExample(texts=['Another sentence from class 0'], label=0),
                  InputExample(texts=['Sentence from class 1'], label=1),
                  InputExample(texts=['Sentence from class 2'], label=2)]
train_dataset = SentencesDataset(train_examples, model)
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=train_batch_size)
train_loss = losses.BatchAllTripletLoss(model=model)

# BatchHardTripletLoss
train_examples = [InputExample(texts=['Sentence from class 0'], label=0),
                  InputExample(texts=['Another sentence from class 0'], label=0),
                  InputExample(texts=['Sentence from class 1'], label=1),
                  InputExample(texts=['Sentence from class 2'], label=2)]
train_dataset = SentencesDataset(train_examples, model)
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=train_batch_size)
train_loss = losses.BatchHardTripletLoss(model=model)

# ContrastiveLoss
train_examples = [InputExample(texts=['This is a positive pair', 'Where the distance will be minimized'], label=1),
                  InputExample(texts=['This is a negative pair', 'Their distance will be increased'], label=0)]
train_dataset = SentencesDataset(train_examples, model)
train_dataloader = DataLoader(train_dataset, shuffle=True, batch_size=train_batch_size)
train_loss = losses.ContrastiveLoss(model=model)
