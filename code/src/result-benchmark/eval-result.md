|                                                 |   MAP@5 |   MAP@10 |   MAP@15 |   MAP@20 |   NDCG@5 |   NDCG@10 |   NDCG@15 |   NDCG@20 |
|:------------------------------------------------|--------:|---------:|---------:|---------:|---------:|----------:|----------:|----------:|
| random                                          |  0.7933 |   0.7722 |   0.7541 |   0.7428 |   0.807  |    0.7767 |    0.764  |    0.7607 |
| infersent-v2                                    |  0.8214 |   0.7993 |   0.7874 |   0.7788 |   0.8397 |    0.8145 |    0.8076 |    0.8058 |
| xprc                                            |  0.8434 |   0.8198 |   0.8059 |   0.7928 |   0.8532 |    0.8243 |    0.8178 |    0.8139 |
| infersent-v1                                    |  0.8521 |   0.8216 |   0.8041 |   0.7932 |   0.8656 |    0.8331 |    0.8235 |    0.8184 |
| lda                                             |  0.8544 |   0.8266 |   0.8036 |   0.791  |   0.8651 |    0.8291 |    0.8131 |    0.809  |
| word-ebm-fasttext                               |  0.8575 |   0.8281 |   0.8179 |   0.8023 |   0.8679 |    0.8379 |    0.8312 |    0.8249 |
| word-ebm-glove                                  |  0.8671 |   0.8372 |   0.8227 |   0.8096 |   0.875  |    0.8424 |    0.8383 |    0.8327 |
| doc2vec                                         |  0.8831 |   0.8583 |   0.8461 |   0.8323 |   0.8902 |    0.8623 |    0.8557 |    0.8502 |
| bm25                                            |  0.8891 |   0.8672 |   0.8454 |   0.8325 |   0.8948 |    0.8739 |    0.8621 |    0.8588 |
| word-ebm-biowordvec                             |  0.8984 |   0.8651 |   0.8467 |   0.8321 |   0.899  |    0.8667 |    0.8553 |    0.8489 |
| pmra                                            |  0.903  |   0.8757 |   0.8575 |   0.845  |   0.9095 |    0.884  |    0.8745 |    0.8719 |
| sent2vec-BioSentVec_PubMed_MIMICIII-bigram_d700 |  0.9076 |   0.881  |   0.8616 |   0.8485 |   0.9005 |    0.8776 |    0.8689 |    0.8638 |

|                        |   MAP@5 |   MAP@10 |   MAP@15 |   MAP@20 |   NDCG@5 |   NDCG@10 |   NDCG@15 |   NDCG@20 |
|:-----------------------|--------:|---------:|---------:|---------:|---------:|----------:|----------:|----------:|
| random-no-query        |  0.791  |   0.7835 |        1 |      nan |   0.8158 |    0.7917 |    0.9691 |       nan |
| mscanner-no-query      |  0.8577 |   0.856  |        1 |      nan |   0.8636 |    0.8552 |    0.9314 |       nan |
| bioreader-no-query     |  0.8621 |   0.8551 |        1 |      nan |   0.8642 |    0.8482 |    0.9702 |       nan |
| medlineranker-no-query |  0.8652 |   0.8627 |        1 |      nan |   0.8717 |    0.8718 |    0.9915 |       nan |

|                                                 |   MAP@5 |   MAP@10 |   MAP@15 |   MAP@20 |   NDCG@5 |   NDCG@10 |   NDCG@15 |   NDCG@20 |
|:------------------------------------------------|--------:|---------:|---------:|---------:|---------:|----------:|----------:|----------:|
| random                                          |  0.3154 |   0.3074 |   0.2928 |   0.2777 |   0.4343 |    0.4466 |    0.4386 |    0.435  |
| lda                                             |  0.3859 |   0.3805 |   0.3623 |   0.3483 |   0.5146 |    0.5111 |    0.5019 |    0.4967 |
| doc2vec                                         |  0.4349 |   0.4177 |   0.393  |   0.3731 |   0.5467 |    0.531  |    0.5173 |    0.5169 |
| bm25                                            |  0.4648 |   0.4453 |   0.4189 |   0.4002 |   0.5818 |    0.5668 |    0.5525 |    0.5457 |
| infersent-v2                                    |  0.4764 |   0.452  |   0.4205 |   0.4042 |   0.5745 |    0.5664 |    0.5496 |    0.548  |
| pmra                                            |  0.4783 |   0.454  |   0.4238 |   0.4039 |   0.595  |    0.5764 |    0.5585 |    0.5512 |
| infersent-v1                                    |  0.4816 |   0.45   |   0.4245 |   0.4033 |   0.5846 |    0.5632 |    0.5515 |    0.5478 |
| xprc                                            |  0.4933 |   0.4731 |   0.4518 |   0.4349 |   0.5921 |    0.5846 |    0.5806 |    0.5811 |
| word-ebm-fasttext                               |  0.5005 |   0.4718 |   0.4461 |   0.4292 |   0.6081 |    0.5796 |    0.5647 |    0.5621 |
| word-ebm-biowordvec                             |  0.5089 |   0.4857 |   0.4625 |   0.447  |   0.6128 |    0.5964 |    0.5877 |    0.5868 |
| word-ebm-glove                                  |  0.549  |   0.5069 |   0.4844 |   0.4628 |   0.6477 |    0.6087 |    0.6003 |    0.5954 |
| sent2vec-BioSentVec_PubMed_MIMICIII-bigram_d700 |  0.5653 |   0.5346 |   0.5078 |   0.4933 |   0.6574 |    0.6331 |    0.6228 |    0.6223 |

|                        |   MAP@5 |   MAP@10 |   MAP@15 |   MAP@20 |   NDCG@5 |   NDCG@10 |   NDCG@15 |   NDCG@20 |
|:-----------------------|--------:|---------:|---------:|---------:|---------:|----------:|----------:|----------:|
| random-no-query        |  0.1437 |   0.1631 |   0.1619 |   0.1633 |   0.2273 |    0.2572 |    0.2578 |    0.2718 |
| mscanner-no-query      |  0.3971 |   0.3908 |   0.3795 |   0.3652 |   0.4753 |    0.48   |    0.481  |    0.4887 |
| medlineranker-no-query |  0.4587 |   0.4336 |   0.4104 |   0.4003 |   0.509  |    0.4949 |    0.4878 |    0.4922 |
| bioreader-no-query     |  0.488  |   0.4741 |   0.4631 |   0.4456 |   0.5756 |    0.5592 |    0.5737 |    0.5598 |
