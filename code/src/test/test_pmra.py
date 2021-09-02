from gensim.summarization.bm25 import BM25
from tqdm import tqdm

from myio.data_reader import DBReader
from scorer.prc import PMRAScorer

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

        bm25_ranked_c_contents = sorted(zip(c_contents, scores), key=lambda x: x[1], reverse=True)
        bm25_ranked_c_contents = [n[0] for n in bm25_ranked_c_contents]
        pmra_scorer = PMRAScorer()
        scores = pmra_scorer.score(q_content, bm25_ranked_c_contents)
        print(scores)
