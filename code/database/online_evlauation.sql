-- TODO dataset was crawled at 2010-05-15 to 2010-05-17


-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/pubmed_official_similar_paper_bulk.tsv | clickhouse-local --input_format_allow_errors_ratio=0.1 --input-format=TSV --table='input' --structure="line String"  --query="select JSONExtractString(line, 'pm_id')            as pm_id, \
--        arrayMap(y1-> \
--                     [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')], \
--                 JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed', \
--                                                 JSONExtractArrayRaw(JSONExtractRaw(line, 'record') as record, \
--                                                                     'LinkSetDb'))[1], 'Link') \
--            )                                           as pubmed_pubmed, \
--        arrayMap(y1-> \
--                     [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')], \
--                 JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_citedin', \
--                                                 JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link') \
--            )                                           as pubmed_pubmed_citedin, \
--        arrayMap(y1-> \
--                     [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')], \
--                 JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_combined', \
--                                                 JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link') \
--            )                                           as pubmed_pubmed_combined, \
--        arrayMap(y1-> \
--                     [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')], \
--                 JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_five', \
--                                                 JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link') \
--            )                                           as pubmed_pubmed_five, \
--        arrayMap(y1-> \
--                     [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')], \
--                 JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_refs', \
--                                                 JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link') \
--            )                                           as pubmed_pubmed_refs, \
--        arrayMap(y1-> \
--                     [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')], \
--                 JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_reviews', \
--                                                 JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link') \
--            )                                           as pubmed_pubmed_reviews, \
--        arrayMap(y1-> \
--                     [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')], \
--                 JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_reviews_five', \
--                                                 JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link') \
--            )                                           as pubmed_pubmed_reviews_five, \
--        JSONExtractArrayRaw(record, 'ERROR')            as error, \
--        JSONExtractArrayRaw(record, 'LinkSetDbHistory') as linkset_db_history, \
--        JSONExtractRaw(record, 'DbFrom')                as db_from, \
--        JSONExtractArrayRaw(record, 'IdList')           as id_list \
-- from input" --format=Native | clickhouse-client --query='INSERT INTO sp.pubmed_official_similar_paper FORMAT Native' --port=9001 --password=root

-- drop table sp.pubmed_official_similar_paper;
create table if not exists sp.pubmed_official_similar_paper
(
    pm_id                      String,
    pubmed_pubmed              Array(Array(String)),
    pubmed_pubmed_citedin      Array(Array(String)),
    pubmed_pubmed_combined     Array(Array(String)),
    pubmed_pubmed_five         Array(Array(String)),
    pubmed_pubmed_refs         Array(Array(String)),
    pubmed_pubmed_reviews      Array(Array(String)),
    pubmed_pubmed_reviews_five Array(Array(String)),
    error                      Array(String),
    linkset_db_history         Array(String),
    db_from                    String,
    id_list                    Array(String)
) ENGINE = MergeTree order by length(pm_id);

-- 1002495 pubmed_official_similar_paper_bulk.tsv
-- 1002495
-- 0
select count()
from sp.pubmed_official_similar_paper
where length(error) > 0
   or length(linkset_db_history) > 0
   or db_from != '"pubmed"'
   or length(id_list) != 1
   or id_list[1] != concat('\"', pm_id, '\"');

-- we found similar article in the field "pubmed_pubmed" has the longest record, whose ranking results are also identical with the PubMed interface.
-- 137.79505433942313	17.926720831525344	4.120554217228016	4.9999341642601705	32.842172778916606	12.779190918657948	3.8077775949007227
with (select count() from sp.pubmed_official_similar_paper) as num_papers
select sum(length(pubmed_pubmed)) / num_papers              as avg_pubmed_pubmed,
       sum(length(pubmed_pubmed_citedin)) / num_papers      as avg_pubmed_pubmed_citedin,
       sum(length(pubmed_pubmed_combined)) / num_papers     as avg_pubmed_pubmed_combined,
       sum(length(pubmed_pubmed_five)) / num_papers         as avg_pubmed_pubmed_five,
       sum(length(pubmed_pubmed_refs)) / num_papers         as avg_pubmed_pubmed_refs,
       sum(length(pubmed_pubmed_reviews)) / num_papers      as avg_pubmed_pubmed_reviews,
       sum(length(pubmed_pubmed_reviews_five)) / num_papers as avg_pubmed_pubmed_reviews_five
from sp.pubmed_official_similar_paper;
-- 8567	22537	8	6	1904	2489	5
select max(length(pubmed_pubmed))              as max_pubmed_pubmed,
       max(length(pubmed_pubmed_citedin))      as max_pubmed_pubmed_citedin,
       max(length(pubmed_pubmed_combined))     as max_pubmed_pubmed_combined,
       max(length(pubmed_pubmed_five))         as max_pubmed_pubmed_five,
       max(length(pubmed_pubmed_refs))         as max_pubmed_pubmed_refs,
       max(length(pubmed_pubmed_reviews))      as max_pubmed_pubmed_reviews,
       max(length(pubmed_pubmed_reviews_five)) as max_pubmed_pubmed_reviews_five
from sp.pubmed_official_similar_paper;


-- ############################################# Online Evaluation #####################################################
-- we can only use NDCG to evaluate non-binary relevance, other ranking metrics like MRR and MAP are not suitable,
-- please refer to https://medium.com/swlh/rank-aware-recsys-evaluation-metrics-5191bba16832

-- Evaluating the ranking result of similar article by official PubMed
-- 1002495
select count()
from sp.pubmed_official_similar_paper;

-- 0 ranking order validation for PubMed similar articles, the similar paper is in corrected ranking order
select count()
from sp.pubmed_official_similar_paper
where arrayMap(x->x[1], pubmed_pubmed) != arrayMap(z->z[1], arrayReverseSort(y->toFloat32(y[2]), pubmed_pubmed));


select length(pubmed_pubmed) as num_similar_papers, count() as cnt
from sp.pubmed_official_similar_paper
group by num_similar_papers
order by cnt desc;

-- drop table sp.oneline_evaluating_pubmed_official_result;
create table if not exists sp.oneline_evaluating_pubmed_official_result
    ENGINE = MergeTree order by pm_id as
select pm_id,
       arrayMap(x->(arrayFirstIndex(y->tupleElement(y, 1) = x[1], true_ranking) as idx) >
                   0 ? tupleElement(true_ranking[idx], 2):0, pred_ranking) as true_relevance,
       arrayMap(z->z[2], pred_ranking)                                     as scores
--        pred_ranking,
--        true_ranking
from (with 250 as topn
      select pm_id,
             arrayReverseSort(y->y[2],
                              arrayMap(x->
                                           [toUInt64(x[1]), toUInt64(x[2])],
                                       arraySlice(
                                               arrayFilter(y->y[1] != pm_id and y[2] != '0', pubmed_pubmed),
                                               1, topn)
                                  )
                 ) as pred_ranking
      from sp.pubmed_official_similar_paper
      where length(pred_ranking) <= topn) any
         inner join (select pm_id,
                            arrayMap(x-> (toUInt64(tupleElement(x, 1)), toFloat32(tupleElement(x, 2))),
                                     ehcmeshref_ranking) as true_ranking
                     from sp.unified_test_set
                     where train1_test0_val2 in (0)
    ) using pm_id
;

-- 618325
select count()
from sp.oneline_evaluating_pubmed_official_result;
