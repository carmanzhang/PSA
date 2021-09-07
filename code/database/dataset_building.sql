-- ######################################### Note RELISH V1 Evaluation Dataset #########################################
-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/evaluation-datasets/RELISH_v1.tsv | clickhouse-client --query='INSERT INTO sp.eval_data_relish_v1 FORMAT TSVWithNames' --port=9001 --password=root
-- drop table sp.eval_data_relish_v1;
create table if not exists sp.eval_data_relish_v1
(
    uid          String,
    pm_id        String,
    experience   String,
    is_anonymous String,
    relevant     Array(String),
    partial      Array(String),
    irrelevant   Array(String)
) ENGINE = Log;


-- 3278 Note test duplicates
select count(), count(distinct uid), count(distinct pm_id)
from sp.eval_data_relish_v1
union all
select count(), count(distinct uid), count(distinct pm_id)
from sp.eval_data_relish_v1 any
         inner join (select pm_id
                     from sp.eval_data_relish_v1
                     group by pm_id
                     having count() == 1) using pm_id;

-- Note test the bad cases, note eliminate them from analysis
select count()
from (
      select count() as cnt
      from sp.eval_data_relish_v1
      group by pm_id
      having cnt != 1
      order by cnt desc)
union all
select count()
from sp.eval_data_relish_v1
where length(arrayConcat(relevant, partial, irrelevant)) != arrayUniq(arrayConcat(relevant, partial, irrelevant))
;


-- 0 Note test convert string to UInt64
select count()
from sp.eval_data_relish_v1
where arrayMap(x->toString(toUInt64(x)), relevant) != relevant
   or arrayMap(x->toString(toUInt64(x)), partial) != partial
   or arrayMap(x->toString(toUInt64(x)), irrelevant) != irrelevant
;

select sum(length(relevant))   as num_relevant,
       sum(length(partial))    as num_partial,
       sum(length(irrelevant)) as num_irrelevant
from sp.eval_data_relish_v1;

-- ######################################### Note TREC 2005 Evaluation Dataset #########################################
-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/evaluation-datasets/trec-gen/data/2005/genomics.qrels.large.txt | clickhouse-client --query='INSERT INTO sp.eval_data_trec_genomic_2005 FORMAT TSV' --port=9001 --password=root
-- drop table sp.eval_data_trec_genomic_2005;
-- genomics.qrels.large.txt - file of all relevance judgments done, with "0" indicating not relevant, "1" possibly relevant, and "2" definitely relevant

-- This file contains the topics for the ad hoc retrieval task of the TREC 2005 Genomics Track.
-- There are a total of 50 topics, numbered from 100 to 149.
-- The topics all generally follow a semantic template, with 10 in each of the 5 templates.
-- 100-109 Information describing standard methods or protocols for doing some sort of experiment or procedure.
-- 110-119 Information describing the role(s) of a gene involved in a disease.
-- 120-129 Information describing the role of a gene in a specific biological process.
-- 130-139 Information describing interactions (e.g., promote, suppress, inhibit, etc.) between two or more genes in the function of an organ or in a disease.
-- 140-149 Information describing one or more mutations of a given gene and its biological impact or role.
create table if not exists sp.eval_data_trec_genomic_2005
(
    topic_id         UInt32,
    no_meaning_field String,
    pm_id            String,
    relevant_level   UInt32
) ENGINE = Log;

-- 39958 /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/evaluation-datasets/trec-gen/data/2005/genomics.qrels.large.txt
-- 39958
select count()
from sp.eval_data_trec_genomic_2005;

-- 4584	4491
select count(), count(distinct pm_id)
from sp.eval_data_trec_genomic_2005
where relevant_level in (1, 2);

-- Note show the relevant score distribution: definitely, possibly, or not relevant
select topic_id,
       length(tmp)                                             as pool_size,
       arrayCount(x->x = 2, groupArray(relevant_level) as tmp) as num_relevant,
       arrayCount(x->x = 1, groupArray(relevant_level) as tmp) as num_partial,
       arrayCount(x->x = 0, groupArray(relevant_level) as tmp) as num_irrelevant
from sp.eval_data_trec_genomic_2005
group by topic_id
order by topic_id;
;

-- ######################################### Note TREC 2014 Evaluation Dataset #########################################
-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/evaluation-datasets/trec-cds/qrels-treceval-2014.txt | clickhouse-client --query='INSERT INTO sp.eval_data_trec_cds_2014 FORMAT TSV' --port=9001 --password=root
-- drop table sp.eval_data_trec_cds_2014;
create table if not exists sp.eval_data_trec_cds_2014
(
    topic_id         UInt32,
    no_meaning_field String,
    pm_id            String,
    relevant_level   UInt32
) ENGINE = Log;

-- 37949 /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/evaluation-datasets/trec-cds/qrels-treceval-2014.txt
-- 37949
select count()
from sp.eval_data_trec_cds_2014;

-- 3356	3271
select count(), count(distinct pm_id)
from sp.eval_data_trec_cds_2014
where relevant_level in (1, 2);

-- Note show the relevant score distribution: definitely, possibly, or not relevant
select topic_id,
       length(tmp)                                             as pool_size,
       arrayCount(x->x = 2, groupArray(relevant_level) as tmp) as num_relevant,
       arrayCount(x->x = 1, groupArray(relevant_level) as tmp) as num_partial,
       arrayCount(x->x = 0, groupArray(relevant_level) as tmp) as num_irrelevant
from sp.eval_data_trec_cds_2014
group by topic_id
order by topic_id;
;

-- Note associate the key metadata from PubMed citations
-- drop table sp.eval_data_related_pubmed_article_clean_metadata;
create table if not exists sp.eval_data_related_pubmed_article_clean_metadata
    ENGINE = MergeTree order by length(pm_id) as
select pm_id,
       clean_title,
       clean_abstract,
       journal_nlm_id,
       journal_title,
       two_level_mesh_arr,
       two_level_mesh_ui_arr,
       datetime_str,
       sources
from (
      select pm_id,
             sources,
             arrayStringConcat(extractAll(CAST(if(article_title is NULL, '', article_title), 'String'), '\\w+'),
                               ' ')                                      as clean_title,
             arrayStringConcat(arrayFilter(x -> x not in
                                                ('abstracttext', 'abstract') and
                                                not match(x, '\\d+'), splitByChar(' ',
                                                                                  trimBoth(
                                                                                          replaceRegexpAll(
                                                                                                  replaceRegexpAll(
                                                                                                          CAST(if(abstract_str is NULL, '', abstract_str), 'String'),
                                                                                                          '[^a-z]',
                                                                                                          ' '),
                                                                                                  '\\s+',
                                                                                                  ' '))
                                               )),
                               ' ')                                      as clean_abstract,
--              arrayStringConcat(arrayFilter(x -> x not in
--                                                 ('descriptorNameUI', 'descriptorName', 'majorTopicYN',
--                                                  'qualifierNameList',
--                                                  'false', 'true', 'null') and not match(x, '\\d+'),
--                                            extractAll(CAST(if(mesh_headings is NULL, '', mesh_headings), 'String'),
--                                                       '\\w+')),
--                                ' ')                                      as clean_mesh_headings,
             tupleElement(JSONExtract(CAST(journal, 'String'),
                                      'Tuple(nlmUniqueID String, title String)') as journal_inifo,
                          1)                                             as journal_nlm_id,
             arrayStringConcat(arrayFilter(x->length(x) > 2, extractAll(tupleElement(journal_inifo, 2), '\\w+')),
                               ' ')                                      as journal_title,
             datetime_str,
             JSONExtractArrayRaw(CAST(mesh_headings, 'String'))          as mesh_heading_content_list,
             arrayMap(x ->JSONExtractString(x, 'descriptorName'),
                      mesh_heading_content_list)                         as descriptor_names,
             arrayMap(x ->JSONExtractString(x, 'descriptorNameUI'),
                      mesh_heading_content_list)                         as descriptor_name_uis,

             arrayMap(x ->
                          arrayMap(y -> JSONExtractString(y, 'descriptorName'),
                                   JSONExtractArrayRaw(x, 'qualifierNameList') as qualifierNameList),
                      mesh_heading_content_list)                         as qualifier_name_lists,

             arrayMap(x ->
                          arrayMap(y -> JSONExtractString(y, 'descriptorNameUI'),
                                   JSONExtractArrayRaw(x, 'qualifierNameList')),
                      mesh_heading_content_list)                         as qualifier_name_ui_lists,

             arrayFlatten(arrayMap(i->length(qualifier_name_ui_lists[i]) ==
                                      0 ? [descriptor_name_uis[i]]: arrayMap(x->concat(descriptor_name_uis[i], '|', x),
                                                                             qualifier_name_ui_lists[i]),
                                   arrayEnumerate(descriptor_name_uis))) as two_level_mesh_ui_arr,

             arrayFlatten(arrayMap(i->length(qualifier_name_lists[i]) ==
                                      0 ? [descriptor_names[i]]: arrayMap(x->concat(descriptor_names[i], '; ', x),
                                                                          qualifier_name_lists[i]),
                                   arrayEnumerate(descriptor_names)))    as two_level_mesh_arr
      from (select toString(pm_id) as pm_id,
                   journal,
                   mesh_headings,
                   article_title,
                   abstract_str,
                   datetime_str
            from pubmed.nft_paper) any
               inner join (
          select pm_id, groupUniqArray(source) as sources
          from (
                select arrayJoin(arrayConcat(relevant, partial, irrelevant, [pm_id])) as pm_id, 'relishv1' as source
                from sp.eval_data_relish_v1
                union all
                select pm_id, 'trec2005' as source
                from sp.eval_data_trec_genomic_2005
                union all
                select pm_id, 'trec2014' as source
                from sp.eval_data_trec_cds_2014)
          group by pm_id
          order by length(sources) desc) using pm_id);

-- ######################################### Note RELISH Evaluation Dataset with content #########################################
-- drop table sp.eval_data_relish_v1_with_content;
create table if not exists sp.eval_data_relish_v1_with_content
    ENGINE = MergeTree order by q_pm_id as
select any(q_id)                                                                    as q_id, -- q_pm_id and q_id is 1 to 1
       q_pm_id,
       any(train1_val2_test0)                                                       as train1_val2_test0,
       any(q_clean_content)                                                         as q_content,
       groupArray((c_pm_id, c_clean_content, score, train1_val2_test0_inner_query)) as c_tuples,
       any(other_metadata)                                                          as other_metadata
from (
         select q_id, q_pm_id, train1_val2_test0, c_pm_id, score, q_clean_content, other_metadata
         from (
                  select uid                        as q_id,
                         pm_id                      as q_pm_id,
                         train1_val2_test0,
                         toString((arrayJoin(arrayFilter(
                                 x->x[2] != toUInt64(q_pm_id),
                                 arrayConcat(arrayMap(x->
                                                          [2, toUInt64(x)], relevant),
                                             arrayMap(x->
                                                          [1, toUInt64(x)], partial),
                                             arrayMap(x->
                                                          [0, toUInt64(x)], irrelevant))))
                             as item)[2])           as c_pm_id,
                         item[1]                    as score,
                         [experience, is_anonymous] as other_metadata
                  from (select *,
                               (xxHash32(uid) % 100 < 80 ? 1 :
                                                      (xxHash32(concat(uid, 'random string here')) % 100 < 50 ? 2 : 0)) as train1_val2_test0
                        from sp.eval_data_relish_v1 any
                                 inner join (select pm_id, count() as cnt
                                             from sp.eval_data_relish_v1
                                             group by pm_id
                                             having cnt = 1) using pm_id
                           )
                  order by uid asc, q_pm_id asc, score desc) any
                  inner join (select pm_id                                                                                                  as q_pm_id,
                                     [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as q_clean_content
                              from sp.eval_data_related_pubmed_article_clean_metadata) using q_pm_id) any
         inner join (select pm_id                                                                                                  as c_pm_id,
                            (xxHash32(pm_id) % 100 < 80 ? 1 :
                                                     (xxHash32(concat(pm_id, 'random string here')) % 100 < 50 ? 2 : 0))           as train1_val2_test0_inner_query,
                            [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as c_clean_content
                     from sp.eval_data_related_pubmed_article_clean_metadata) using c_pm_id
group by q_pm_id
;


-- Note test the number of missing samples
-- 196680	original_data_instances
-- 186960	filtered_duplicates_data_instances
-- 186925	filtered_duplicates_unmatched_data_instances
select sum(length(partial) + length(relevant) + length(irrelevant)) as cnt, 'original_data_instances' as desc
from sp.eval_data_relish_v1
union all
select sum(length(partial) + length(relevant) + length(irrelevant)) as cnt,
       'filtered_duplicates_data_instances'                         as desc
from sp.eval_data_relish_v1 any
         inner join (select pm_id, count() as cnt
                     from sp.eval_data_relish_v1
                     group by pm_id
                     having cnt = 1) using pm_id
union all
select sum(length(c_tuples)) as cnt, 'filtered_duplicates_unmatched_data_instances' as desc
from sp.eval_data_relish_v1_with_content;

-- ######################################### Note TREC-2005 Evaluation Dataset with content #########################################
-- drop table sp.eval_data_trec_genomic_2005_with_content;
create table if not exists sp.eval_data_trec_genomic_2005_with_content
    ENGINE = MergeTree order by q_pm_id as
select any(q_id)                                                                    as q_id,
       q_pm_id,
       any(train1_val2_test0)                                                       as train1_val2_test0,
       any(q_clean_content)                                                         as q_content,
       groupArray((c_pm_id, c_clean_content, score, train1_val2_test0_inner_query)) as c_tuples,
       any(other_metadata)                                                          as other_metadata
from (
--          select qid, q_pm_id, c_pm_id, score, q_clean_content, other_metadata
         select q_id,
                q_pm_id,
                train1_val2_test0,
                tupleElement(arrayJoin(c_pm_id_score_arr) as item, 1) as c_pm_id,
                tupleElement(item, 2)                                 as score,
                q_clean_content,
                other_metadata
         from (
                  with ['within-topic', 'cross-topic'][1] as which_strategy_used_for_making_ranking_pool,
                      (select arraySort(arrayFilter(x-> length(x) > 0, groupUniqArray(pm_id)))
                       from sp.eval_data_trec_genomic_2005
                       where relevant_level in (1, 2)) as corss_topic_ranking_pool
                  select concat(toString(topic_id) as topic_id_str, '_', q_pm_id)                                                   as q_id,
                         (xxHash32(topic_id_str) % 100 < 80 ? 1 :
                                                         (xxHash32(concat(topic_id_str, 'random string here')) % 100 < 50 ? 2 : 0)) as train1_val2_test0,
                         arrayReverseSort(x->
                                              x.2,
                                          groupArray((pm_id, relevant_level)))                                                      as pm_id_topical_relevant_scores,
                         tupleElement(arrayJoin(arrayFilter(x->x.2 in (1, 2), pm_id_topical_relevant_scores)) as item,
                                      1)                                                                                            as q_pm_id,
                         tupleElement(item, 2)                                                                                      as q_pm_id_topical_relevant_score,
                         arrayFilter(y->tupleElement(y, 1) != q_pm_id,
                                     which_strategy_used_for_making_ranking_pool == 'cross-topic' ? arrayMap(
                                             x-> (x,
                                                  if(length(arrayFilter(y->y.1 = x,
                                                                        pm_id_topical_relevant_scores) as tmp) = 1,
                                                     tmp[1].2, 0)),
                                             corss_topic_ranking_pool) :
                                                                                    pm_id_topical_relevant_scores)                  as c_pm_id_score_arr,
                         [toString(topic_id), toString(q_pm_id_topical_relevant_score)]                                             as other_metadata
                  from sp.eval_data_trec_genomic_2005
                  where relevant_level in (0, 1, 2)
                  group by topic_id) any
                  inner join (select pm_id                                                                                                  as q_pm_id,
                                     [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as q_clean_content
                              from sp.eval_data_related_pubmed_article_clean_metadata) using q_pm_id) any
         inner join (select pm_id                                                                                                  as c_pm_id,
                            (xxHash32(pm_id) % 100 < 80 ? 1 :
                                                     (xxHash32(concat(pm_id, 'this is a random string here')) % 100 <
                                                      50 ? 2 :
                                                      0))                                                                          as train1_val2_test0_inner_query,
                            [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as c_clean_content
                     from sp.eval_data_related_pubmed_article_clean_metadata) using c_pm_id
group by q_pm_id
;

-- Note test the number of missing samples
-- 4490	filtered_unmatched_data_instances
-- 4584	original_data_instances
select count(), 'original_data_instances' as desc
from sp.eval_data_trec_genomic_2005
where relevant_level in (1, 2)
union all
select count(), 'filtered_unmatched_data_instances' as desc
from sp.eval_data_trec_genomic_2005_with_content;

-- ######################################### Note TREC-2014 Evaluation Dataset with content #########################################
-- drop table sp.eval_data_trec_cds_2014_with_content;
create table if not exists sp.eval_data_trec_cds_2014_with_content
    ENGINE = MergeTree order by q_pm_id as
select any(q_id)                                                                    as q_id,
       q_pm_id,
       any(train1_val2_test0)                                                       as train1_val2_test0,
       any(q_clean_content)                                                         as q_content,
       groupArray((c_pm_id, c_clean_content, score, train1_val2_test0_inner_query)) as c_tuples,
       any(other_metadata)                                                          as other_metadata
from (
--          select qid, q_pm_id, c_pm_id, score, q_clean_content, other_metadata
         select q_id,
                q_pm_id,
                train1_val2_test0,
                tupleElement(arrayJoin(c_pm_id_score_arr) as item, 1) as c_pm_id,
                tupleElement(item, 2)                                 as score,
                q_clean_content,
                other_metadata
         from (
                  with ['within-topic', 'cross-topic'][1] as which_strategy_used_for_making_ranking_pool,
                      (select arraySort(arrayFilter(x-> length(x) > 0, groupUniqArray(pm_id)))
                       from sp.eval_data_trec_cds_2014
                       where relevant_level in (1, 2)) as corss_topic_ranking_pool
                  select concat(toString(topic_id) as topic_id_str, '_', q_pm_id)                                                                         as q_id,
                         (xxHash32(concat(topic_id_str, 'xxxxxxxxxx')) % 100 < 80 ? 1 :
                                                                               (xxHash32(concat(topic_id_str, 'random string here')) % 100 < 50 ? 2 : 0)) as train1_val2_test0,
                         arrayReverseSort(x->
                                              x.2,
                                          groupArray((pm_id, relevant_level)))                                                                            as pm_id_topical_relevant_scores,
                         tupleElement(arrayJoin(arrayFilter(x->x.2 in (1, 2), pm_id_topical_relevant_scores)) as item,
                                      1)                                                                                                                  as q_pm_id,
                         tupleElement(item, 2)                                                                                                            as q_pm_id_topical_relevant_score,

                         arrayFilter(y->tupleElement(y, 1) != q_pm_id,
                                     which_strategy_used_for_making_ranking_pool == 'cross-topic' ? arrayMap(
                                             x-> (x,
                                                  if(length(arrayFilter(y->y.1 = x,
                                                                        pm_id_topical_relevant_scores) as tmp) = 1,
                                                     tmp[1].2, 0)),
                                             corss_topic_ranking_pool) :
                                                                                    pm_id_topical_relevant_scores)                                        as c_pm_id_score_arr,

                         [toString(topic_id), toString(q_pm_id_topical_relevant_score)]                                                                   as other_metadata
                  from sp.eval_data_trec_cds_2014
                  where relevant_level in (0, 1, 2)
                  group by topic_id) any
                  inner join (select pm_id                                                                                                  as q_pm_id,
                                     [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as q_clean_content
                              from sp.eval_data_related_pubmed_article_clean_metadata) using q_pm_id) any
         inner join (select pm_id                                                                                                  as c_pm_id,
                            (xxHash32(pm_id) % 100 < 80 ? 1 :
                                                     (xxHash32(concat(pm_id, 'this is a random string here')) % 100 <
                                                      50 ? 2 :
                                                      0))                                                                          as train1_val2_test0_inner_query,
                            [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as c_clean_content
                     from sp.eval_data_related_pubmed_article_clean_metadata) using c_pm_id
group by q_pm_id
;

-- Note test the number of missing samples
-- 3271	filtered_unmatched_data_instances
-- 3356	original_data_instances
select count(), 'original_data_instances' as desc
from sp.eval_data_trec_cds_2014
where relevant_level in (1, 2)
union all
select count(), 'filtered_unmatched_data_instances' as desc
from sp.eval_data_trec_cds_2014_with_content;

select max(length(q_content))
from sp.eval_data_trec_cds_2014_with_content;
select max(length(q_content))
from sp.eval_data_relish_v1_with_content;

-- ######################################### Note Making Models Adapted Evaluation Dataset #########################################
select train1_val2_test0, count() as cnt, 'relishv1' as source
from sp.eval_data_relish_v1_with_content
group by train1_val2_test0
union all
select train1_val2_test0, count() as cnt, 'trec2005' as source
from sp.eval_data_trec_genomic_2005_with_content
group by train1_val2_test0
union all
select train1_val2_test0, count() as cnt, 'trec2014' as source
from sp.eval_data_trec_cds_2014_with_content
group by train1_val2_test0;

-- Title + Abstract + MeSH as the input
select q_id,
       q_pm_id,
       concat(q_content[1], ' ', q_content[2], ' ', q_content[3]) as q_content,
       arrayMap(x->
                    (tupleElement(x, 1),
                     concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2], ' ', tupleElement(x, 2)[3]),
                     tupleElement(x, 3))
           , c_tuples)                                            as c_tuples
-- from sp.eval_data_trec_genomic_2005_with_content;
-- from sp.eval_data_trec_cds_2014_with_content;
from sp.eval_data_relish_v1_with_content;


-- MeSH and Journal as input
select q_id,
       arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                              splitByChar('|', q_content[3]) as mesh_arr)) as q_mesh_headings,
       arrayDistinct(arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[2],
                                                            mesh_arr)))                    as q_mesh_qualifiers,
       q_content[4]                                                                        as q_journal,
       arrayMap(x->
                    (tupleElement(x, 1),
                     arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                                            splitByChar('|', tupleElement(x, 2)[3]) as c_mesh_arr)),
                     arrayDistinct(arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[2], c_mesh_arr))),
                     tupleElement(x, 2)[4],
                     tupleElement(x, 3)),
                c_tuples)                                                                  as c_tuples
from sp.eval_data_relish_v1_with_content
where rand() % 100 < 1;

-- Title + Abstract as input, using triplet loss
select q_id,
       train1_val2_test0,
       q_pm_id,
       q_content,
       c_pos_pm_id,
       c_pos_content,
       c_neg_pm_id,
       c_neg_content
from (
      select q_id,
             train1_val2_test0,
             q_pm_id,
             concat(q_content[1], ' ', q_content[2]) as q_content,
             tupleElement(arrayJoin(arrayFilter(y->tupleElement(y, 3) in (1, 2) and xxHash32(y) % 100 < 25,
                                                tmp_arr) as relevant_or_partial_tuples) as pos_item,
                          1)                         as c_pos_pm_id,
             tupleElement(pos_item, 2)               as c_pos_content,
             tupleElement(arrayJoin(
                                  arrayFilter(y->tupleElement(y, 3) = 0 and xxHash32(y) % 100 < 25,
                                              arrayMap(x-> (tupleElement(x, 1),
                                                            concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                                            tupleElement(x, 3))
                                                  , c_tuples) as tmp_arr) as irrelevant_tuples) as neg_item,
                          1)                         as c_neg_pm_id,
             tupleElement(neg_item, 2)               as c_neg_content
      from sp.eval_data_relish_v1_with_content
      where train1_val2_test0 in (1));

select q_id,
       train1_val2_test0,
       q_pm_id,
       q_content,
       c_tuples
from (
      select q_id,
             train1_val2_test0,
             q_pm_id,
             concat(q_content[1], ' ', q_content[2]) as q_content,
             arrayMap(x->
                          (tupleElement(x, 1),
                           concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2], ' ', tupleElement(x, 2)[3]),
                           tupleElement(x, 3))
                 , c_tuples)                         as c_tuples
      from sp.eval_data_relish_v1_with_content
      where train1_val2_test0 in (0, 2));

-- TODO should delete this table when we using faster retrieval algorithm, this table will be replaced by pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth
-- 998659 pubmed_paper_found_sample_similar_article.tsv
-- 998659
select count()
from sp.pubmed_randomly_selected_papers_found_similar_paper;

-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/pubmed_paper_found_sample_similar_article_using_matched_entity.tsv | clickhouse-local --input_format_allow_errors_ratio=1 --input-format=TSV --table='input' --structure="pm_id String, s1 String, s2 String, s3 String, s4 String, s5 String, s6 String"  --query="select pm_id, \
--        arrayMap(f1-> (JSONExtractString(f1, 'pm_id'), toFloat32(JSONExtractFloat(f1, 'score')), toUInt32(JSONExtractInt(f1, 'intersect'))), JSONExtractArrayRaw(s1)) as original_mesh_field_search_result, \
--        arrayMap(f2-> (JSONExtractString(f2, 'pm_id'), toFloat32(JSONExtractFloat(f2, 'score')), toUInt32(JSONExtractInt(f2, 'intersect'))), JSONExtractArrayRaw(s2)) as original_reference_field_search_result, \
--        arrayMap(f3-> (JSONExtractString(f3, 'pm_id'), toFloat32(JSONExtractFloat(f3, 'score')), toUInt32(JSONExtractInt(f3, 'intersect'))), JSONExtractArrayRaw(s3)) as original_mesh_reference_field_search_result, \
--        arrayMap(f4-> (JSONExtractString(f4, 'pm_id'), toFloat32(JSONExtractFloat(f4, 'score')), toUInt32(JSONExtractInt(f4, 'intersect'))), JSONExtractArrayRaw(s4)) as enhanced_mesh_field_search_result, \
--        arrayMap(f5-> (JSONExtractString(f5, 'pm_id'), toFloat32(JSONExtractFloat(f5, 'score')), toUInt32(JSONExtractInt(f5, 'intersect'))), JSONExtractArrayRaw(s5)) as enhanced_reference_field_search_result, \
--        arrayMap(f6-> (JSONExtractString(f6, 'pm_id'), toFloat32(JSONExtractFloat(f6, 'score')), toUInt32(JSONExtractInt(f6, 'intersect'))), JSONExtractArrayRaw(s6)) as enhanced_mesh_reference_field_search_result \
-- from input" --format=Native | clickhouse-client --query='INSERT INTO sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth FORMAT Native' --port=9001 --password=root

-- drop table sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth;
create table if not exists sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth
(
    pm_id                                       String,
    original_mesh_field_search_result           Array(Tuple(String, Float32, UInt32)),
    original_reference_field_search_result      Array(Tuple(String, Float32, UInt32)),
    original_mesh_reference_field_search_result Array(Tuple(String, Float32, UInt32)),
    enhanced_mesh_field_search_result           Array(Tuple(String, Float32, UInt32)),
    enhanced_reference_field_search_result      Array(Tuple(String, Float32, UInt32)),
    enhanced_mesh_reference_field_search_result Array(Tuple(String, Float32, UInt32))
) ENGINE = MergeTree order by length(pm_id);

-- 21817 pubmed_paper_found_sample_similar_article_using_matched_entity.tsv
-- 21816	200	200	200	200	199.990832416575	200
select count(),
       avg(length(original_mesh_field_search_result)),
       avg(length(original_reference_field_search_result)),
       avg(length(original_mesh_reference_field_search_result)),
       avg(length(enhanced_mesh_field_search_result)),
       avg(length(enhanced_reference_field_search_result)),
       avg(length(enhanced_mesh_reference_field_search_result))
from sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth;

-- visual the titles of similar papers and examine the results, to verified whether the dataset building method is good enough
select pm_id,
       clean_title as title,
       od
-- from pubmed.nft_paper
from (select pm_id, clean_title, clean_abstract, clean_mesh_headings, clean_keywords, datetime_str
      from fp.paper_clean_content)
         any
         inner join (
    select (arrayJoin(paper_its_similar_papres) as item)[1] as pm_id,
           item[2]                                          as od
    from (select arrayMap(i->
                              [toUInt64(pm_id_arr[i]), i], arrayEnumerate(arrayPushFront(
            arrayMap(x-> tupleElement(x, 1), enhanced_mesh_reference_field_search_result),
            pm_id) as pm_id_arr)) as paper_its_similar_papres
          from sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth
          order by rand()
          limit 1)) using pm_id
order by od
;

-- create evaluation dataset. the ranking results are computed by an information retrieval tool
-- verify how many broken instances
-- 5
select count()
from sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth
where length(original_mesh_field_search_result) == 0
   or length(original_reference_field_search_result) == 0
   or length(enhanced_mesh_field_search_result) == 0
   or length(enhanced_reference_field_search_result) == 0
   or length(enhanced_mesh_reference_field_search_result) == 0
;

-- TODO we may need to build more than one dataset using several strategies
-- to demonstrate our these strategies is better enough for building the "gold standard" evaluation dataset, we need to compute the words overlap
-- and we will see the overlapping is high than other strategies and the official result in PubMed.

-- Because different losses should take different forms of training/eval dataset, so we should adapt the training data.
-- Whereas the test dataset, it does not have any variants, because we want to evaluate the ranking performance.
-- So, next we will create the independent test dataset, and then create various forms of training data.

-- split the dataset
-- drop table sp.dataset_split;
create table if not exists sp.dataset_split
    ENGINE = MergeTree order by pm_id as
select *,
       -- the exercise is deterministic as a specific pm_id will have same xxhash32 code.
       multiIf(
                   function != 'dataset', -1, -- -1 as user study
                   (xxHash32(pm_id) % 100 as rand) < 10, 1, -- [0:10) as train
                   rand < 60, -2, -- [10:60) as drop
                   rand < 80, 0, -- [60:80) as test
                   2 -- [80:100) as val
           ) as train1_test0_val2
from sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth any
         -- associate function field
         inner join sp.pubmed_randomly_selected_papers using pm_id
where train1_test0_val2 in (-1, 0, 1, 2);

-- 0	198572
-- 1	99300
-- 2	198650
-- -1	4477
select train1_test0_val2, count()
from sp.dataset_split
group by train1_test0_val2;

-- drop table sp.unified_test_set;
create table if not exists sp.unified_test_set
    ENGINE = MergeTree order by pm_id as
    -- whether to filter out some dissimilar papers
-- with 0.3 as true_similar_articles_threshold
select pm_id,
       train1_test0_val2,
       (original_mesh_field_search_result as original_mesh_reference_field_search_result)[1] as meshref_entities,
       arrayMap(i1->
                    (tupleElement(sorted_arr1[i1], 1),
                     tupleElement(sorted_arr1[i1], 3) / tupleElement(meshref_entities, 3),
                     i1),
                arrayEnumerate(arrayReverseSort(x1->tupleElement(x1, 3),
                                                arraySlice(original_mesh_reference_field_search_result, 2)) as sorted_arr1)
           )                                                                                 as meshref_ranking,

       enhanced_mesh_reference_field_search_result[1]                                        as ehcmeshref_entities,
       arrayMap(i->
                    (tupleElement(sorted_arr[i], 1),
                     tupleElement(sorted_arr[i], 3) / tupleElement(ehcmeshref_entities, 3),
                     i),
                arrayEnumerate(arrayReverseSort(x->tupleElement(x, 3),
                                                arraySlice(enhanced_mesh_reference_field_search_result, 2)) as sorted_arr)
           )                                                                                 as ehcmeshref_ranking
from sp.dataset_split
where tupleElement(enhanced_mesh_reference_field_search_result[1], 1) == pm_id
  -- -1 represents data may used for user study
  and train1_test0_val2 in (-1, 0);

-- 0	198681
-- -1	4476
select train1_test0_val2, count()
from sp.unified_test_set
group by train1_test0_val2;

-- drop table sp.train_val_set_pairwise_sim_score;
create table if not exists sp.train_val_set_pairwise_sim_score
    ENGINE = MergeTree order by pm_id as
select pm_id,
       clean_title,
       clean_abstract,
       rcm_pm_id,
       rcm_clean_title,
       rcm_clean_abstract,
       train1_test0_val2,
       score
from (select toString(pm_id) as rcm_pm_id, clean_title as rcm_clean_title, clean_abstract rcm_clean_abstract
      from fp.paper_clean_content) any
         inner join (
    select toString(pm_id) as pm_id, clean_title, clean_abstract, train1_test0_val2, rcm_pm_id, score
    from (select pm_id, clean_title, clean_abstract from fp.paper_clean_content) any
             inner join (

        select pm_id,
               train1_test0_val2,
               tupleElement(item, 1) as rcm_pm_id,
               tupleElement(item, 2) as score
        from (select pm_id,
                     train1_test0_val2,
                     tupleElement(enhanced_mesh_reference_field_search_result[1], 3) as number_entities,
                     arrayMap(y->
                                  (tupleElement(y, 1), tupleElement(y, 3) / number_entities),
                              arrayReverseSort(x->tupleElement(x, 3),
                                               arraySlice(enhanced_mesh_reference_field_search_result, 2))
                         )                                                           as ehcmeshref_ranking
              from sp.dataset_split
              where tupleElement(enhanced_mesh_reference_field_search_result[1], 1) == pm_id
                and train1_test0_val2 in (1, 2)
                 )
                 array join ehcmeshref_ranking as item
        ) using pm_id) using rcm_pm_id
where lengthUTF8(clean_title) > 0
   or lengthUTF8(clean_abstract) > 0
   or lengthUTF8(rcm_clean_title) > 0
   or lengthUTF8(rcm_clean_title) > 0;

-- unified facet dataset
select pm_id,
       concat(clean_title, ' ', clean_abstract)         as content1,
       concat(rcm_clean_title, ' ', rcm_clean_abstract) as content2,
       score                                            as label_or_score,
       train1_test0_val2
from sp.train_val_set_pairwise_sim_score
order by rand();

-- 49347
select count(), count(distinct pm_id)
from sp.train_val_set_pairwise_sim_score;

select count()
from (
      select count() as cnt
      from sp.train_val_set_pairwise_sim_score
      group by pm_id
      order by cnt desc)
where cnt != 19;

select tupleElement(arrayJoin(arrayMap(
        i-> (i * 0.05, arrayCount(x->x > i * 0.05 and x <= (i + 1) * 0.05, groupArray(score) as scores)),
        range(20))) as itemx, 1) as prob,
       tupleElement(itemx, 2)    as cnt
from sp.train_val_set_pairwise_sim_score;

select q_id,
       train1_val2_test0,
       q_pm_id,
       q_content,
       c_pos_pm_id,
       c_pos_content,
       c_neg_pm_id,
       c_neg_content
from (
      select q_id,
             train1_val2_test0,
             q_pm_id,
             q_content,
             arrayMap(i->
                          [xxHash32(i, randomPrintableASCII(5)) % num_pos + 1, xxHash32(i, randomPrintableASCII(5)) % num_neg + 1],
                      range(num_sampled_instances))                 as pos_neg_idx,
             arrayJoin(arrayFilter(y->length(y[1].1) > 0 and length(y[2].1) > 0,
                                   arrayDistinct(
                                           arrayMap(idx->
                                                        [pos_arr[idx[1]],neg_arr[idx[2]]],
                                                    pos_neg_idx)))) as pos_neg_item,
             tupleElement(pos_neg_item[1], 1)                       as c_pos_pm_id,
             tupleElement(pos_neg_item[1], 2)                       as c_pos_content,
             tupleElement(pos_neg_item[2], 1)                       as c_neg_pm_id,
             tupleElement(pos_neg_item[2], 2)                       as c_neg_content
      from (with ['relish_v1', 'trec_genomic_2005', 'trec_cds_2014'] as available_datasets,
                [7, 0.07, 0.07] as sampling_factors,
                indexOf(available_datasets, 'trec_cds_2014') as dataset_idx,
                sampling_factors[dataset_idx] as dataset_sampling_factor
            select q_id,
                   train1_val2_test0,
                   q_pm_id,
                   concat(q_content[1], ' ', q_content[2])                                     as q_content,
                   arrayFilter(y->tupleElement(y, 3) in (2), arrayMap(x-> (tupleElement(x, 1),
                                                                           concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                                                                           tupleElement(x, 3))
                       , c_tuples) as tmp_arr)                                                    pos_arr,
                   arrayFilter(y->tupleElement(y, 3) in (0), tmp_arr)                             neg_arr,
                   length(pos_arr)                                                             as num_pos,
                   length(neg_arr)                                                             as num_neg,
                   toUInt32((num_pos > num_neg ? num_pos : num_neg) * dataset_sampling_factor) as num_sampled_instances
            from sp.eval_data_trec_cds_2014_with_content
            where train1_val2_test0 in (1))
      where num_pos > 0
        and num_neg > 0
         );

select sum(num_pos), sum(num_neg), sum(num_pos) / sum(num_neg)
from (
      select topic_id,
             arrayCount(x->x in (1, 2), groupArray(relevant_level)) as num_pos,
             arrayCount(x->x in (0), groupArray(relevant_level))    as num_neg
      from sp.eval_data_trec_genomic_2005
      group by topic_id)
;


-- drop table sp.eval_data_relish_v1_with_content_without_query;
create table if not exists sp.eval_data_relish_v1_with_content_without_query
    ENGINE = MergeTree order by id as
select id,
       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 1,
                                               groupArray((c_pm_id, c_clean_content, score, train1_val2_test0)) as c_tuples) as train_part_tmp),
           arrayFilter(y->y.3 in (1), train_part_tmp),
           arrayFilter(y->y.3 in (2), train_part_tmp)
           ] as train_part,

       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 2, c_tuples) as val_part_tmp),
           arrayFilter(y->y.3 in (1), val_part_tmp),
           arrayFilter(y->y.3 in (2), val_part_tmp)
           ] as val_part,

       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 0, c_tuples) as test_part_tmp),
           arrayFilter(y->y.3 in (1), test_part_tmp),
           arrayFilter(y->y.3 in (2), test_part_tmp)
           ] as test_part
from (
         select uid                        as id,
                pm_id                      as q_pm_id,
                toString((arrayJoin(arrayFilter(
                        x->x[2] != toUInt64(q_pm_id),
                        arrayConcat(arrayMap(x->
                                                 [2, toUInt64(x)], relevant),
                                    arrayMap(x->
                                                 [1, toUInt64(x)], partial),
                                    arrayMap(x->
                                                 [0, toUInt64(x)], irrelevant))))
                    as item)[2])           as c_pm_id,
                item[1]                    as score,
                [experience, is_anonymous] as other_metadata
         from (select *
               from sp.eval_data_relish_v1 any
                        inner join (select pm_id, count() as cnt
                                    from sp.eval_data_relish_v1
                                    group by pm_id
                                    having cnt = 1) using pm_id
                  )
         order by uid asc, q_pm_id asc, score desc) any
         inner join (select pm_id                                                                                                  as c_pm_id,
                            (xxHash32(pm_id) % 100 < 80 ? 1 :
                                                     (xxHash32(concat(pm_id, 'random string here')) % 100 < 50 ? 2 : 0))           as train1_val2_test0,
                            [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as c_clean_content
                     from sp.eval_data_related_pubmed_article_clean_metadata) using c_pm_id
group by id;

-- drop table sp.eval_data_trec_cds_2014_with_content_without_query;
create table if not exists sp.eval_data_trec_cds_2014_with_content_without_query
    ENGINE = MergeTree order by id as
select toString(topic_id) as id,
       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 1,
                                               groupArray((pm_id, c_clean_content, relevant_level, train1_val2_test0)) as c_tuples) as train_part_tmp),
           arrayFilter(y->y.3 in (1), train_part_tmp),
           arrayFilter(y->y.3 in (2), train_part_tmp)
           ]              as train_part,

       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 2, c_tuples) as val_part_tmp),
           arrayFilter(y->y.3 in (1), val_part_tmp),
           arrayFilter(y->y.3 in (2), val_part_tmp)
           ]              as val_part,

       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 0, c_tuples) as test_part_tmp),
           arrayFilter(y->y.3 in (1), test_part_tmp),
           arrayFilter(y->y.3 in (2), test_part_tmp)
           ]              as test_part

from sp.eval_data_trec_cds_2014 any
         inner join (select pm_id,
                            (xxHash32(pm_id) % 100 < 80 ? 1 :
                                                     (xxHash32(concat(pm_id, 'this is a random string here')) % 100 <
                                                      50 ? 2 :
                                                      0))                                                                          as train1_val2_test0,
                            [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as c_clean_content
                     from sp.eval_data_related_pubmed_article_clean_metadata) using pm_id
group by id
;

-- drop table sp.eval_data_trec_genomic_2005_with_content_without_query;
create table if not exists sp.eval_data_trec_genomic_2005_with_content_without_query
    ENGINE = MergeTree order by id as
select toString(topic_id) as id,

       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 1,
                                               groupArray((pm_id, c_clean_content, relevant_level, train1_val2_test0)) as c_tuples) as train_part_tmp),
           arrayFilter(y->y.3 in (1), train_part_tmp),
           arrayFilter(y->y.3 in (2), train_part_tmp)
           ]              as train_part,

       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 2, c_tuples) as val_part_tmp),
           arrayFilter(y->y.3 in (1), val_part_tmp),
           arrayFilter(y->y.3 in (2), val_part_tmp)
           ]              as val_part,

       [arrayFilter(y->y.3 in (0), arrayFilter(x->x.4 = 0, c_tuples) as test_part_tmp),
           arrayFilter(y->y.3 in (1), test_part_tmp),
           arrayFilter(y->y.3 in (2), test_part_tmp)
           ]              as test_part

from sp.eval_data_trec_genomic_2005 any
         inner join (select pm_id,
                            (xxHash32(pm_id) % 100 < 80 ? 1 :
                                                     (xxHash32(concat(pm_id, 'this is a random string here')) % 100 <
                                                      50 ? 2 :
                                                      0))                                                                          as train1_val2_test0,
                            [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as c_clean_content
                     from sp.eval_data_related_pubmed_article_clean_metadata) using pm_id
group by id
;


with (select count() from sp.eval_data_relish_v1) as original_cnt
select count() as cnt, original_cnt, 'relish_v1' as source
from sp.eval_data_relish_v1_with_content_without_query
union all
with (select count(distinct topic_id) from sp.eval_data_trec_cds_2014) as original_cnt
select count() as cnt, original_cnt, 'trec_cds_2014' as source
from sp.eval_data_trec_cds_2014_with_content_without_query
union all
with (select count(distinct topic_id) from sp.eval_data_trec_genomic_2005) as original_cnt
select count() as cnt, original_cnt, 'trec_genomic_2005' as source
from sp.eval_data_trec_genomic_2005_with_content_without_query;

-- Note test train/val/test distribution
select sum(arraySum(x-> length(x), train_part)) as num_train,
       sum(arraySum(x-> length(x), val_part))   as num_val,
       sum(arraySum(x-> length(x), test_part))  as num_test,
       num_train / num_val,
       num_train / num_test,
       'relish_v1'                              as source
from sp.eval_data_relish_v1_with_content_without_query
union all
select sum(arraySum(x-> length(x), train_part)) as num_train,
       sum(arraySum(x-> length(x), val_part))   as num_val,
       sum(arraySum(x-> length(x), test_part))  as num_test,
       num_train / num_val,
       num_train / num_test,
       'trec_cds_2014'                          as source
from sp.eval_data_trec_cds_2014_with_content_without_query
union all
select sum(arraySum(x-> length(x), train_part)) as num_train,
       sum(arraySum(x-> length(x), val_part))   as num_val,
       sum(arraySum(x-> length(x), test_part))  as num_test,
       num_train / num_val,
       num_train / num_test,
       'trec_genomic_2005'                      as source
from sp.eval_data_trec_genomic_2005_with_content_without_query;

-- [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str]
select id,
       arrayMap(x-> (tupleElement(x, 1),
                     (concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                      arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                                             splitByChar('|', tupleElement(x, 2)[3]) as train_mesh_arr)),
                      arrayDistinct(arrayFilter(n->length(n) > 0,
                                                arrayMap(m->splitByString('; ', m)[2], train_mesh_arr))),
                      tupleElement(x, 2)[4]),
                     tupleElement(x, 3)), arraySort(z->xxHash32(z.1), arrayFlatten(train_part))) as train_part,

       arrayMap(x-> (tupleElement(x, 1),
                     (concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                      arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                                             splitByChar('|', tupleElement(x, 2)[3]) as val_mesh_arr)),
                      arrayDistinct(arrayFilter(n->length(n) > 0,
                                                arrayMap(m->splitByString('; ', m)[2], val_mesh_arr))),
                      tupleElement(x, 2)[4]),
                     tupleElement(x, 3)), arraySort(z->xxHash32(z.1), arrayFlatten(val_part)))   as val_part,

       arrayMap(x-> (tupleElement(x, 1),
                     (concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2]),
                      arrayFilter(y->length(y) > 0, arrayMap(x->splitByString('; ', x)[1],
                                                             splitByChar('|', tupleElement(x, 2)[3]) as test_mesh_arr)),
                      arrayDistinct(arrayFilter(n->length(n) > 0,
                                                arrayMap(m->splitByString('; ', m)[2], test_mesh_arr))),
                      tupleElement(x, 2)[4]),
                     tupleElement(x, 3)), arraySort(z->xxHash32(z.1), arrayFlatten(test_part)))  as test_part
from sp.eval_data_relish_v1_with_content_without_query;

-- [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str]
select id,
       arrayMap(x-> (tupleElement(x, 1),
                     tupleElement(x, 3)), arraySort(z->xxHash32(z.1), arrayFlatten(train_part))) as train_part
from sp.eval_data_relish_v1_with_content_without_query;


-- Note dataset for contrastive loss
-- select count()
select id,
       pm_id1,
       content1,
       pm_id2,
       content2,
       score
from (with 5 as dataset_balance_ratio
      select id,
             arrayFilter(x->x[1] != x[2], arrayDistinct(
                     arrayMap(i->
                                  [xxHash32(i, randomPrintableASCII(5)) % num_pos + 1,
                                      xxHash32(i, 'xxx', randomPrintableASCII(5)) % num_pos + 1, 1],
                              range(num_sampled_pos_pos_instances))))                    as pos_pos_idx,

             arrayFilter(x->x[1] != x[2], arrayDistinct(
                     arrayMap(i->
                                  [xxHash32(i, randomPrintableASCII(5)) % num_neg + 1,
                                      xxHash32(i, 'yyy', randomPrintableASCII(5)) % num_neg + 1, 0],
                              range(num_sampled_neg_neg_instances))))                    as neg_neg_idx,

             length(pos_pos_idx) >
             length(neg_neg_idx) ?
             arrayConcat(arraySlice(pos_pos_idx, 1, length(neg_neg_idx) * dataset_balance_ratio), neg_neg_idx) :
             arrayConcat(pos_pos_idx, arraySlice(neg_neg_idx, 1, length(pos_pos_idx) *
                                                                 dataset_balance_ratio)) as pos_neg_idx,

             arrayJoin(arrayFilter(y->length(y.1.1) > 0 and length(y.2.1) > 0,
                                   arrayDistinct(
                                           arrayMap(idx->
                                                            idx[3] = 1 ?
                                                                     (pos_arr[idx[1]], pos_arr[idx[2]], 1) :
                                                                     (neg_arr[idx[1]], neg_arr[idx[2]], 0),
                                                    pos_neg_idx))))                      as pos_pos_or_neg_neg_pair_item,
             tupleElement(pos_pos_or_neg_neg_pair_item.1, 1)                             as pm_id1,
             tupleElement(pos_pos_or_neg_neg_pair_item.1, 2)                             as content1,
             tupleElement(pos_pos_or_neg_neg_pair_item.2, 1)                             as pm_id2,
             tupleElement(pos_pos_or_neg_neg_pair_item.2, 2)                             as content2,
             pos_pos_or_neg_neg_pair_item.3                                              as score
      from (with ['relish_v1', 'trec_genomic_2005', 'trec_cds_2014'] as available_datasets,
                [1.5, 12, 10] as sampling_factors,
                indexOf(available_datasets, 'trec_cds_2014') as dataset_idx,
                sampling_factors[dataset_idx] as dataset_sampling_factor
            select id,
                   -- [irrelevant -> partial -> relevant]
                   -- [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str]
                   arrayMap(x-> (tupleElement(x, 1), concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2])),
                            train_part[3])                        pos_arr,
                   arrayMap(x-> (tupleElement(x, 1), concat(tupleElement(x, 2)[1], ' ', tupleElement(x, 2)[2])),
                            train_part[1])                        neg_arr,
                   length(pos_arr)                             as num_pos,
                   length(neg_arr)                             as num_neg,
                   toUInt32(num_pos * dataset_sampling_factor) as num_sampled_pos_pos_instances,
                   toUInt32(num_neg * dataset_sampling_factor) as num_sampled_neg_neg_instances
--             from sp.eval_data_relish_v1_with_content_without_query
--             from sp.eval_data_trec_genomic_2005_with_content_without_query
            from sp.eval_data_trec_cds_2014_with_content_without_query
            where num_pos > 0
              and num_neg > 0)
         )
where length(pm_id1) > 0
  and length(pm_id2) > 0;

