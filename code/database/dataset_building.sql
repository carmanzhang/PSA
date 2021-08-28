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
select any(q_id)                                     as q_id, -- q_pm_id and q_id is 1 to 1
       q_pm_id,
       any(q_clean_content)                          as q_content,
       groupArray((c_pm_id, c_clean_content, score)) as c_tuples,
       any(other_metadata)                           as other_metadata
from (
         select q_id, q_pm_id, c_pm_id, score, q_clean_content, other_metadata
         from (
                  select uid                        as q_id,
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
                  inner join (select pm_id                                                                                                  as q_pm_id,
                                     [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as q_clean_content
                              from sp.eval_data_related_pubmed_article_clean_metadata) using q_pm_id) any
         inner join (select pm_id                                                                                                  as c_pm_id,
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
select any(q_id)                                     as q_id,
       q_pm_id,
       any(q_clean_content)                          as q_content,
       groupArray((c_pm_id, c_clean_content, score)) as c_tuples,
       any(other_metadata)                           as other_metadata
from (
--          select qid, q_pm_id, c_pm_id, score, q_clean_content, other_metadata
         select q_id,
                q_pm_id,
                tupleElement(arrayJoin(c_pm_id_score_arr) as item, 1) as c_pm_id,
                tupleElement(item, 2)                                 as score,
                q_clean_content,
                other_metadata
         from (
                  with ['within-topic', 'cross-topic'][1] as which_strategy_used_for_making_ranking_pool,
                      (select arraySort(arrayFilter(x-> length(x) > 0, groupUniqArray(pm_id)))
                       from sp.eval_data_trec_genomic_2005
                       where relevant_level in (1, 2)) as corss_topic_ranking_pool
                  select concat(toString(topic_id), '_', q_pm_id)                                                  as q_id,
                         arrayReverseSort(x->
                                              x.2,
                                          groupArray((pm_id, relevant_level)))                                     as pm_id_topical_relevant_scores,
                         tupleElement(arrayJoin(arrayFilter(x->x.2 in (1, 2), pm_id_topical_relevant_scores)) as item,
                                      1)                                                                           as q_pm_id,
                         tupleElement(item, 2)                                                                     as q_pm_id_topical_relevant_score,
                         arrayFilter(y->tupleElement(y, 1) != q_pm_id,
                                     which_strategy_used_for_making_ranking_pool == 'cross-topic' ? arrayMap(
                                             x-> (x,
                                                  if(length(arrayFilter(y->y.1 = x,
                                                                        pm_id_topical_relevant_scores) as tmp) = 1,
                                                     tmp[1].2, 0)),
                                             corss_topic_ranking_pool) :
                                                                                    pm_id_topical_relevant_scores) as c_pm_id_score_arr,
                         [toString(topic_id), toString(q_pm_id_topical_relevant_score)]                            as other_metadata
                  from sp.eval_data_trec_genomic_2005
                  where relevant_level in (0, 1, 2)
                  group by topic_id) any
                  inner join (select pm_id                                                                                                  as q_pm_id,
                                     [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as q_clean_content
                              from sp.eval_data_related_pubmed_article_clean_metadata) using q_pm_id) any
         inner join (select pm_id                                                                                                  as c_pm_id,
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
select any(q_id)                                     as q_id,
       q_pm_id,
       any(q_clean_content)                          as q_content,
       groupArray((c_pm_id, c_clean_content, score)) as c_tuples,
       any(other_metadata)                           as other_metadata
from (
--          select qid, q_pm_id, c_pm_id, score, q_clean_content, other_metadata
         select q_id,
                q_pm_id,
                tupleElement(arrayJoin(c_pm_id_score_arr) as item, 1) as c_pm_id,
                tupleElement(item, 2)                                 as score,
                q_clean_content,
                other_metadata
         from (
                  with ['within-topic', 'cross-topic'][1] as which_strategy_used_for_making_ranking_pool,
                      (select arraySort(arrayFilter(x-> length(x) > 0, groupUniqArray(pm_id)))
                       from sp.eval_data_trec_cds_2014
                       where relevant_level in (1, 2)) as corss_topic_ranking_pool
                  select concat(toString(topic_id), '_', q_pm_id)                                                  as q_id,
                         arrayReverseSort(x->
                                              x.2,
                                          groupArray((pm_id, relevant_level)))                                     as pm_id_topical_relevant_scores,
                         tupleElement(arrayJoin(arrayFilter(x->x.2 in (1, 2), pm_id_topical_relevant_scores)) as item,
                                      1)                                                                           as q_pm_id,
                         tupleElement(item, 2)                                                                     as q_pm_id_topical_relevant_score,

                         arrayFilter(y->tupleElement(y, 1) != q_pm_id,
                                     which_strategy_used_for_making_ranking_pool == 'cross-topic' ? arrayMap(
                                             x-> (x,
                                                  if(length(arrayFilter(y->y.1 = x,
                                                                        pm_id_topical_relevant_scores) as tmp) = 1,
                                                     tmp[1].2, 0)),
                                             corss_topic_ranking_pool) :
                                                                                    pm_id_topical_relevant_scores) as c_pm_id_score_arr,

                         [toString(topic_id), toString(q_pm_id_topical_relevant_score)]                            as other_metadata
                  from sp.eval_data_trec_cds_2014
                  where relevant_level in (0, 1, 2)
                  group by topic_id) any
                  inner join (select pm_id                                                                                                  as q_pm_id,
                                     [clean_title, clean_abstract, arrayStringConcat(two_level_mesh_arr, '|'), journal_title, datetime_str] as q_clean_content
                              from sp.eval_data_related_pubmed_article_clean_metadata) using q_pm_id) any
         inner join (select pm_id                                                                                                  as c_pm_id,
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
