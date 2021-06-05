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
