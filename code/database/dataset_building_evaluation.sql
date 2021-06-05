-- -- used ~11 GB Disk space
-- -- drop table sp.pubmed_all_paper_clean_content_joinget;
-- create table if not exists sp.pubmed_all_paper_clean_content_joinget ENGINE = Join(any, left, pm_id) as
-- select pm_id, concat(clean_title, '\t', clean_abstract) as clean_content, datetime_str
-- from fp.paper_clean_content;

-- -- 30418043
select count()
from pr.pmc_paper_info_joinget;

-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/pubmed_paper_found_sample_similar_article_using_bm25.tsv | clickhouse-local --input_format_allow_errors_ratio=1 --input-format=TSV --table='input' --structure="pm_id String, s1 String"  --query="select pm_id, \
--        arrayMap(f1-> (JSONExtractString(f1, 'pm_id'), toFloat32(JSONExtractFloat(f1, 'score')), toUInt32(JSONExtractInt(f1, 'intersect'))), JSONExtractArrayRaw(s1)) as title_abstract_field_search_result \
-- from input" --format=Native | clickhouse-client --query='INSERT INTO sp.pubmed_randomly_selected_papers_found_similar_paper_using_lexical_bm25 FORMAT Native' --port=9001 --password=root
-- drop table sp.pubmed_randomly_selected_papers_found_similar_paper_using_lexical_bm25;
create table if not exists sp.pubmed_randomly_selected_papers_found_similar_paper_using_lexical_bm25
(
    pm_id                              String,
    title_abstract_field_search_result Array(Tuple(String, Float32, UInt32))
) ENGINE = MergeTree order by length(pm_id);

-- 40353 pubmed_paper_found_sample_similar_article_using_bm25.tsv
-- 40353
select count()
from sp.pubmed_randomly_selected_papers_found_similar_paper_using_lexical_bm25;


-- drop table sp.decision_explain_about_ground_truth_dataset_building;
create table if not exists sp.decision_explain_about_ground_truth_dataset_building ENGINE = Log as
select pm_id,
       clean_title1,
       clean_abstract1,
       similar_pm_id,
       clean_title2,
       clean_abstract2,
       source
from (
         select toString(pm_id) as similar_pm_id,
                clean_title     as clean_title2,
                clean_abstract  as clean_abstract2
         from fp.paper_clean_content) all
         inner join (
    select pm_id, clean_title1, clean_abstract1, similar_pm_id, source
    from (
             select toString(pm_id) as pm_id,
                    clean_title     as clean_title1,
                    clean_abstract  as clean_abstract1
             from fp.paper_clean_content)
             all
             inner join (
        select pm_id, (arrayJoin(ranked_simialr_papers) as item)[1] as similar_pm_id, item[2] as source
        from (
              select pm_id,
                     arraySort([length(pubmed_pubmed),
                         length(title_abstract_field_search_result),
                         length(original_mesh_field_search_result),
                         length(original_reference_field_search_result),
                         length(original_mesh_reference_field_search_result),
                         length(enhanced_mesh_field_search_result),
                         length(enhanced_reference_field_search_result),
                         length(enhanced_mesh_reference_field_search_result)])[1]                                                                               as min_len,

                     arrayConcat(
                             arrayMap(x->
                                          [tupleElement(x, 1), 'bm'],
                                      arraySlice(title_abstract_field_search_result, 1, min_len)) as ranked_simialr_papers_by_bm25,
                             arrayMap(x->
                                          [x[1], 'pm'],
                                      arraySlice(pubmed_pubmed, 1, min_len)) as ranked_simialr_papers_by_pubmed_official,
                             arrayMap(x->
                                          [tupleElement(x, 1), 'om'],
                                      arraySlice(original_mesh_field_search_result, 1, min_len)) as ranked_simialr_papers_by_original_mesh,
                             arrayMap(x->
                                          [tupleElement(x, 1), 'or'],
                                      arraySlice(original_reference_field_search_result, 1, min_len)) as ranked_simialr_papers_by_original_reference,
                             arrayMap(x->
                                          [tupleElement(x, 1), 'omr'],
                                      arraySlice(original_mesh_reference_field_search_result, 1, min_len)) as ranked_simialr_papers_by_original_mesh_reference,
                             arrayMap(x->
                                          [tupleElement(x, 1), 'em'],
                                      arraySlice(enhanced_mesh_field_search_result, 1, min_len)) as ranked_simialr_papers_by_enhanced_mesh,
                             arrayMap(x->
                                          [tupleElement(x, 1), 'er'],
                                      arraySlice(enhanced_reference_field_search_result, 1, min_len)) as ranked_simialr_papers_by_enhanced_reference,
                             arrayMap(x->
                                          [tupleElement(x, 1), 'emr'],
                                      arraySlice(enhanced_mesh_reference_field_search_result, 1, min_len)) as ranked_simialr_papers_by_enhanced_mesh_reference) as ranked_simialr_papers
              from (select pm_id,
                           title_abstract_field_search_result,
                           original_mesh_field_search_result,
                           original_reference_field_search_result,
                           original_mesh_reference_field_search_result,
                           enhanced_mesh_field_search_result,
                           enhanced_reference_field_search_result,
                           enhanced_mesh_reference_field_search_result
                    from (select pm_id,
                                 original_mesh_field_search_result,
                                 original_reference_field_search_result,
                                 original_mesh_reference_field_search_result,
                                 enhanced_mesh_field_search_result,
                                 enhanced_reference_field_search_result,
                                 enhanced_mesh_reference_field_search_result
                          from (select pm_id from sp.pubmed_randomly_selected_papers) any
                                   inner join sp.pubmed_randomly_selected_papers_found_similar_paper_potential_ground_truth
                                              using pm_id) any
                             inner join sp.pubmed_randomly_selected_papers_found_similar_paper_using_lexical_bm25
                                        using pm_id) any
                       inner join (select pm_id, pubmed_pubmed from sp.pubmed_official_similar_paper) using pm_id
                 )
        ) using pm_id) using similar_pm_id;


select source,
       count()                                                                             as cnt,
       sum(length(arrayIntersect(splitByChar(' ', clean_title1) as title_token_1,
                                 splitByChar(' ', clean_title2) as title_token_2))) / cnt  as title_intersect,
       sum(length(arrayIntersect(splitByChar(' ', clean_abstract1) as abs_token_1,
                                 splitByChar(' ', clean_abstract2) as abs_token_2))) / cnt as abstract_intersect,
       title_intersect + abstract_intersect                                                as title_abstract_intersect
from sp.decision_explain_about_ground_truth_dataset_building
group by source
order by title_abstract_intersect desc
;

-- 2854359	emr
-- 2495504	em
-- 2490988	omr
-- 2278401	om
-- 2185027	er
-- 2077548	pm
-- 1924068	bm
-- 1673860	or
select sum(length(arrayIntersect(paper_entities1, paper_entities2))) as entity_intersect,
       source
from (
         select pm_id                                                                               as similar_pm_id,
                arrayDistinct(
                        arrayConcat(mesh_ids, references, european_pm_references, bern_entity_ids)) as paper_entities2
         from sp.pubmed_paper_mesh_reference_bioentity) all
         inner join (
    select pm_id, paper_entities1, similar_pm_id, source
    from (
             select pm_id,
                    arrayDistinct(
                            arrayConcat(mesh_ids, references, european_pm_references,
                                        bern_entity_ids)) as paper_entities1
             from sp.pubmed_paper_mesh_reference_bioentity)
             all
             inner join (select pm_id, similar_pm_id, source
                         from sp.decision_explain_about_ground_truth_dataset_building)
                        using pm_id) using similar_pm_id
group by source
order by entity_intersect desc
;
