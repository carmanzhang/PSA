create database if not exists sp;

-- PubMed
-- positive/negative samples

-- for a paper, find its MeSH/Reference/Enhanced Reference using European PubMed/Bern-Bio-Entity/Full-text-Matched-MeSH
-- TODO we can also add some networks features to improve the recommendation system. not only from semantic similarity. However, we need build graphs

create table if not exists sp.pubmed_paper_extracted_mesh_reference ENGINE = MergeTree order by pm_id as
select pm_id, two_level_mesh_arr, two_level_mesh_ui_arr, reference
from (
      select toString(pm_id)                                                                                                                         as pm_id,
             mesh_headings,
             JSONExtractArrayRaw(CAST(mesh_headings, 'String'))                                                                                      as mesh_heading_content_list,
             arrayMap(x ->JSONExtractString(x, 'descriptorName'),
                      mesh_heading_content_list)                                                                                                     as descriptor_names,
             arrayMap(x ->JSONExtractString(x, 'descriptorNameUI'),
                      mesh_heading_content_list)                                                                                                     as descriptor_name_uis,

             arrayMap(x ->
                          arrayMap(y -> JSONExtractString(y, 'descriptorName'),
                                   JSONExtractArrayRaw(x, 'qualifierNameList') as qualifierNameList),
                      mesh_heading_content_list)                                                                                                     as qualifier_name_lists,

             arrayMap(x ->
                          arrayMap(y -> JSONExtractString(y, 'descriptorNameUI'),
                                   JSONExtractArrayRaw(x, 'qualifierNameList')),
                      mesh_heading_content_list)                                                                                                     as qualifier_name_ui_lists,

             arrayFlatten(arrayMap(i->length(qualifier_name_ui_lists[i]) ==
                                      0 ? [descriptor_name_uis[i]]: arrayMap(x->concat(descriptor_name_uis[i], '|', x),
                                                                             qualifier_name_ui_lists[i]),
                                   arrayEnumerate(descriptor_name_uis)))                                                                             as two_level_mesh_ui_arr,

             arrayFlatten(arrayMap(i->length(qualifier_name_lists[i]) ==
                                      0 ? [descriptor_names[i]]: arrayMap(x->concat(descriptor_names[i], '; ', x),
                                                                          qualifier_name_lists[i]),
                                   arrayEnumerate(descriptor_names)))                                                                                as two_level_mesh_arr,

             arrayMap(x->JSONExtractString(x, 'articleId'), arrayFilter(x->JSONExtractString(x, 'idType') == 'pubmed',
                                                                        arrayFlatten(arrayMap(
                                                                                x->JSONExtractArrayRaw(x, 'articleIdList'),
                                                                                JSONExtractArrayRaw(cast(references, 'String'), 'referenceList'))))) as reference

      from pubmed.nft_paper);

-- 'cited_match' means whether this citation is corrected matched to a PubMed paper
-- 1,N,993
-- 0,N,110738958
-- 1,Y,330869640
select length(cited_pm_id) > 0 as x1, cited_match, count()
from pubmed.europen_pubmed_citation_network
group by x1, cited_match;


-- 441609591
select count()
from pubmed.europen_pubmed_citation_network;

--  69434856
select count()
from pubmed.europen_pubmed_citation_network
where length(cited_title) == 0;

-- TODO
select cited_pm_id, citing_pm_id, 1 as weight
from pubmed.europen_pubmed_citation_network
where length(citing_pm_id) > 0
  and match(citing_pm_id, '^[0-9]+$')
  and length(cited_pm_id) > 0
  and match(cited_pm_id, '^[0-9]+$');

-- European PubMed Citation Network
-- used around 57GB memory
-- drop table sp.european_pubmed_paper_references;
create table if not exists sp.european_pubmed_paper_references ENGINE = MergeTree order by length(pm_id) as
select pm_id, groupUniqArray(cited_pid) as references
from (
      select citing_pm_id as pm_id,
             length(cited_pm_id) > 0 ? cited_pm_id :
                                   lowerUTF8(replaceAll(MACNumToString(
                                                                xxHash64(arrayStringConcat(
                                                                        arrayFilter(x->x >= 'a' and x <= 'z',
                                                                                    extractAll(lowerUTF8(cited_title), '.')),
                                                                        ''))
                                                            ), ':', ''))
                          as cited_pid
      from pubmed.europen_pubmed_citation_network
      where length(citing_pm_id) > 0
        and (length(cited_pm_id) > 0 or length(cited_title) > 0)
         )
group by pm_id;

-- 13350487
select count()
from sp.european_pubmed_paper_references;

-- drop table sp.pubmed_paper_bern_entities;
create table if not exists sp.pubmed_paper_bern_entities ENGINE = MergeTree order by length(pm_id) as
select cast(toString(pmid), 'String') as pm_id,
       groupUniqArray(lowerUTF8(
               replaceAll(
                       MACNumToString(
                               xxHash64(lowerUTF8(mention))
                           ),
                       ':', '')
           ))                         as bern_entity_id
from pr.pubmed_bern_main
where pmid != 0
group by pm_id;

-- 18361409
select count()
from sp.pubmed_paper_bern_entities;


-- clickhouse-client --password root --port 9001 --input_format_allow_errors_ratio=0.1 --query='insert into sp.pubmed_paper_abstract_matched_mesh_using_string_matching FORMAT CSV' < /home/zhangli/mesh-data/pubmed_abstract_mesh_matches.txt

-- cat /home/zhangli/mesh-data/pubmed_abstract_mesh_matches.txt | clickhouse-local --input-format=TSV --table='input' --structure="line String"  --query="select \
-- (splitByChar('|', line) as fields)[1] as pm_id, toUInt32(fields[2]) as freq, fields[3] as mesh from input" --format=Native | clickhouse-client --query='INSERT INTO sp.pubmed_paper_abstract_matched_mesh_using_string_matching FORMAT Native' --port=9001 --password=root
create table if not exists sp.pubmed_paper_abstract_matched_mesh_using_string_matching
(
    pm_id String,
    freq  UInt32,
    mesh  String
) ENGINE = MergeTree order by length(pm_id);

-- 372899456
select count()
from sp.pubmed_paper_abstract_matched_mesh_using_string_matching;

-- drop table sp.pubmed_paper_abstract_matched_mesh;
-- create table if not exists sp.pubmed_paper_abstract_matched_mesh ENGINE = MergeTree order by length(pm_id) as
-- select pm_id,
--        groupUniqArray(lowerUTF8(
--                replaceAll(
--                        MACNumToString(
--                                xxHash64(lowerUTF8(mesh))
--                            ),
--                        ':', '')
--            )) as matched_mesh_id
-- from sp.pubmed_paper_abstract_matched_mesh_using_string_matching
-- group by pm_id;


-- 344359960
-- 372899456
select pm_id, mesh_id
from sp.pubmed_paper_abstract_matched_mesh_using_string_matching any
         inner join (select mesh_keyword as mesh, mesh_id from pr.mesh_id_keyword_map) using mesh;

-- used around 56GB memory
create table if not exists sp.pubmed_paper_abstract_matched_mesh ENGINE = MergeTree order by length(pm_id) as
select pm_id, groupUniqArray(mesh_id) as matched_mesh_ids
from (
         select pm_id, mesh
         from sp.pubmed_paper_abstract_matched_mesh_using_string_matching any
                  inner join
              (select pm_id, count() as cnt
               from sp.pubmed_paper_abstract_matched_mesh_using_string_matching
               group by pm_id
               having cnt <= 30) using pm_id
         ) any
         inner join
     (select mesh_keyword as mesh, mesh_id from pr.mesh_id_keyword_map) using mesh
group by pm_id;

select count()
from sp.pubmed_paper_abstract_matched_mesh;

-- for a paper, we have found its MeSH/Reference/Enhanced Reference using European PubMed/Bern-Bio-Entity/Full-text-Matched-MeSH
-- 30419647
select count()
from sp.pubmed_paper_extracted_mesh_reference;
-- 13350487
select count()
from sp.european_pubmed_paper_references;
-- 18361409
select count()
from sp.pubmed_paper_bern_entities;
-- 19118690
select count()
from sp.pubmed_paper_abstract_matched_mesh;

-- used around 35G memory
create table if not exists sp.pubmed_paper_mesh_reference_bioentity ENGINE = MergeTree order by pm_id as
select pm_id, mesh_ids, references, european_pm_references, bern_entity_ids, matched_mesh_ids
from (
         select pm_id, mesh_ids, references, european_pm_references, bern_entity_ids
         from (
                  select pm_id, two_level_mesh_ui_arr as mesh_ids, reference as references, european_pm_references
                  from sp.pubmed_paper_extracted_mesh_reference any
                           left join (
                      select pm_id, references as european_pm_references
                      from sp.european_pubmed_paper_references) using pm_id
                  ) any
                  left join (select pm_id, bern_entity_id as bern_entity_ids from sp.pubmed_paper_bern_entities)
                            using pm_id
         ) any
         left join sp.pubmed_paper_abstract_matched_mesh using pm_id;

-- 30419647
select count()
from sp.pubmed_paper_mesh_reference_bioentity;

-- drop table sp.pubmed_mesh_citation_unfold;
create table if not exists sp.pubmed_mesh_citation_unfold ENGINE = Log as
select pm_id,
       (arrayJoin(arrayConcat(
               arrayMap(x1->
                            [x1, 'm'], mesh_ids),
               arrayMap(x2->
                            [x2, 'r'], references),
               arrayMap(x3->
                            [x3, 'er'], european_pm_references),
               arrayMap(x4->
                            [x4, 'be'], bern_entity_ids),
               arrayMap(x5->
                            [x5, 'mm'], matched_mesh_ids)
           )) as entity_source)[1] as entity,
       entity_source[2]            as source
from sp.pubmed_paper_mesh_reference_bioentity;

-- 1317100543
select count()
from sp.pubmed_mesh_citation_unfold;

-- drop table sp.pubmed_mesh_citation_unified_id;
create table if not exists sp.pubmed_mesh_citation_unified_id ENGINE = Log as
select entity, cnt, rowNumberInBlock() as id
from (
      select entity,
             count() as cnt
      from (select entity
            from sp.pubmed_mesh_citation_unfold)
      group by entity
      order by cnt desc)
;

-- 54790949
select count()
from sp.pubmed_mesh_citation_unified_id;

-- drop table sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part1;
-- drop table sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part2;
-- drop table sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part3;
-- drop table sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part4;
-- drop table sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part5;
create table if not exists sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part1
    ENGINE = Log as
select pm_id, groupUniqArray(id) as mesh_ids
from (select * from sp.pubmed_mesh_citation_unfold where source in ('m')) any
         inner join sp.pubmed_mesh_citation_unified_id using entity
group by pm_id;
create table if not exists sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part2
    ENGINE = Log as
select pm_id, groupUniqArray(id) as references
from (select * from sp.pubmed_mesh_citation_unfold where source in ('r')) any
         inner join sp.pubmed_mesh_citation_unified_id using entity
group by pm_id;
create table if not exists sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part3
    ENGINE = Log as
select pm_id, groupUniqArray(id) as european_pm_references
from (select * from sp.pubmed_mesh_citation_unfold where source in ('er')) any
         inner join sp.pubmed_mesh_citation_unified_id using entity
group by pm_id;
create table if not exists sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part4
    ENGINE = Log as
select pm_id, groupUniqArray(id) as bern_entity_ids
from (select * from sp.pubmed_mesh_citation_unfold where source in ('be')) any
         inner join sp.pubmed_mesh_citation_unified_id using entity
group by pm_id;
create table if not exists sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part5
    ENGINE = Log as
select pm_id, groupUniqArray(id) as matched_mesh_ids
from (select * from sp.pubmed_mesh_citation_unfold where source in ('mm')) any
         inner join sp.pubmed_mesh_citation_unified_id using entity
group by pm_id;

select *
from (
      select 'part1' as name, count() as cnt
      from sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part1
      union all
      select 'part2' as name, count() as cnt
      from sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part2
      union all
      select 'part3' as name, count() as cnt
      from sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part3
      union all
      select 'part4' as name, count() as cnt
      from sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part4
      union all
      select 'part5' as name, count() as cnt
      from sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part5)
order by name;

-- drop table sp.pubmed_paper_mesh_reference_bioentity_with_unified_id;
create table if not exists sp.pubmed_paper_mesh_reference_bioentity_with_unified_id
    ENGINE = MergeTree
        order by pm_id as
select *
from (
         select *
         from (
                  select *
                  from (
                           select *
                           from (select pm_id from sp.pubmed_paper_mesh_reference_bioentity)
                                    any
                                    left join sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part1
                                              using pm_id
                           ) any
                           left join sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part2
                                     using pm_id) any
                  left join sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part3 using pm_id) any
         left join sp.pubmed_paper_mesh_reference_bioentity_with_unified_id_part4 using pm_id;

-- 30419647
select count()
from sp.pubmed_paper_mesh_reference_bioentity_with_unified_id;

-- -- drop table sp.pubmed_paper_mesh_reference_bioentity_with_unified_id
-- create table if not exists sp.pubmed_paper_mesh_reference_bioentity_with_unified_id ENGINE = MergeTree order by pm_id as
-- select pm_id,
--        arrayMap(y1->y1[1], arrayFilter(x1->x1[2] = 'm', groupArray([source, toString(id)]) as temp_arr)) as mesh_ids,
--        arrayMap(y2->y2[1], arrayFilter(x2->x2[2] = 'r', temp_arr))                                       as references,
--        arrayMap(y3->y3[1], arrayFilter(x3->x3[2] = 'er',
--                                        temp_arr))                                                        as european_pm_references,
--        arrayMap(y4->y4[1], arrayFilter(x4->x4[2] = 'be',
--                                        temp_arr))                                                        as bern_entity_ids
-- --        arrayMap(y5->y5[1], arrayFilter(x5->x5[2] = 'mm', temp_arr))  as matched_mesh_ids
-- from (
--       select pm_id,
--              (arrayJoin(arrayConcat(
--                      arrayMap(x1->
--                                   [x1, 'm'], mesh_ids),
--                      arrayMap(x2->
--                                   [x2, 'r'], references),
--                      arrayMap(x3->
--                                   [x3, 'er'], european_pm_references),
--                      arrayMap(x4->
--                                   [x4, 'be'], bern_entity_ids)
-- --                      arrayMap(x5->
-- --                                   [x5, 'mm'], matched_mesh_ids)
--                  )) as entity_source)[1] as entity,
--              entity_source[2]            as source,
--              id
--       from sp.pubmed_paper_mesh_reference_bioentity any
--                inner join sp.pubmed_mesh_citation_unified_id using entity)
-- group by pm_id
-- ;


-- Common neurosurgical diseases, may be used in user study.
-- drop table sp.pubmed_randomly_selected_papers;
create table if not exists sp.pubmed_randomly_selected_papers ENGINE = MergeTree order by pm_id as
select *
from sp.pubmed_paper_mesh_reference_bioentity any
         inner join (
    select pm_id,
           has(groupArray(source) as sources, 'dataset') and length(sources) > 1 ? 'dataset': sources[1] as function
    from (
          select pm_id, 'dataset' as source
          from sp.pubmed_paper_mesh_reference_bioentity
          where rand(xxHash32(now64(), pm_id)) % 100 < 30
            and length(mesh_ids) > 0
            and length(references) > 0
          order by rand(xxHash32(pm_id))
          limit 1000000
          union all
          select toString(pm_id) as pm_id, 'user-study-glioma' as source
          from pubmed.nft_paper
          where positionCaseInsensitive(article_title, 'brain glioma') > 0
          union all
          select toString(pm_id) as pm_id, 'user-study-epilepsy' as source
          from pubmed.nft_paper
          where positionCaseInsensitive(article_title, 'epilepsy') > 0)
    group by pm_id) using pm_id
where length(mesh_ids) > 0
  and length(references) > 0
;


-- dataset,1000000
-- user-study-epilepsy,4442
-- user-study-glioma,53
select function, count()
from sp.pubmed_randomly_selected_papers
group by function
order by function desc;


create table if not exists sp.pubmed_randomly_selected_pm_ids_for_development ENGINE = MergeTree order by pm_id as
select pm_id, function
from sp.pubmed_randomly_selected_papers;

-- running 26 minutes while using 50GB RAM
-- drop table sp.pubmed_paper_clean_content_with_originalentity_entityid_selectedsampledataset;
create table if not exists sp.pubmed_paper_clean_content_with_originalentity_entityid_selectedsampledataset
    ENGINE = MergeTree order by pm_id as
select *
from (
         select *
         from (
                  select *
                  from (
                           select toString(pm_id) as pm_id, clean_title, clean_abstract
                           from fp.paper_clean_content) any
                           left join sp.pubmed_paper_mesh_reference_bioentity_with_unified_id
                                     using pm_id
                  ) any
                  left join (select pm_id,
                                    mesh_ids               as original_mesh_ids,
                                    references             as original_references,
                                    european_pm_references as original_european_pm_references,
                                    bern_entity_ids        as original_bern_entity_ids
                             from sp.pubmed_paper_mesh_reference_bioentity) using pm_id) any
         left join (select * from sp.pubmed_randomly_selected_pm_ids_for_development) using pm_id;

-- 30419647
select count()
from sp.pubmed_paper_clean_content_with_originalentity_entityid_selectedsampledataset;

-- drop table sp.pubmed_paper_content_entity_data_for_indexing_searching;
create view if not exists sp.pubmed_paper_content_entity_data_for_indexing_searching as
select pm_id,
       content,
       arrayStringConcat(mesh_ids, ' ')                                                   as original_mesh,
       arrayStringConcat(references, ' ')                                                 as original_reference,
       arrayStringConcat(arrayDistinct(arrayConcat(mesh_ids, references)), ' ')           as original_mesh_reference,
       arrayStringConcat(arrayDistinct(arrayConcat(mesh_ids, bern_entity_ids)) as m, ' ') as enhanced_mesh,
       arrayStringConcat(arrayDistinct(arrayConcat(references, european_pm_references)) as n,
                         ' ')                                                             as enhanced_reference,
       arrayStringConcat(arrayDistinct(arrayConcat(m, n)), ' ')                           as enhanced_mesh_reference,
       function
from (select pm_id,
             concat(clean_title, ' ', clean_abstract)           as content,
             arrayMap(x1->toString(x1), mesh_ids)               as mesh_ids,
             arrayMap(x2->toString(x2), references)             as references,
             arrayMap(x3->toString(x3), european_pm_references) as european_pm_references,
             arrayMap(x4->toString(x4), bern_entity_ids)        as bern_entity_ids,
             function
      from sp.pubmed_paper_clean_content_with_originalentity_entityid_selectedsampledataset)
;

-- 30419647
select count()
from sp.pubmed_paper_content_entity_data_for_indexing_searching
;
-- 1004514
select count()
from sp.pubmed_paper_content_entity_data_for_indexing_searching
where length(function) > 0;

