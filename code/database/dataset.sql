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

-- used around 35 memory
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


-- TODO select top related paper for the each paper in the sample dataset
select pm_id,
       arrayStringConcat(mesh_ids, ' ')                                                   as original_mesh,
       arrayStringConcat(references, ' ')                                                 as original_reference,
       arrayStringConcat(arrayDistinct(arrayConcat(mesh_ids, bern_entity_ids) as m), ' ') as enhanced_mesh,
       arrayStringConcat(arrayDistinct(arrayConcat(references, european_pm_references) as n),
                         ' ')                                                             as enhanced_reference,
       arrayStringConcat(arrayDistinct(arrayConcat(m, n)), ' ')                           as enhanced_mesh_reference
from sp.pubmed_paper_mesh_reference_bioentity
where length(mesh_ids) > 0
   or length(references) > 0
   or length(european_pm_references) > 0
   or length(bern_entity_ids) > 0
   or length(matched_mesh_ids) > 0
;

select pm_id,
       arrayStringConcat(mesh_ids, ' ')                                                   as original_mesh,
       arrayStringConcat(references, ' ')                                                 as original_reference,
       arrayStringConcat(arrayDistinct(arrayConcat(mesh_ids, bern_entity_ids) as m), ' ') as enhanced_mesh,
       arrayStringConcat(arrayDistinct(arrayConcat(references, european_pm_references) as n),
                         ' ')                                                             as enhanced_reference,
       arrayStringConcat(arrayDistinct(arrayConcat(m, n)), ' ')                           as enhanced_mesh_reference
from sp.pubmed_randomly_selected_papers;

-- TODO
select cited_pm_id, citing_pm_id, 1 as weight
from pubmed.europen_pubmed_citation_network
where length(citing_pm_id) > 0
  and match(citing_pm_id, '^[0-9]+$')
  and length(cited_pm_id) > 0
  and match(cited_pm_id, '^[0-9]+$');

-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/pubmed_paper_found_sample_similar_article.tsv | clickhouse-local --input_format_allow_errors_ratio=0.1 --input-format=TSV --table='input' --structure="pm_id String, s1 String, s2 String, s3 String, s4 String, s5 String"  --query="select pm_id, \
--        arrayMap(f1-> (JSONExtractString(f1, 'pm_id'), toFloat32(JSONExtractFloat(f1, 'score')), toUInt32(JSONExtractInt(f1, 'intersect'))), JSONExtractArrayRaw(s1)) as original_mesh_field_search_result, \
--        arrayMap(f2-> (JSONExtractString(f2, 'pm_id'), toFloat32(JSONExtractFloat(f2, 'score')), toUInt32(JSONExtractInt(f2, 'intersect'))), JSONExtractArrayRaw(s2)) as original_reference_field_search_result, \
--        arrayMap(f3-> (JSONExtractString(f3, 'pm_id'), toFloat32(JSONExtractFloat(f3, 'score')), toUInt32(JSONExtractInt(f3, 'intersect'))), JSONExtractArrayRaw(s3)) as enhanced_mesh_field_search_result, \
--        arrayMap(f4-> (JSONExtractString(f4, 'pm_id'), toFloat32(JSONExtractFloat(f4, 'score')), toUInt32(JSONExtractInt(f4, 'intersect'))), JSONExtractArrayRaw(s4)) as enhanced_reference_field_search_result, \
--        arrayMap(f5-> (JSONExtractString(f5, 'pm_id'), toFloat32(JSONExtractFloat(f5, 'score')), toUInt32(JSONExtractInt(f5, 'intersect'))), JSONExtractArrayRaw(s5)) as enhanced_mesh_reference_field_search_result \
-- from input" --format=Native | clickhouse-client --query='INSERT INTO sp.pubmed_randomly_selected_papers_found_similar_paper FORMAT Native' --port=9001 --password=root

-- drop table sp.pubmed_randomly_selected_papers_found_similar_paper;
create table if not exists sp.pubmed_randomly_selected_papers_found_similar_paper
(
    pm_id                                       String,
    original_mesh_field_search_result           Array(Tuple(String, Float32, UInt32)),
    original_reference_field_search_result      Array(Tuple(String, Float32, UInt32)),
    enhanced_mesh_field_search_result           Array(Tuple(String, Float32, UInt32)),
    enhanced_reference_field_search_result      Array(Tuple(String, Float32, UInt32)),
    enhanced_mesh_reference_field_search_result Array(Tuple(String, Float32, UInt32))
) ENGINE = MergeTree order by length(pm_id);

-- 998659 pubmed_paper_found_sample_similar_article.tsv
-- 998659
select count()
from sp.pubmed_randomly_selected_papers_found_similar_paper;

select count()
-- from fp.paper_clean_content;
from fp.paper_clean_content;


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

-- TODO dataset was crawled at 2010-05-15 to 2010-05-17
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


-- visual the titles of similar papers and examine the results
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
          from sp.pubmed_randomly_selected_papers_found_similar_paper
          order by rand()
          limit 1)) using pm_id
order by od
;