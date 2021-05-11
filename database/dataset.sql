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

