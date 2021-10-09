-- Note calculate sequence length to decide max_seq_len parameter
--  0.6549141086351199
with (select count() from fp.paper_clean_content) as all_articles
select count() / all_articles as with_title_and_abstract_precentage
from fp.paper_clean_content
where length(clean_abstract) > 10
  and length(clean_title) > 0
;

-- Note the average length of title+abstract if abstract field is available
-- 205.35461144561137
select avg(length(splitByChar(' ', clean_title)) + length(splitByChar(' ', clean_abstract))) as avg_seq_len
from fp.paper_clean_content
where length(clean_abstract) > 10
  and length(clean_title) > 0
;
-- select clean_abstract
-- from fp.paper_clean_content
-- where length(clean_abstract) > 30
-- order by length(clean_abstract) limit 5000;

-- 205.35461144561137
-- 0.34393748882095837
with (select count() from fp.paper_clean_content) as all_articles
select count() / all_articles as sql_len_over_200_percentage
from fp.paper_clean_content
where length(clean_abstract) > 10
  and length(clean_title) > 0
  and length(splitByChar(' ', clean_title)) + length(splitByChar(' ', clean_abstract)) > 200
;

-- Note this data is to infer the research area from PubMed
select pm_id, content
from (select toString(pm_id)                          as pm_id,
             concat(clean_title, ' ', clean_abstract) as content
      from fp.paper_clean_content) any
         inner join(
    select distinct (pm_id) as pm_id
    from (
          select pm_id
          from sp.eval_data_relish_v1
          union all
          select pm_id
          from sp.eval_data_trec_genomic_2005)) using pm_id
into outfile 'query_pm_id_content.tsv' FORMAT TSV;

with '' as line
select (splitByChar('|', line) as tmp_arr)[2] as id, tmp_arr[3] as name, 'JD' as source;

-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/JD-ST-list/jds.txt | clickhouse-local --input_format_allow_errors_ratio=1 --input-format=TSV --table='input' --structure="line String"  --query="select \
-- (splitByChar('|', line) as tmp_arr)[2] as id, tmp_arr[3] as name, 'JD' as source \
-- from input" --format=Native | clickhouse-client --query='INSERT INTO sp.biomedical_paper_JD_ST FORMAT Native' --port=9001 --password=root
-- cat /home/zhangli/mydisk-2t/repo/pubmed-paper-clustering/code/data/JD-ST-list/sts.txt | clickhouse-local --input_format_allow_errors_ratio=1 --input-format=TSV --table='input' --structure="line String"  --query="select \
-- (splitByChar('|', line) as tmp_arr)[2] as id, tmp_arr[4] as name, 'ST' as source \
-- from input" --format=Native | clickhouse-client --query='INSERT INTO sp.biomedical_paper_JD_ST FORMAT Native' --port=9001 --password=root
-- drop table sp.biomedical_paper_JD_ST;

create table if not exists sp.biomedical_paper_JD_ST
(
    id     String,
    name   String,
    source String
) ENGINE = MergeTree order by length(id);

--  123 jds.txt
--  133 sts.txt
-- JD	123
-- ST	133
select source, count()
from sp.biomedical_paper_JD_ST
group by source;

-- 123	132
select count(distinct splitByChar('|', JD_ids)[1]),
       count(distinct splitByChar('|', ST_ids)[1])
from and.pubmed_paper_level_profile_JD_ST;

-- drop table sp.eval_data_relish_v1_related_JD_ST;
create table if not exists sp.eval_data_relish_v1_related_JD_ST ENGINE = Log as
with (select groupArray((id, name)) from sp.biomedical_paper_JD_ST) as JD_ST_list
select pm_id,
       arrayMap(x-> arrayFilter(y->y.1 = x, JD_ST_list)[1], splitByChar('|', JD_ids)) as JDs,
       arrayMap(x-> arrayFilter(y->y.1 = x, JD_ST_list)[1], splitByChar('|', ST_ids)) as STs
from and.pubmed_paper_level_profile_JD_ST
         -- Note associate ReLiSH dataset
         any
         inner join (
    select arrayJoin(arrayDistinct(
            arrayFlatten(groupArray(arrayConcat(relevant, partial, irrelevant, [pm_id]))))) as pm_id
    from sp.eval_data_relish_v1
    ) using pm_id;


-- Note analyze the journal descriptor distribution of the who PubMed and the dataset
select id, name, global_cnt, ds_cnt
from (
         select id, name, global_cnt
         from (
                  select splitByChar('|', JD_ids)[1] as id, count() as global_cnt
                  from and.pubmed_paper_level_profile_JD_ST
                  group by id
                  order by global_cnt desc) any
                  inner join sp.biomedical_paper_JD_ST using id) any
         left join (
    select id, name, ds_cnt
    from (
             select arrayJoin(splitByChar('|', JD_ids)) as id, count() as ds_cnt
             from and.pubmed_paper_level_profile_JD_ST
                      -- Note associate ReLiSH dataset
                      any
                      inner join (
                 select arrayJoin(arrayDistinct(
                         arrayFlatten(groupArray(arrayConcat(relevant, partial, irrelevant, [pm_id]))))) as pm_id
                 from sp.eval_data_relish_v1
                 --                  select distinct (pm_id) as pm_id
--                  from sp.eval_data_trec_genomic_2005
                 ) using pm_id
             group by id
             order by ds_cnt desc) any
             inner join sp.biomedical_paper_JD_ST using id) using id;
;

-- Note analyze  distribution of the who PubMed and the dataset
select id, name, global_cnt, ds_cnt
from (
         select id, name, global_cnt
         from (
                  select splitByChar('|', JD_ids)[1] as id, count() as global_cnt
                  from and.pubmed_paper_level_profile_JD_ST
                  group by id
                  order by global_cnt desc) any
                  inner join sp.biomedical_paper_JD_ST using id) any
         left join (
    select id, name, ds_cnt
    from (
             select splitByChar('|', JD_ids)[1] as id, count() as ds_cnt
             from and.pubmed_paper_level_profile_JD_ST
                      -- Note associate ReLiSH dataset
                      any
                      inner join (
                 select arrayJoin(arrayDistinct(
                         arrayFlatten(groupArray(arrayConcat(relevant, partial, irrelevant, [pm_id]))))) as pm_id
                 from sp.eval_data_relish_v1
                 --                  select distinct (pm_id) as pm_id
--                  from sp.eval_data_trec_genomic_2005
                 ) using pm_id
             group by id
             order by ds_cnt desc) any
             inner join sp.biomedical_paper_JD_ST using id) using id;
;

-- 163289
select count(arrayJoin(arrayPushBack(arrayConcat(relevant, partial, irrelevant), pm_id))) as num_involved_article
from sp.eval_data_relish_v1;