-- calculate sequence length to decide mas_seq_len parameter
select length(splitByChar(' ', clean_title)) + length(splitByChar(' ', clean_abstract)) as seq_len, count() as cnt
from fp.paper_clean_content
where rand() % 100 < 2
group by seq_len
order by seq_len
;
