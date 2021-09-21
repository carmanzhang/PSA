import sys

sys.path.append("..")
import numpy as np
from matplotlib import pyplot as plot

plot.rcParams['font.family'] = 'serif'
plot.rcParams['font.serif'] = ['Times New Roman'] + plot.rcParams['font.serif']

from myio.data_reader import DBReader

colors = ['green', 'red', 'gold', 'black', 'cyan', 'blue', 'magenta', 'purple', 'gray', 'fuchsia', 'orange', 'yellow']
linestyles = ['--', '-.', '--', '--']
line_markers = ['<', '>', '^', 'v']
linewidth = 3

df = DBReader.tcp_model_cached_read("xxxx",
                                    '''select id, name, global_cnt, ds_cnt
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
                inner join (select arrayJoin(arrayDistinct(
               arrayFlatten(groupArray(arrayConcat(relevant, partial, irrelevant, [pm_id]))))) as pm_id
                            from sp.eval_data_relish_v1) using pm_id
       group by id
       order by ds_cnt desc) any
       inner join sp.biomedical_paper_JD_ST using id) using id;''',
                                    cached=False)

print('df_whole.shape', df.shape)
columns = df.columns.values
print(len(columns), columns)

values = df[['global_cnt', 'ds_cnt', 'id', 'name']].values
values = np.array(values)

global_cnt = values[:, 0] / np.sum(values[:, 0])
ds_cnt = values[:, 1] / np.sum(values[:, 1])
# names = values[:, 2]
names = values[:, 3]

# Note outliers
outliers = [[names[i], ds_cnt[i]] for i in range(len(global_cnt)) if abs(global_cnt[i] - ds_cnt[i]) > 0.015]

idx = 0
plot.plot(names, global_cnt, linestyle=linestyles[idx],
          # marker=line_markers[idx], markersize=8, markevery=0.2,
          color=colors[idx], label='Whole PubMed', linewidth=linewidth)
idx = 1
plot.plot(names, ds_cnt, linestyle=linestyles[idx],
          # marker=line_markers[idx], markersize=8, markevery=0.2,
          color=colors[idx], label='ReliSH Dataset', linewidth=linewidth)

# Note add text to the outliers
for o_name, o_value in outliers:
    plot.text(o_name, o_value, o_name, fontsize=12)

# plot.yscale('log')
# plot.title('journal descriptor distribution', fontsize=18)
plot.xlabel('journal descriptor', fontsize=12)
plot.ylabel('proportion (%)', fontsize=12)
plot.legend(loc='best')

# Note set invisible to x axis
plot.xticks([])

plot.tight_layout()

plot.savefig(
    '/home/zhangli/mydisk-2t/repo/manuscripts/ongoning-works/similar-article-recommendation-evaluation/src/figures/jd-dist.png',
    dpi=600)

plot.show()
