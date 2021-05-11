-- [{"descriptorName":"cell nucleus","descriptorNameUI":"d002467","majorTopicYN":false,"qualifierNameList":[]},{"descriptorName":"cytogenetics","descriptorNameUI":"d003582","majorTopicYN":false,"qualifierNameList":[]},{"descriptorName":"diagnosis, differential","descriptorNameUI":"d003937","majorTopicYN":false,"qualifierNameList":[]},{"descriptorName":"endometrium","descriptorNameUI":"d004717","majorTopicYN":false,"qualifierNameList":[{"descriptorName":"pathology","descriptorNameUI":"q000473","majorTopicYN":true}]},{"descriptorName":"female","descriptorNameUI":"d005260","majorTopicYN":false,"qualifierNameList":[]},{"descriptorName":"humans","descriptorNameUI":"d006801","majorTopicYN":false,"qualifierNameList":[]},{"descriptorName":"prognosis","descriptorNameUI":"d011379","majorTopicYN":false,"qualifierNameList":[]},{"descriptorName":"sex chromatin","descriptorNameUI":"d012728","majorTopicYN":false,"qualifierNameList":[{"descriptorName":"analysis","descriptorNameUI":"q000032","majorTopicYN":true}]},{"descriptorName":"staining and labeling","descriptorNameUI":"d013194","majorTopicYN":false,"qualifierNameList":[]},{"descriptorName":"uterine neoplasms","descriptorNameUI":"d014594","majorTopicYN":false,"qualifierNameList":[{"descriptorName":"pathology","descriptorNameUI":"q000473","majorTopicYN":true},{"descriptorName":"radiotherapy","descriptorNameUI":"q000532","majorTopicYN":false}]}]
select mesh_headings,
       arrayMap(x->
                    [JSONExtractString(x, 'descriptorName'),
                        JSONExtractString(x, 'descriptorNameUI')
                        ],
                JSONExtractArrayRaw(mesh_headings))
from pubmed.nft_paper
where length(mesh_headings) > 20
limit 100;;
