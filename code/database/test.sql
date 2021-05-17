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

-- test json extraction
desc table (
with '{"pm_id": "19555", "record": {"LinkSetDb": [{"Link": [{"Id": "6893201", "Score": "24289376"}, {"Id": "2579240", "Score": "22437389"}, {"Id": "4421919", "Score": "21049830"}, {"Id": "4805006", "Score": "20790831"}, {"Id": "4531025", "Score": "20572771"}, {"Id": "5133445", "Score": "20425695"}, {"Id": "6315856", "Score": "19278171"}, {"Id": "270701", "Score": "19139750"}, {"Id": "10618163", "Score": "19076326"}, {"Id": "6643577", "Score": "18918443"}, {"Id": "2556494", "Score": "18773850"}, {"Id": "6480", "Score": "18711975"}, {"Id": "7226252", "Score": "18591522"}, {"Id": "807722", "Score": "18121715"}, {"Id": "6840399", "Score": "17791403"}, {"Id": "1158971", "Score": "17787424"}, {"Id": "2579386", "Score": "17635087"}, {"Id": "22096590", "Score": "17474066"}, {"Id": "1935991", "Score": "17400470"}, {"Id": "6090605", "Score": "17375461"}, {"Id": "284407", "Score": "17343795"}, {"Id": "13587543", "Score": "17332846"}, {"Id": "6928670", "Score": "17304784"}, {"Id": "1484285", "Score": "17296722"}, {"Id": "6893200", "Score": "17159595"}, {"Id": "1336540", "Score": "17138158"}, {"Id": "1141376", "Score": "17136962"}, {"Id": "2418170", "Score": "17117608"}, {"Id": "3404208", "Score": "17028443"}, {"Id": "7410485", "Score": "16939156"}, {"Id": "9129842", "Score": "16850529"}, {"Id": "10587471", "Score": "16844076"}, {"Id": "753896", "Score": "16836917"}, {"Id": "6090604", "Score": "16751329"}, {"Id": "5862500", "Score": "16739925"}, {"Id": "1713947", "Score": "16692065"}, {"Id": "8661484", "Score": "16585670"}, {"Id": "1111109", "Score": "16489429"}, {"Id": "8824731", "Score": "16455554"}, {"Id": "8410694", "Score": "16397039"}, {"Id": "3544307", "Score": "16197934"}, {"Id": "16661000", "Score": "16183956"}, {"Id": "2537386", "Score": "16090840"}, {"Id": "16592630", "Score": "16048099"}, {"Id": "239004", "Score": "15950350"}, {"Id": "7249093", "Score": "15932484"}, {"Id": "7249092", "Score": "15091088"}, {"Id": "13848774", "Score": "14399393"}, {"Id": "2335563", "Score": "13903714"}, {"Id": "3792142", "Score": "12637477"}, {"Id": "5578693", "Score": "12072746"}, {"Id": "592878", "Score": "11351718"}, {"Id": "13708576", "Score": "10616304"}, {"Id": "20272033", "Score": "6004911"}], "DbTo": "pubmed", "LinkName": "pubmed_pubmed"}, {"Link": [{"Id": "31837520", "Score": "0"}, {"Id": "28305906", "Score": "0"}, {"Id": "26598726", "Score": "0"}, {"Id": "26012633", "Score": "0"}, {"Id": "25548923", "Score": "0"}, {"Id": "24761354", "Score": "0"}, {"Id": "24306585", "Score": "0"}, {"Id": "16592630", "Score": "0"}, {"Id": "7459998", "Score": "0"}, {"Id": "7410485", "Score": "0"}, {"Id": "7228899", "Score": "0"}, {"Id": "6893201", "Score": "0"}, {"Id": "6893200", "Score": "0"}, {"Id": "6582473", "Score": "0"}, {"Id": "6404931", "Score": "0"}, {"Id": "6256753", "Score": "0"}, {"Id": "3958044", "Score": "0"}, {"Id": "3178568", "Score": "0"}, {"Id": "270701", "Score": "0"}, {"Id": "42649", "Score": "0"}], "DbTo": "pubmed", "LinkName": "pubmed_pubmed_citedin"}, {"Link": [{"Id": "6893201", "Score": "24289376"}, {"Id": "2579240", "Score": "22437389"}, {"Id": "4421919", "Score": "21050359"}, {"Id": "10587471", "Score": "16844076"}, {"Id": "3544307", "Score": "16197934"}], "DbTo": "pubmed", "LinkName": "pubmed_pubmed_combined"}, {"Link": [{"Id": "6893201", "Score": "24289376"}, {"Id": "2579240", "Score": "22437389"}, {"Id": "4421919", "Score": "21050359"}, {"Id": "4805006", "Score": "20791517"}, {"Id": "4531025", "Score": "20572771"}], "DbTo": "pubmed", "LinkName": "pubmed_pubmed_five"}, {"Link": [{"Id": "14187282", "Score": "0"}, {"Id": "14184449", "Score": "0"}, {"Id": "5915609", "Score": "0"}, {"Id": "5862500", "Score": "0"}, {"Id": "5044754", "Score": "0"}, {"Id": "4805006", "Score": "0"}, {"Id": "4531270", "Score": "0"}, {"Id": "4421919", "Score": "0"}, {"Id": "4351590", "Score": "0"}, {"Id": "4224575", "Score": "0"}, {"Id": "1254647", "Score": "0"}, {"Id": "326151", "Score": "0"}, {"Id": "239004", "Score": "0"}], "DbTo": "pubmed", "LinkName": "pubmed_pubmed_refs"}, {"Link": [{"Id": "10587471", "Score": "16844076"}, {"Id": "3544307", "Score": "16197934"}], "DbTo": "pubmed", "LinkName": "pubmed_pubmed_reviews"}, {"Link": [{"Id": "10587471", "Score": "16844076"}, {"Id": "3544307", "Score": "16197934"}], "DbTo": "pubmed", "LinkName": "pubmed_pubmed_reviews_five"}], "ERROR": [], "LinkSetDbHistory": [], "DbFrom": "pubmed", "IdList": ["19555"]}}' as line
select JSONExtractString(line, 'pm_id')            as pm_id,
       arrayMap(y1->
                    [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')],
                JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed',
                                                JSONExtractArrayRaw(JSONExtractRaw(line, 'record') as record,
                                                                    'LinkSetDb'))[1], 'Link')
           )                                           as pubmed_pubmed,

       arrayMap(y1->
                    [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')],
                JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_citedin',
                                                JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link')
           )                                           as pubmed_pubmed_citedin,

       arrayMap(y1->
                    [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')],
                JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_combined',
                                                JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link')
           )                                           as pubmed_pubmed_combined,

       arrayMap(y1->
                    [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')],
                JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_five',
                                                JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link')
           )                                           as pubmed_pubmed_five,

       arrayMap(y1->
                    [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')],
                JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_refs',
                                                JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link')
           )                                           as pubmed_pubmed_refs,
       arrayMap(y1->
                    [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')],
                JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_reviews',
                                                JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link')
           )                                           as pubmed_pubmed_reviews,
       arrayMap(y1->
                    [JSONExtractString(y1, 'Id'), JSONExtractString(y1, 'Score')],
                JSONExtractArrayRaw(arrayFilter(x1->JSONExtractString(x1, 'LinkName') == 'pubmed_pubmed_reviews_five',
                                                JSONExtractArrayRaw(record, 'LinkSetDb'))[1], 'Link')
           )                                           as pubmed_pubmed_reviews_five,
       JSONExtractArrayRaw(record, 'ERROR')            as error,
       JSONExtractArrayRaw(record, 'LinkSetDbHistory') as linkset_db_history,
       JSONExtractRaw(record, 'DbFrom')                as db_from,
       JSONExtractArrayRaw(record, 'IdList')           as id_list)
;