# 这份代码做的事情：


# 0_create_table.sh qianyu ''
## 建表 dws_app_regexp_20211030_mid_1到8, dws_app_regexp_20210530_mid_1到3，dws_app_duplicate_statistic，dws_app_regexp_20211030
# 1_update_regexp_table_from_0530.sh qianyu ''
## 从profile.dws_app_regexp_20210530同步了所有规则，写入到了profile.dws_app_regexp_20211030，将version_no=008的开发时间改成2021-06-10
# 2_nine_app_type.sh qianyu ''
## 将1030表中的app_type规范成了九大类，社交，新闻，购物，娱乐，财务，商务，生活，其他，重点
# 3_one_name_one_type.sh qianyu ''
## 将mapping_app_name和app_type调整成了一对一的关系
# 4_change_version_no.sh qianyu ''
## 将版本号规范化成了001，002，003，004，005，006，007，008，009九个版本，对应了9批开发的规则
# 5_duplicate_mapping_app_name_on_different_version.sh qianyu ''
## 找出9个版本种重复开发的mapping_app_name
# 6_one_mapping_app_name_one_version_no.sh qianyu ''
## 将重复开发的mapping_app_name对应的规则统统划分到version_no='010'
# 8_unique_date.sh qianyu ''
## 算法助理将version_no='010'中所有重复的规则逐条审核去重，我将version_no=008的开发时间改成2021-06-10
# 9_return_008_and_009.sh qianyu ''
## 无用功，懒得删了
# 10_create_version_10_and_11.sh qianyu ''
## 从dws_app_regexp_20211030_mid_2去除重复规则的表中，同步001,002,003,004,005,006,007版本的规则，从dws_app_regexp_20211030_mid_1原始的规则中，同步008，009，因为这两个版本是云漠用来提取virtual_id的，比较特殊，不能动
## 从dws_app_regexp_20211030_mid_5去除重复规则的表中，同步010，011，010是用app_name连接短文本的表，011是用suspected_app_name连接短文本的表
# 11_equally_distributed.sh qianyu ''
## 为了让app行为结果表中的数据量分布均衡，将001，002，003，011所有的规则统一写入001，004，005，006，007，010的规则均匀划分后，分配到002，003，004，005，006，007，008和009的规则不动
# 12_update_index.sh qianyu ''
## 规则表，app_name,behavior,app_type三个字段索引化
# 13_unify_null.sh qianyu ''
## 将正则表中所有的''空字符,'null'字符，统一转化成Null,hive不能识别的数据形式
