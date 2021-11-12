source /etc/profile
source  ~/.bash_profile

sql_part="

create table if not exists ga.interface_app_cnt_by_mobile_id_20210930
(
mobile_id string comment '手机号',
count string comment '一个手机号安装app种类的总数'
)
comment 'app标签总量表'
partitioned by (
mobile_id_end_no string comment '手机尾号'
)
row format delimited fields terminated by '\t' stored as orc;

create table if not exists ga.interface_app_cnt_by_strategy_20210930
(
mobile_id string comment '手机号',
count string comment '一个手机号某个策略类型的app种类数',
times string comment '一个手机号app行为出现的次数'
)
comment 'app标签表'
partitioned by (
app_strategy string comment 'app策略类型',
mobile_id_end_no string comment '手机尾号'
)
row format delimited fields terminated by '\t' stored as orc;

create table if not exists ga.interface_app_individual_percent_20210930
(
mobile_id string comment '手机号',
percent string comment '一个手机号某个策略类型的app的占比'
)
comment 'app标签表'
partitioned by (
app_strategy string comment 'app策略',
mobile_id_end_no string comment '手机尾号'
)
row format delimited fields terminated by '\t' stored as orc;

create table if not exists ga.interface_app_cnt_by_time_20210930
(
mobile_id string comment '手机号',
count string comment '距离现在最近的n个月安装的app数量'
)
comment 'app时间统计表'
partitioned by (
month string comment '距离现在最近的n个月',
mobile_id_end_no string comment '手机尾号'
)
row format delimited fields terminated by '\t' stored as orc;

create table if not exists ga.interface_percent_distribution_20210930
(
app_strategy string comment 'app策略',
percent string comment '手机上某策略类app安装百分比',
mobile_id_count string comment '手机号个数统计'
)
comment '阈值参考表'
row format delimited fields terminated by '\t' stored as orc;

create table if not exists ga.interface_count_distribution_20210930
(
app_strategy string comment 'app策略',
strategy_count string comment '手机上某策略类安装个数',
mobile_id_count string comment '手机号个数统计'
)
comment '阈值参考表'
row format delimited fields terminated by '\t' stored as orc;

"

cd /home/qianyu/ && bash beeline.sh -e "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 数据写入完成
