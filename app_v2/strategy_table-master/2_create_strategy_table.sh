source /etc/profile
source  ~/.bash_profile
sql_path=$1

sql_part="

drop table if exists ga.interface_app_strategy_20211030;
create table if not exists ga.interface_app_strategy_20211030 
(
app_name string comment 'app名称',
app_package string comment '已知安装包名'
)
comment 'app初始化策略表'
partitioned by (
app_strategy string comment 'app分类策略',
event_time string comment '数据入库时间'
)
row format delimited fields terminated by '\t' stored as orc;
"

bash /home/${sql_path}/beeline.sh -e "$sql_part"


if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成



