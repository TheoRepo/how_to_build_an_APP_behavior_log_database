source /etc/profile
source  ~/.bash_profile
spark_sql_path=$1

sql_part="

insert overwrite table ga.interface_app_strategy_20211030 partition(app_strategy, event_time) 
select 
mapping_app_name as app_name, 
null as app_package, 
app_type as app_strategy ,
'2021-10-30' as event_time
from profile.dws_app_regexp_20210530
where app_type not regexp '社交类|新闻类|购物类|娱乐类|财务类|商务类|生活类|其他类|未找到';
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"


if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
