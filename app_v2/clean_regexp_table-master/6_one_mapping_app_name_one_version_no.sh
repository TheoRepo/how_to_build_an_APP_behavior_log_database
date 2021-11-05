source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2


sql_part="
insert overwrite table profile${label}.dws_app_regexp_20211030_mid_2
select 
a.app_name,
a.suspected_app_name,
a.posi_regexp,
a.nege_regexp,
a.mapping_app_name,
a.app_index,
a.app_type,
a.type_index,
a.behavior,
a.behavior_index,
a.virtual_id,
case when b.mapping_app_name is not null then '010'
else a.version_no
end as version_no,
a.the_date
from profile${label}.dws_app_regexp_20211030_mid_1 a
left join 
(select distinct(mapping_app_name) from profile${label}.dws_app_duplicate_statistic) b
on a.mapping_app_name = b.mapping_app_name;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
