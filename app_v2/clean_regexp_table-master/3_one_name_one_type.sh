source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="
insert overwrite table profile${label}.dws_app_regexp_20210530_mid_3 partition(the_date, version_no, project_name)
select
a.version,
a.project,
a.app_name,
a.suspected_app_name,
a.posi_regexp,
a.nege_regexp,
a.mapping_app_name,
b.app_type,
a.behavior,
a.virtual_id,
a.the_date,
a.version_no,
a.project_name
from
profile${label}.dws_app_regexp_20210530_mid_2 a
left join
(select mapping_app_name, max(app_type) as app_type from profile${label}.dws_app_regexp_20210530_mid_2 group by mapping_app_name) b
on a.mapping_app_name = b.mapping_app_name;

"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
