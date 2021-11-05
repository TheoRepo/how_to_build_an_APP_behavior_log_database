source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2


sql_part="
insert overwrite table profile${label}.dws_app_regexp_20211030_mid_5 partition(version_no, the_date)
select
app_name,
suspected_app_name,
posi_regexp,
nege_regexp,
mapping_app_name,
app_index,
app_type,
type_index,
behavior,
behavior_index,
virtual_id,
case 
when the_date = '2021-06-10' then '008'
when the_date = '2021-09-30' then '009'
else version_no
end as version_no,
the_date
from profile${label}.dws_app_regexp_20211030_mid_4
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
