source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2
regexp_table=$3

sql_part="


insert overwrite table profile${label}.dws_app_index_20211030_2
select 
app_id,
a.app_name,
app_name_after_download,
app_package,
package_name_after_download,
case
when b.app_name is not null and status = '已入库' then '已开发'
else status
end as status,
app_type,
type_code,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') as event_time
from profile${label}.dws_app_index_20211030_1 a
left join 
(select mapping_app_name as app_name from ${regexp_table} group by mapping_app_name) b
on a.app_name = b.app_name;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区数据写入完成
