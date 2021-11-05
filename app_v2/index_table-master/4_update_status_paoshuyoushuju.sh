source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2
app_result_table=$3

sql_part="

insert overwrite table profile${label}.dws_app_index_20211030_3
select 
app_id,
a.app_name,
app_name_after_download,
app_package,
package_name_after_download,
case when b.app_name is not null and status = '已入库' then '跑数有数据'
when b.app_name is not null and status = '已打标' then '跑数有数据' 
when b.app_name is not null and status = '已开发' then '跑数有数据' 
else status
end as status,
app_type,
type_code,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') as event_time
from profile.dws_app_index_20211030_2 a
left join 
(select app_name from ${app_result_table} group by app_name) b
on a.app_name = b.app_name;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区数据写入完成
