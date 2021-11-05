source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="

insert overwrite table profile${label}.dws_app_index_20211030
select 
app_id,
app_name,
app_name_after_download,
app_package,
package_name_after_download,
case when status = '已开发' then '跑数且无数据'
else status
end as status,
app_type,
case 
when app_type = '社交类' then '1'
when app_type = '新闻类' then '2'
when app_type = '购物类' then '3'
when app_type = '娱乐类' then '4'
when app_type = '财务类' then '5'
when app_type = '商务类' then '6'
when app_type = '生活类' then '7'
when app_type = '其他类' then '8'
when app_type = '重点类' then '9'
end as type_code,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') as event_time
from profile${label}.dws_app_index_20211030_3

"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区数据写入完成
