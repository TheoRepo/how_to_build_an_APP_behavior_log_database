source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="
insert overwrite table profile${label}.dws_app_index_20211030_1
select 
app_id, 
app_name, 
Null as app_name_after_download, 
app_package, 
Null as package_name_after_download, 
'已入库' as status, 
app_type, 
Null as type_code,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') as event_time
from profile${label}.dws_app_index_mid2;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区数据写入完成
