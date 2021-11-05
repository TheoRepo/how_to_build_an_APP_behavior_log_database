source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="
insert overwrite table profile${label}.dws_app_regexp_20210530_mid_2 partition(the_date, version_no, project_name)
select 
version,
project,
app_name,
suspected_app_name,
posi_regexp,
nege_regexp,
mapping_app_name,
case 
when app_type regexp '社交类|新闻类|购物类|娱乐类|财务类|商务类|生活类|其他类' then app_type
else '重点类'
end as app_type, 
behavior,
virtual_id,
the_date,
version_no,
project_name
from profile${label}.dws_app_regexp_20210530_mid_1;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
