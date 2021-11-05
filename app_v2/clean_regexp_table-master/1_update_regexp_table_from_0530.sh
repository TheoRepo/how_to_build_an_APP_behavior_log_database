source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="
insert overwrite table profile${label}.dws_app_regexp_20210530_mid_1 partition(the_date, version_no, project_name)
select
version,
project,
app_name,
suspected_app_name,
posi_regexp,
nege_regexp,
mapping_app_name,
app_type,
behavior,
virtual_id,
case 
when project_name = 'ah' and version_no ='001' then '2021-05-25'
when project_name = 'sj' and version_no ='001' then '2021-05-20'
when project_name = 'ys' and version_no ='001' then '2021-07-09'
when project_name = 'zs' and version_no ='001' then '2021-06-08'
when project_name = 'zs' and version_no ='002' then '2021-05-12'
when project_name = 'zs' and version_no ='003' then '2021-07-19'
when project_name = 'zs' and version_no ='004' then '2021-06-16'
when project_name = 'zs' and version_no ='005' then '2021-06-10'
when project_name = 'zs' and version_no ='006' then '2021-09-30'
end as the_date,
version_no, 
project_name
from profile${label}.dws_app_regexp_20210530;

"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
