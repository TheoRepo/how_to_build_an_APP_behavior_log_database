source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2




sql_part="
insert overwrite table profile${label}.dws_app_regexp_20211030_mid_1 partition(version_no, the_date)
select
app_name, 
suspected_app_name, 
posi_regexp, 
nege_regexp,
mapping_app_name,
concat(null) as app_index,
app_type,
concat(null) as type_index,
behavior,
concat(null) as behavior_index,
virtual_id,
case 
when version_no='001' and project = 'ah' then '001'
when version_no='001' and project = 'sj' then '002'
when version_no='001' and project = 'ys' then '003'
when version_no='001' and project = 'zs' then '004'
when version_no='002' and project = 'zs' then '005'
when version_no='003' and project = 'zs' then '006'
when version_no='004' and project = 'zs' then '007'
when version_no='005' and project = 'zs' then '008'
when version_no='006' and project = 'zs' then '009'
end as version_no,
the_date
from profile${label}.dws_app_regexp_20210530_mid_3;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
