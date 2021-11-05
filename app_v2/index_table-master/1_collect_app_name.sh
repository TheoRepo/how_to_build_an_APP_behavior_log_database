source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2
regexp_table=$3
strategy_table=$4

sql_part="
insert overwrite table profile${label}.dws_app_index_mid0
select 
app_name,
row_number() over(partition by 1 order by app_name desc) AS app_id
from 
(
select trim(mapping_app_name) as app_name from ${regexp_table} group by mapping_app_name
union all
select trim(app_name) as app_name from ${strategy_table} group by app_name
)a
where app_name != '+' and app_name !='-' and app_name != '-钓-' and app_name != '..' and app_name != '0' and app_name != '/68' and app_name != '. .'
group by app_name
order by app_name
;


insert overwrite table profile${label}.dws_app_index_mid1
select
a.app_name,
a.app_id,
case
when b.app_name is not null then b.app_package
end as app_package,
case
when b.app_name is not null then '重点类'
else Null
end as app_type
from 
profile${label}.dws_app_index_mid0 a
left join 
(select trim(app_name) as app_name, concat_ws(', ',collect_set(app_package)) as app_package from ${strategy_table} group by app_name) b
on a.app_name = b.app_name;


insert overwrite table profile${label}.dws_app_index_mid2
select 
a.app_name,
a.app_id,
a.app_package,
case
when b.app_name is not null then b.app_type
else a.app_type
end as app_type
from profile${label}.dws_app_index_mid1 a
left join
(select trim(mapping_app_name) as app_name, app_type from ${regexp_table} group by mapping_app_name,app_type) b
on a.app_name = b.app_name

"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区数据写入完成
