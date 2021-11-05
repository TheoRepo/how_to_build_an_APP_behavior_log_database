source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="
insert overwrite table profile${label}.dws_app_duplicate_statistic
select mapping_app_name,version_no,rnk from 
(
select 
mapping_app_name, 
version_no, 
count(mapping_app_name) over(partition by mapping_app_name order by mapping_app_name) as rnk
from 
(
select mapping_app_name, '001' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '001' group by mapping_app_name) a
union all
select mapping_app_name, '002' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '002' group by mapping_app_name) a
union all
select mapping_app_name, '003' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '003' group by mapping_app_name) a
union all
select mapping_app_name, '004' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '004' group by mapping_app_name) a
union all
select mapping_app_name, '005' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '005' group by mapping_app_name) a
union all
select mapping_app_name, '006' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '006' group by mapping_app_name) a
union all
select mapping_app_name, '007' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '007' group by mapping_app_name) a
union all
select mapping_app_name, '008' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '008' group by mapping_app_name) a
union all
select mapping_app_name, '009' as version_no from (select mapping_app_name from profile${label}.dws_app_regexp_20211030_mid_1 where version_no = '009' group by mapping_app_name) a
) b
) c
where rnk > 1
order by mapping_app_name

"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
