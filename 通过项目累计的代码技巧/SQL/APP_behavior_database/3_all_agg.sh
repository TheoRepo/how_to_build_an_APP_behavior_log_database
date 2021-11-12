# 第三步
#数据按年汇聚

source /etc/profile
source ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="
set hive.exec.dynamic.partition.mode = nonstrict;
INSERT overwrite TABLE profile${label}.app_zs_20210530 partition(version_no='004',app_type,mobile_id_end_no)

#
select
a.mobile_id,
a.app_name,
case when length(a.behavior) > 0 then a.behavior end as behavior,
a.start_time,
a.last_time,
a.times,
a.app_type,
a.mobile_id_end_no
from
(

#
select
mobile_id,
app_name,
behavior,
min(start_time) over(partition by app_name,mobile_id,app_type,behavior) as start_time,
max(last_time) over(partition by app_name,mobile_id,app_type,behavior) as last_time,
sum(times) over(partition by app_name,mobile_id,app_type,behavior) as times,
version_no,
app_type,
substr(mobile_id,-1,1) as mobile_id_end_no
from
profile${label}.app_zs_fdm_20210530 where version_no='004'
)a group by app_name,mobile_id,app_type,behavior,start_time,last_time,times,version_no,mobile_id_end_no
distribute by rand();

"
bash /home/${spark_sql_path}/spark_sql.sh "zs_app" "$sql_part"

if [[ $? != 0 ]];then
echo "sql error 自动退出"
exit 1
fi

echo "运行成功"

exit 0

