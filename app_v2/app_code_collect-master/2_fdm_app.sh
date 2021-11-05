source /etc/profile
source  ~/.bash_profile

the_date=$1
version_no=$2
spark_sql_path=$3
label=$4
the_date_1=${the_date:0:7}


sql_part="
INSERT overwrite TABLE profile${label}.app_fdm_20211030 partition(the_date,version_no)
select 
a.mobile_id, 
a.app_name,
a.app_type, 
a.behavior,
a.start_time,
a.last_time,
a.times,
a.the_date,
a.version_no
from
    (
    select
    mobile_id, 
    app_name,
    app_type, 
    behavior,
    min(start_time) over(partition by app_name,mobile_id,app_type,behavior) as start_time,
    max(start_time) over(partition by app_name,mobile_id,app_type,behavior) as last_time,
    count(1) over(partition by app_name,mobile_id,app_type,behavior) as times,
    substring(the_date,0,7) as the_date,
    version_no
        from
        profile${label}.dwd_app_20211030 where version_no='${version_no}' and the_date rlike '${the_date_1}'  and app_name!='14667'
    )a 
group by app_name,mobile_id,app_type,behavior,start_time,last_time,times,the_date,version_no
distribute by rand();
"
bash /home/${spark_sql_path}/spark_sql.sh "zs_app" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【${version_no}版本规则对${the_date_1}的APP行为按月汇总】跑数失败了55555"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

msg2="定时调度【${version_no}版本规则对${the_date_1}的APP行为按月汇总】跑数成功~！！！"
curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg2}'", "mobiles":"'${notice_ids}'" }' "http://10.10.15.52:8888/conch/notice/ding"



exit 0
