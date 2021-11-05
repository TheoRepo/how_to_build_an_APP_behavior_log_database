source /etc/profile
source  ~/.bash_profile
version_no=$1
spark_sql_path=$2
label=$3

sql_part="

set hive.exec.dynamic.partition.mode = nonstrict;
INSERT overwrite TABLE profile${label}.app_20211030 partition(version_no,app_type,mobile_id_end_no)
select 
a.mobile_id, 
a.app_name,
a.behavior,
a.start_time,
a.last_time,
a.times,
a.version_no,
a.app_type, 
a.mobile_id_end_no
    from
    (
    select
    mobile_id, 
    app_name,
    case when behavior<>'' then behavior
    else null
    end as behavior,
    min(start_time) over(partition by app_name,mobile_id,app_type,behavior) as start_time,
    max(last_time) over(partition by app_name,mobile_id,app_type,behavior) as last_time,
    sum(times) over(partition by app_name,mobile_id,app_type,behavior) as times,
    version_no,
    app_type,
    case when mobile_id='0' then '00'
    when mobile_id='1' then '01'
    when mobile_id='2' then '02'
    when mobile_id='3' then '03'
    when mobile_id='4' then '04'
    when mobile_id='5' then '05'
    when mobile_id='6' then '06'
    when mobile_id='7' then '07'
    when mobile_id='8' then '08'
    when mobile_id='9' then '09'
    else substring(mobile_id,-2,2) 
    end as mobile_id_end_no
        from
        profile${label}.app_fdm_20211030 where version_no='${version_no}' and app_name!='14667'
    )a 
    group by app_name,mobile_id,app_type,behavior,start_time,last_time,times,version_no,mobile_id_end_no
distribute by rand();

"
bash /home/${spark_sql_path}/spark_sql.sh "zs_app" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【APP行为全量汇总】跑数失败了55555"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

msg2="定时调度【APP行为全量汇总】跑数成功~！！！"
curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg2}'", "mobiles":"'${notice_ids}'" }' "http://10.10.15.52:8888/conch/notice/ding"


exit 0
