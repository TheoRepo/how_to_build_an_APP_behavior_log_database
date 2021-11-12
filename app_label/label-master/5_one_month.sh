source /etc/profile
source  ~/.bash_profile

notice_ids="18801036793,13100669434"

sql_part="

insert overwrite table ga.interface_app_cnt_by_time_20210930 partition(month,mobile_id_end_no)
select 
mobile_id,
count(app_name),
"1" as month,
mobile_id_end_no
from 
(
select mobile_id,mobile_id_end_no,app_name from profile.dws_app_all_20210530 where unix_timestamp(start_time,'yyyy-MM-dd HH:mm:ss') >  unix_timestamp(add_months(from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'), -1), 'yyyy-MM-dd' )
group by mobile_id,app_name,mobile_id_end_no
) a
group by mobile_id,mobile_id_end_no
distribute by rand();

"

cd /home/qianyu/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【手机号过去1个月的app使用总量表(ga.interface_app_cnt_by_time_20210930)】脚本5_one_month.sh跑数失败"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "2f82e5451cfe7886232e4355ace307c58f1e5f157054e929eff1d0697ff6a94d","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

exit 0
