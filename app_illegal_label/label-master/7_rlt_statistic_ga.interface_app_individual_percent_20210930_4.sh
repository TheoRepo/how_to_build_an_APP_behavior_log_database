source /etc/profile
source  ~/.bash_profile

the_date=$1
notice_ids="18801036793,13100669434"

sql_part="
insert overwrite table ga.interface_count_rlt_20210820 partition(the_date='${the_date}',tablename='手机号app百分比表')
select '网盘类app_覆盖人数' as label, count(mobile_id) from ga.interface_app_individual_percent_20210930 where percent = '100%' and app_strategy ='投资理财'
union all
select '境外通联类app_覆盖人数' as label, count(mobile_id) from ga.interface_app_individual_percent_20210930 where percent = '100%' and app_strategy ='虚拟币'
union all
select '敌对媒体类app_覆盖人数' as label, count(mobile_id) from ga.interface_app_individual_percent_20210930 where percent = '100%' and app_strategy ='港澳台'
union all
select '邪教类app_覆盖人数' as label, count(mobile_id) from ga.interface_app_individual_percent_20210930 where percent = '100%' and app_strategy ='网络传销'
union all
select '借贷类app_覆盖人数' as label, count(mobile_id) from ga.interface_app_individual_percent_20210930 where percent = '100%' and app_strategy ='海外'
union all
select '涉维类app_覆盖人数' as label, count(mobile_id) from ga.interface_app_individual_percent_20210930 where percent = '100%' and app_strategy ='小众通联';
"

cd /home/qianyu/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【app使用情况数据统计】脚本7_rlt_statistic_ga.interface_app_cnt_by_time_20210930.sh跑数失败"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "2f82e5451cfe7886232e4355ace307c58f1e5f157054e929eff1d0697ff6a94d","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

msg2="定时调度【app使用情况数据统计】7_rlt_statistic_ga.interface_app_cnt_by_time_20210930.sh脚本跑数成功！！"
curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "2f82e5451cfe7886232e4355ace307c58f1e5f157054e929eff1d0697ff6a94d","content": "'${msg2}'", "mobiles":"'${notice_ids}'" }' "http://10.10.15.52:8888/conch/notice/ding"



exit 0
