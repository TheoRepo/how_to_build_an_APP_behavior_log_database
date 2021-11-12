source /etc/profile
source  ~/.bash_profile

the_date=$1
notice_ids="18801036793,13100669434"

sql_part="

insert overwrite table ga.interface_count_rlt_20210820 partition(the_date='${the_date}',tablename='手机号不同策略app使用个数统计表')
select CONCAT(app_strategy, '类app使用个数统计表_覆盖人数') as label, count(1) from ga.interface_app_cnt_by_strategy_20210930 group by app_strategy;
"

cd /home/qianyu/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【app使用情况数据统计】脚本7_rlt_statistic_interface_app_cnt_by_strategy_20210930_3.sh跑数失败"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "2f82e5451cfe7886232e4355ace307c58f1e5f157054e929eff1d0697ff6a94d","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

msg2="定时调度【app使用情况数据统计】7_rlt_statistic_interface_app_cnt_by_strategy_20210930_3.sh脚本跑数成功！！"
curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "2f82e5451cfe7886232e4355ace307c58f1e5f157054e929eff1d0697ff6a94d","content": "'${msg2}'", "mobiles":"'${notice_ids}'" }' "http://10.10.15.52:8888/conch/notice/ding"



exit 0
