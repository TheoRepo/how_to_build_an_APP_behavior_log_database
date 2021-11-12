source /etc/profile
source  ~/.bash_profile

the_date=$1
notice_ids="18801036793,13100669434"

sql_part="
insert overwrite table ga.interface_count_rlt_20210820 partition(the_date='${the_date}',tablename='手机号app使用总量表')
select '手机号最近1个月app使用总量表_单人数据量最大值' as label, max(count) cnt from ga.interface_app_cnt_by_time_20210930 where month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量最小值' as label, min(count) cnt from ga.interface_app_cnt_by_time_20210930 where month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量中位数' as label, percentile_approx(count,0.5) cnt from ga.interface_app_cnt_by_time_20210930 where month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量为1' as label, count(count) cnt from ga.interface_app_cnt_by_time_20210930 where count = 1 and month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量为2~10' as label, count(count) cnt from ga.interface_app_cnt_by_time_20210930 where count > 1 and count <= 10 and month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量为11~100' as label, count(count) cnt from ga.interface_app_cnt_by_time_20210930 where count > 10 and count <= 100 and month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量为101~1000' as label, count(count) cnt from ga.interface_app_cnt_by_time_20210930 where count > 100 and count <= 1000 and month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量为1000~10000' as label, count(count) cnt from ga.interface_app_cnt_by_time_20210930 where count > 1000 and count <= 10000 and month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量为10001~100000' as label, count(count) cnt from ga.interface_app_cnt_by_time_20210930 where count > 10000 and count <= 100000 and month = '1'
union all
select '手机号最近1个月app使用总量表_单人数据量大于100000' as label, count(count) cnt from ga.interface_app_cnt_by_time_20210930 where count > 100000 and month = '1'
union all
select '手机号最近1个月app使用总量表_覆盖人数' as label, count(1) from ga.interface_app_cnt_by_time_20210930 where month = '1';
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
