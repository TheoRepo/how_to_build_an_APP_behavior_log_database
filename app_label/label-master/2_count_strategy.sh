source /etc/profile
source  ~/.bash_profile

notice_ids="18801036793,13100669434"

sql_part="


insert overwrite table ga.interface_app_cnt_by_strategy_20210930 partition(app_strategy,mobile_id_end_no)
select 
mobile_id,
count(1) as cnt,
max(times) as times,
app_strategy,
mobile_id_end_no
from
(
select
/*+mapjoin(b)*/
mobile_id,
b.app_strategy,
row_number() over(partition by mobile_id,a.app_name order by mobile_id) rn,
sum(a.times) over(partition by mobile_id,app_strategy order by mobile_id) as times,
mobile_id_end_no
from profile.dws_app_all_20210530 a 
left join 
(select app_name, app_strategy from ga.interface_app_strategy_20211030 group by app_name, app_strategy) b
on a.app_name = b.app_name where b.app_name is not null
) c
where rn = 1
group by mobile_id,app_strategy,mobile_id_end_no
distribute by rand();


"

cd /home/qianyu/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【手机号不同策略app使用总量表(ga.interface_app_cnt_by_strategy_20210930)】脚本2_count_strategy.sh跑数失败"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "2f82e5451cfe7886232e4355ace307c58f1e5f157054e929eff1d0697ff6a94d","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

exit 0

