source /etc/profile
source  ~/.bash_profile

notice_ids="18801036793,13100669434"

sql_part="

insert overwrite table ga.interface_app_individual_percent_20210930 partition(app_strategy,mobile_id_end_no)
select 
a.mobile_id,
concat(cast(cast(cast(a.count AS BIGINT)/cast(b.count AS BIGINT)* 100 as decimal(10,0)) as string), '%') as percent,
a.app_strategy,
a.mobile_id_end_no
from
ga.interface_app_cnt_by_strategy_20210930 a left join ga.interface_app_cnt_by_mobile_id_20210930 b
on a.mobile_id = b.mobile_id
distribute by rand();

"

cd /home/qianyu/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【手机号不同策略app使用百分比表(ga.interface_app_individual_percent_20210930)】脚本4_percent.sh跑数失败"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "2f82e5451cfe7886232e4355ace307c58f1e5f157054e929eff1d0697ff6a94d","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

exit 0
