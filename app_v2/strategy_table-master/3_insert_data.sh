source /etc/profile
source  ~/.bash_profile
spark_sql_path=$1

sql_part="

insert overwrite table ga.interface_app_strategy_20211030 partition(app_strategy,event_time)
select app_name, app_package, app_strategy, event_time from ga.interface_app_strategy_20211030_temp;


"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"


if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
