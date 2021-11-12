# 第二步
# 数据按月汇聚
# 同一条数据会在多个时间分区the_date中反复出现，为了对50亿的数据进行去重，首先，按月去重。
# 解决办法是使用开窗函数，基本语法如下
# <窗口函数> over (partition by <用于分组的列名> order by <用于排序的列名>)
# 窗口函数，也叫OLAP函数（Online Anallytical Processing，联机分析处理），可以对数据库数据进行实时分析处理。
# 窗口函数具备了我们之前学过的group by子句分组的功能和order by子句排序的功能。那么，为什么还要用窗口函数呢？
# 这是因为，group by分组汇总后改变了表的行数，一行只有一个类别。而partiition by和rank函数不会减少原表中的行数。例如下面统计每个班级的人数。
# https://zhuanlan.zhihu.com/p/92654574


source /etc/profile
source ~/.bash_profile

the_date=$1
spark_sql_path=$2
label=$3
the_date_1=${the_date:0:7}
the_date_2=$(date -d last-month +%Y-%m)


sql_part="
INSERT overwrite TABLE profile${label}.app_zs_fdm_20210530 partition(the_date='${the_date_1}',version_no='004')

#
select
a.mobile_id,
a.app_name,
a.app_type,
case when length(a.behavior) > 0 then a.behavior end as behavior,
a.start_time,
a.last_time,
a.times from
(

# 第一步的结果表dwd_app_zs_20210530
select
mobile_id,
app_name,
app_type,
behavior,
min(start_time) over(partition by app_name,mobile_id,app_type,behavior) as start_time,
max(start_time) over(partition by app_name,mobile_id,app_type,behavior) as last_time,
count(1) over(partition by app_name,mobile_id,app_type,behavior) as times
from
profile${label}.dwd_app_zs_20210530 where version_no='004' and (the_date rlike '${the_date_1}' or the_date rlike '${the_date_2}') and app_type!='未找到'
)a group by app_name,mobile_id,app_type,behavior,start_time,last_time,times
distribute by rand();

"
bash /home/${spark_sql_path}/spark_sql.sh "zs_app" "$sql_part"

if [[ $? != 0 ]];then
echo "sql error 自动退出"
exit 1
fi

echo "运行成功"

exit 0

