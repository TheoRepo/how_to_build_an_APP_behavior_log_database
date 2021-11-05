source /etc/profile
source  ~/.bash_profile

input_table=$1
the_date=$2
spark_sql_path=$3
label=$4

sql_part="

insert overwrite table profile${label}.dwd_app_rlt_statistic_20211030 partition(tablename='${input_table}',the_date='${the_date}')
select label, cnt, version_no from
(
select '数据量' as label, count(*) as cnt, '001' as version_no from ${input_table} where version_no='001' and the_date = '${the_date}'
union all
select '数据量' as label, count(*) as cnt, '002' as version_no from ${input_table} where version_no='002' and the_date = '${the_date}'
union all
select '数据量' as label, count(*) as cnt, '003' as version_no from ${input_table} where version_no='003' and the_date = '${the_date}'
union all
select '数据量' as label, count(*) as cnt, '004' as version_no from ${input_table} where version_no='004' and the_date = '${the_date}'
union all
select '数据量' as label, count(*) as cnt, '005' as version_no from ${input_table} where version_no='005' and the_date = '${the_date}'
union all
select '数据量' as label, count(*) as cnt, '006' as version_no from ${input_table} where version_no='006' and the_date = '${the_date}'
union all
select '数据量' as label, count(*) as cnt, '007' as version_no from ${input_table} where version_no='007' and the_date = '${the_date}' 
union all
select '数据量' as label, count(*) as cnt, '008' as version_no from ${input_table} where version_no='008' and the_date = '${the_date}' 
union all
select '数据量' as label, count(*) as cnt, '009' as version_no from ${input_table} where version_no='009' and the_date = '${the_date}' 
union all
select '数据量' as label, count(*) as cnt, '010' as version_no from ${input_table} where version_no='010' and the_date = '${the_date}' 
union all
select '数据量' as label, count(*) as cnt, '011' as version_no from ${input_table} where version_no='011' and the_date = '${the_date}' 
union all
select '数据量' as label, count(*) as cnt, '012' as version_no from ${input_table} where version_no='012' and the_date = '${the_date}' 
union all
select '覆盖量' as label, count(*) as cnt, '001' as version_no from (select mobile_id from ${input_table} where version_no='001' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '002' as version_no from (select mobile_id from ${input_table} where version_no='002' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '003' as version_no from (select mobile_id from ${input_table} where version_no='003' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '004' as version_no from (select mobile_id from ${input_table} where version_no='004' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '005' as version_no from (select mobile_id from ${input_table} where version_no='005' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '006' as version_no from (select mobile_id from ${input_table} where version_no='006' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '007' as version_no from (select mobile_id from ${input_table} where version_no='007' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '008' as version_no from (select mobile_id from ${input_table} where version_no='008' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '009' as version_no from (select mobile_id from ${input_table} where version_no='009' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '010' as version_no from (select mobile_id from ${input_table} where version_no='010' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '011' as version_no from (select mobile_id from ${input_table} where version_no='011' and the_date = '${the_date}' group by mobile_id)a
union all
select '覆盖量' as label, count(*) as cnt, '012' as version_no from (select mobile_id from ${input_table} where version_no='012' and the_date = '${the_date}' group by mobile_id)a
union all
select 'app种类个数' as label, count(*) as cnt, '001' as version_no from (select app_name from ${input_table} where version_no='001' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '002' as version_no from (select app_name from ${input_table} where version_no='002' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '003' as version_no from (select app_name from ${input_table} where version_no='003' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '004' as version_no from (select app_name from ${input_table} where version_no='004' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '005' as version_no from (select app_name from ${input_table} where version_no='005' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '006' as version_no from (select app_name from ${input_table} where version_no='006' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '007' as version_no from (select app_name from ${input_table} where version_no='007' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '008' as version_no from (select app_name from ${input_table} where version_no='008' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '009' as version_no from (select app_name from ${input_table} where version_no='009' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '010' as version_no from (select app_name from ${input_table} where version_no='010' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '011' as version_no from (select app_name from ${input_table} where version_no='011' and the_date = '${the_date}' group by app_name)a
union all
select 'app种类个数' as label, count(*) as cnt, '012' as version_no from (select app_name from ${input_table} where version_no='012' and the_date = '${the_date}' group by app_name)a

) t1
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_stat" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区数据写入完成
