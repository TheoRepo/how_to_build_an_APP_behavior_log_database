# 第一步
# 激活配置文件
source /etc/profile
source ~/.bash_profile

# 传入变量
the_date=$1
spark_sql_path=$2
label=$3

# sql代码
sql_part="

Insert overwrite TABLE profile${label}.dwd_app_zs_20210530 partition(the_date='${the_date}',version_no='004')


# 让所有的空数据变成None，在behavior 和 virtual_id字段中
# case when then (else None) end as ，在spark SQL当中，else None可以忽略不写

select
row_key,
mobile_id,
mapping_app_name,
app_type,
case when length(behavior) > 0 then behavior end as behavior,
event_time,
case when length(virtual_id) > 0 then virtual_id end as virtual_id
from
(

# 计算机编码中，中文的对应的编码数值大于字符的对应的编码数值，使用max()函数，对behavior 和 virtual_id字段有且仅保留中文内容
select
row_key,
mobile_id,
mapping_app_name,
app_type,
max(behavior) behavior,
event_time,
max(virtual_id) virtual_id
from
(

# 您需要在select语句中使用Hint提示/*+ mapjoin(<table_name>) */才会执行mapjoin
# 当您对一个大表和一个或多个小表执行join操作时，可以在select语句中显式指定mapjoin Hint提示以提升查询性能
# virtual_id这个字段的内容是正则表达式，msg这个字段表示短信内容
# when b.virtual_id<>'' then regexp_extract(a.msg,b.virtual_id,1)
# 当b.virtual_id不为空，使用regexp_extract函数在，按照b.virtual_id的正则表达式，抽取我们想要的内容。

select
/*+mapjoin(b)*/
a.row_key,
a.mobile_id,
b.mapping_app_name,
b.app_type,
b.behavior,
a.event_time,
case
when b.virtual_id<>'' then regexp_extract(a.msg,b.virtual_id,1)
else null
end as virtual_id
from

# date_sub(current_date(),15) )距离当前日期的前15天
(select
row_key,
mobile_id,
event_time,
app_name,
suspected_app_name,
msg
from preprocess.ds_txt_final
where the_date >= date_sub(current_date(),15) )a
inner join
(

# 短信表preprocess.ds_txt_final 和 正则表profile.dws_app_regexp_20210530 inner join on a.app_name = b.app_name
# 分布式存储中一条数据可能会有多一个 file_no = merge_20210906_17995_L2，merge_20210906_17995_L2，distribute by rand() 这一步是为了将HDFS上存储的文件压缩,同一条数据只保留一个file_no
# 分区字段file_no的由来？？？？
# 仅仅提取针对浙数的2021-06-16这一天的，APP数据编写的正则规则，where the_date='2021-06-16' and version_no='004' and project_name='zs'

select
app_name,
app_type,
suspected_app_name,
posi_regexp,
nege_regexp,
mapping_app_name,
behavior,
virtual_id
from profile.dws_app_regexp_20210530 where the_date='2021-06-16' and version_no='004' and project_name='zs'
)b on a.app_name = b.app_name
where if(b.suspected_app_name<>'',a.suspected_app_name = b.suspected_app_name,1=1) and if(b.posi_regexp<>'',a.msg regexp b.posi_regexp,1=1) and if(b.nege_regexp<>'',a.msg not regexp b.nege_regexp,1=1)
) a
group by row_key,mobile_id,mapping_app_name,app_type,event_time
)d
distribute by rand();

"



echo $sql_part
cd /home/${spark_sql_path} && bash spark_sql.sh "zs_app" "$sql_part"

if [[ $? != 0 ]];then
echo "sql 运行失败！！！！！！"
exit 1
fi
echo 分区 '${pt}'数据写入完成