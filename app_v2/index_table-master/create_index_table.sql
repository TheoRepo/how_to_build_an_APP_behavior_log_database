# 以下代码在dataworks跑通
# 以下代码做的一件事情，从策略和正则表中提取的所有app的名称和包名，补全数据写入index表标注已入库，建立初始化的索引表，然后索引表会和正则表，APP结果表分别碰撞，打上已开发，跑数有数据，跑数且无数据的标签

drop table if exists profile_test.dws_app_index_mid0;
create table if not exists profile_test.dws_app_index_mid0
(
app_name string comment 'app名称',
app_apk string comment 'app包名'
)
comment 'app索引表第零个中间表'
lifecycle 365
row format delimited fields terminated by '\t' stored as orc;

drop table if exists profile_test.dws_app_index_mid1;
create table if not exists profile_test.dws_app_index_mid1
(
app_name string comment 'app名称',
app_apk string comment 'app包名'
)
comment 'app索引表第一个中间表'
lifecycle 365
row format delimited fields terminated by '\t' stored as orc;

drop table if exists profile_test.dws_app_index_mid2;
create table if not exists profile_test.dws_app_index_mid2
(
app_name string comment 'app名称',
app_type string comment 'app类型'
)
comment 'app索引表第二个中间表'
lifecycle 365
row format delimited fields terminated by '\t' stored as orc;

insert overwrite table profile_test.dws_app_index_mid2
select app_name, Null as app_type from profile.dws_app_all_20210530 where app_type regexp '' group by app_name;

drop table if exists profile_test.dws_app_index_mid3;
create table if not exists profile_test.dws_app_index_mid3
(
app_name string comment 'app名称',
app_type string comment 'app类型'
)
comment 'app索引表第三个中间表'
lifecycle 365
row format delimited fields terminated by '\t' stored as orc;

insert overwrite table profile_test.dws_app_index_mid3
select t1.app_name,t1.app_type from 
(select app_name, Null as app_type from profile.dws_app_regexp_20211030 where the_date regexp '' group by app_name) t1
left outer join
(select app_name, Null as app_type from profile_test.dws_app_index_mid2) t2
on t1.app_name = t2.app_name where t2.app_name is null;





drop table if exists profile.dws_app_index_20211030;
create table if not exists profile.dws_app_index_20211030
(
app_id string comment 'app唯一识别标识',
app_name string comment 'app名称',
app_name_after_download string comment '下载后的app实际名称',
app_apk string comment 'app包名',
package_name_after_download string comment '下载后的app实际包名',
status string comment 'app开发状态',
app_type string comment 'app类型',
type_code string comment 'app类型编码',
event_time string comment 'app数据导入时间'
)
comment 'app索引表'
lifecycle 365
row format delimited fields terminated by '\t' stored as orc;


insert overwrite table profile_test.dws_app_index_mid0
select app_name,app_apk from 
(
select app_name, concat_ws(', ',collect_set(app_apk)) as app_apk from ga.interface_app_strategy_20211030 where app_strategy regexp '' group by app_name
union all
select app_name, Null as app_pkg from (select app_name from profile.dws_app_regexp_20211030 where the_date regexp '' group by app_name) a
)tmp;


insert overwrite table profile_test.dws_app_index_mid1
select app_name, concat_ws(', ',collect_set(app_apk)) as app_apk from profile_test.dws_app_index_mid0 group by app_name;









insert overwrite table profile.dws_app_index_20211030
select 
row_number() over(partition by 1 order by app_name desc) AS app_id, 
app_name, 
Null as app_name_after_download, 
app_apk, 
Null as package_name_after_download, 
'已入库' as status, 
'重点类' as app_type, 
Null as type_code,
from_unixtime(unix_timestamp(),'yyyy-MM-dd') as event_time
from profile_test.dws_app_index_mid1;





drop table if exists profile.dws_app_index_20211030_back;
create table if not exists profile.dws_app_index_20211030_back
(
app_id string comment 'app唯一识别标识',
app_name string comment 'app名称',
app_name_after_download string comment '下载后的app实际名称',
app_apk string comment 'app包名',
package_name_after_download string comment '下载后的app实际包名',
status string comment 'app开发状态',
app_type string comment 'app类型',
type_code string comment 'app类型编码',
event_time string comment 'app数据导入时间'
)
comment 'app索引表'
lifecycle 365
row format delimited fields terminated by '\t' stored as orc;





insert overwrite table profile.dws_app_index_20211030_back
select * from profile.dws_app_index_20211030;

insert overwrite table profile.dws_app_index_20211030
select 
app_id,
a.app_name,
app_name_after_download,
app_apk,
package_name_after_download,
case
when b.app_name is not null then '已开发'
else status
end as status,
case 
when b.app_type is not null then b.app_type
else a.app_type
end as app_type,
type_code,
event_time
from profile.dws_app_index_20211030_back a
left join 
(select app_name, app_type from 
(select * , row_number() over (partition by app_name order by app_type desc) as rank from
(select app_name, app_type from profile.dws_app_regexp_20211030 where the_date regexp '' group by app_name, app_type) A) B where rank = 1) b
on a.app_name = b.app_name;








insert overwrite table profile.dws_app_index_20211030_back
select * from profile.dws_app_index_20211030;

insert overwrite table profile.dws_app_index_20211030
select 
app_id, 
app_name, 
app_name_after_download, 
app_apk, 
package_name_after_download, 
status, 
case 
when app_type = '社交类' then '社交类'
when app_type = '新闻类' then '新闻类'
when app_type = '购物类' then '购物类'
when app_type = '娱乐类' then '娱乐类'
when app_type = '财务类' then '财务类'
when app_type = '商务类' then '商务类'
when app_type = '生活类' then '生活类'
when app_type = '其他类' then '其他类'
else '重点类'
end as app_type, 
type_code, 
event_time
from profile.dws_app_index_20211030_back;




insert overwrite table profile.dws_app_index_20211030_back
select * from profile.dws_app_index_20211030;

insert overwrite table profile.dws_app_index_20211030
select 
app_id,
a.app_name,
app_name_after_download,
app_apk,
package_name_after_download,
case
when B.app_name <> '' and B.app_name is not null then '跑数且无数据'
else status
end as status,
app_type,
type_code,
event_time
from profile.dws_app_index_20211030_back A
left join 
(select app_name from profile_test.dws_app_index_mid3 group by app_name) B
on A.app_name = B.app_name;









insert overwrite table profile.dws_app_index_20211030_back
select * from profile.dws_app_index_20211030;

insert overwrite table profile.dws_app_index_20211030
select 
app_id,
a.app_name,
app_name_after_download,
app_apk,
package_name_after_download,
case
when b.app_name is not null then '跑数有数据'
else status
end as status,
a.app_type,
case 
when app_type = '社交类' then '1'
when app_type = '新闻类' then '2'
when app_type = '购物类' then '3'
when app_type = '娱乐类' then '4'
when app_type = '财务类' then '5'
when app_type = '商务类' then '6'
when app_type = '生活类' then '7'
when app_type = '其他类' then '8'
when app_type = '重点类' then '9'
else 'unknow_app_type'
end as type_code,
event_time
from profile.dws_app_index_20211030_back a
left join 
(select app_name from profile_test.dws_app_index_mid2 group by app_name) b
on a.app_name = b.app_name;


compress table profile.dws_app_index_20211030 options(type='snappy');
