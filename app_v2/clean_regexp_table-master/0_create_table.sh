source /etc/profile
source  ~/.bash_profile

beeline_path=$1
label=$2

sql_part="
drop table if exists profile${label}.dws_app_regexp_20211030;
CREATE TABLE if not exists profile${label}.dws_app_regexp_20211030 
(
app_name STRING COMMENT 'app_name',
suspected_app_name STRING COMMENT '疑似appname',
posi_regexp STRING COMMENT '正向正则',
nege_regexp STRING COMMENT '反向正则',
mapping_app_name STRING COMMENT '映射app_name',
app_index STRING COMMENT 'app唯一识别索引',
app_type STRING COMMENT 'app类别',
type_index STRING COMMENT 'app类别索引',
behavior STRING COMMENT '行为',
behavior_index STRING COMMENT '行为索引',
virtual_id STRING COMMENT '虚拟id'
) COMMENT 'app_v2版本更新后的过滤器'
PARTITIONED BY 
(
version_no STRING COMMENT '版本号',
the_date STRING
)
row format delimited fields terminated by '\t'
stored as orc;


drop table if exists profile${label}.dws_app_regexp_20211030_mid_1;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_1 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20211030_mid_2;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_2 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20211030_mid_4;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_4 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20211030_mid_5;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_5 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20211030_mid_6;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_6 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20211030_mid_7;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_7 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20211030_mid_8;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_8 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20211030_mid_9;
create table if not exists profile${label}.dws_app_regexp_20211030_mid_9 like profile${label}.dws_app_regexp_20211030;

drop table if exists profile${label}.dws_app_regexp_20210530_mid_1;
create table if not exists profile${label}.dws_app_regexp_20210530_mid_1 like profile.dws_app_regexp_20210530;

drop table if exists profile${label}.dws_app_regexp_20210530_mid_2;
create table if not exists profile${label}.dws_app_regexp_20210530_mid_2 like profile.dws_app_regexp_20210530;

drop table if exists profile${label}.dws_app_regexp_20210530_mid_3;
create table if not exists profile${label}.dws_app_regexp_20210530_mid_3 like profile.dws_app_regexp_20210530;

drop table if exists profile${label}.dws_app_duplicate_statistic;
create table if not exists profile${label}.dws_app_duplicate_statistic
(
mapping_app_name string comment 'app名称',
version_no string comment '版本号',
rnk string comment '重复次数'
)
comment 'app规则重复数据统计表'
row format delimited fields terminated by '\t'
stored as orc;
"

cd /home/${beeline_path}/ && bash beeline.sh -e "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 数据写入完成
