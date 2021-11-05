# 这段代码在dataworks运行

drop table if exists profile.dws_app_regexp_20211030_mid_3;
CREATE TABLE if not exists profile.dws_app_regexp_20211030_mid_3
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
virtual_id STRING COMMENT '虚拟id',
version_no STRING COMMENT '版本号',
the_date STRING
)
lifecycle 365;


load data '/user/datawork/users/qianyu/qianyu_upload/APP库冗余规则优化_3.csv' table tdl_app_list_all options(delimiter=',',header='true');
insert overwrite table profile.dws_app_regexp_20211030_mid_3 select * from tdl_app_list_all;

compress table profile.dws_app_regexp_20211030_mid_3 options(type='snappy')