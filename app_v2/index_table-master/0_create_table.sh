source /etc/profile
source  ~/.bash_profile

beeline_path=$1
label=$2

sql_part="
drop table if exists profile${label}.dws_app_index_mid0;
create table if not exists profile${label}.dws_app_index_mid0
(
app_name string comment 'app名称',
app_id string comment 'app_id'
)
comment 'app索引表第零个中间表'
row format delimited fields terminated by '\t' stored as orc;

drop table if exists profile${label}.dws_app_index_mid1;
create table if not exists profile${label}.dws_app_index_mid1
(
app_name string comment 'app名称',
app_id string comment 'app类型',
app_package string comment 'app包名',
app_type string comment 'app类型'
)
comment 'app索引表第一个中间表'
row format delimited fields terminated by '\t' stored as orc;

drop table if exists profile${label}.dws_app_index_mid2;
create table if not exists profile${label}.dws_app_index_mid2
(
app_name string comment 'app名称',
app_id string comment 'app类型',
app_package string comment 'app包名',
app_type string comment 'app类型'
)
comment 'app索引表第二个中间表'
row format delimited fields terminated by '\t' stored as orc;



drop table if exists profile${label}.dws_app_index_20211030;
create table if not exists profile${label}.dws_app_index_20211030
(
app_id string comment 'app唯一识别标识',
app_name string comment 'app名称',
app_name_after_download string comment '下载后的app实际名称',
app_package string comment 'app包名',
package_name_after_download string comment '下载后的app实际包名',
status string comment 'app开发状态',
app_type string comment 'app类型',
type_code string comment 'app类型编码',
event_time string comment 'app数据导入时间'
)
comment 'app索引表'
row format delimited fields terminated by '\t' stored as orc;


drop table if exists profile${label}.dws_app_index_20211030_1;
create table if not exists profile${label}.dws_app_index_20211030_1
(
app_id string comment 'app唯一识别标识',
app_name string comment 'app名称',
app_name_after_download string comment '下载后的app实际名称',
app_package string comment 'app包名',
package_name_after_download string comment '下载后的app实际包名',
status string comment 'app开发状态',
app_type string comment 'app类型',
type_code string comment 'app类型编码',
event_time string comment 'app数据导入时间'
)
comment 'app索引表'
row format delimited fields terminated by '\t' stored as orc;

drop table if exists profile${label}.dws_app_index_20211030_2;
create table if not exists profile${label}.dws_app_index_20211030_2
(
app_id string comment 'app唯一识别标识',
app_name string comment 'app名称',
app_name_after_download string comment '下载后的app实际名称',
app_package string comment 'app包名',
package_name_after_download string comment '下载后的app实际包名',
status string comment 'app开发状态',
app_type string comment 'app类型',
type_code string comment 'app类型编码',
event_time string comment 'app数据导入时间'
)
comment 'app索引表'
row format delimited fields terminated by '\t' stored as orc;

drop table if exists profile${label}.dws_app_index_20211030_3;
create table if not exists profile${label}.dws_app_index_20211030_3
(
app_id string comment 'app唯一识别标识',
app_name string comment 'app名称',
app_name_after_download string comment '下载后的app实际名称',
app_package string comment 'app包名',
package_name_after_download string comment '下载后的app实际包名',
status string comment 'app开发状态',
app_type string comment 'app类型',
type_code string comment 'app类型编码',
event_time string comment 'app数据导入时间'
)
comment 'app索引表'
row format delimited fields terminated by '\t' stored as orc;

drop table if exists profile${label}.dws_app_index_20211030_4;
create table if not exists profile${label}.dws_app_index_20211030_4
(
app_id string comment 'app唯一识别标识',
app_name string comment 'app名称',
app_name_after_download string comment '下载后的app实际名称',
app_package string comment 'app包名',
package_name_after_download string comment '下载后的app实际包名',
status string comment 'app开发状态',
app_type string comment 'app类型',
type_code string comment 'app类型编码',
event_time string comment 'app数据导入时间'
)
comment 'app索引表'
row format delimited fields terminated by '\t' stored as orc;
"

cd /home/${beeline_path}/ && bash beeline.sh -e "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 数据写入完成
