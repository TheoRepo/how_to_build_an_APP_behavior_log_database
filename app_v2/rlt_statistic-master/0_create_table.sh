source /etc/profile
source  ~/.bash_profile

beeline_path=$1
label=$2

sql_part="

drop table if exists profile${label}.dwd_app_rlt_statistic_20211030;
create table if not exists profile${label}.dwd_app_rlt_statistic_20211030
(
label string comment '统计标签名称',
cnt string comment '统计结果',
version_no string comment '规则版本号'
) comment 'APP明细表'
partitioned by 
(
tablename string comment '统计的结果表名称',
the_date string comment '统计日期'
)
row format delimited fields terminated by '\t'
stored as orc;

"

cd /home/${beeline_path}/ && bash beeline.sh -e "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 数据写入完成
