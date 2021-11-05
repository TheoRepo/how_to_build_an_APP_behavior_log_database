source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2


sql_part="
drop table if exists profile${label}.dws_app_regexp_20211030_mid_1;
drop table if exists profile${label}.dws_app_regexp_20211030_mid_2;
drop table if exists profile${label}.dws_app_regexp_20211030_mid_4;
drop table if exists profile${label}.dws_app_regexp_20211030_mid_5;
drop table if exists profile${label}.dws_app_regexp_20211030_mid_6;
drop table if exists profile${label}.dws_app_regexp_20211030_mid_7;
drop table if exists profile${label}.dws_app_regexp_20211030_mid_8;
drop table if exists profile${label}.dws_app_regexp_20210530_mid_1;
drop table if exists profile${label}.dws_app_regexp_20210530_mid_2;
drop table if exists profile${label}.dws_app_regexp_20210530_mid_3;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
