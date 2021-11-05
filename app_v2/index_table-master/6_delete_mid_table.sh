source /etc/profile
source  ~/.bash_profile

spark_sql_path=$1
label=$2

sql_part="
drop table if exists profile${label}.dws_app_index_mid0;
drop table if exists profile${label}.dws_app_index_mid1;
drop table if exists profile${label}.dws_app_index_mid2;
drop table if exists profile${label}.dws_app_index_20211030_1;
drop table if exists profile${label}.dws_app_index_20211030_2;
drop table if exists profile${label}.dws_app_index_20211030_3;
drop table if exists profile${label}.dws_app_index_20211030_4;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app_index" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区数据写入完成
