source /etc/profile
source  ~/.bash_profile

sql_part="
select '1' scene
"

cd /home/qianyu/ && bash spark_sql.sh "app" "$sql_part"



if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 数据写入完成
