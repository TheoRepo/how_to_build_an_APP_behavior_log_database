source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2


sql_part="
insert overwrite table profile${label}.dws_app_regexp_20211030_mid_7
select
a.app_name,
a.suspected_app_name,
a.posi_regexp,
a.nege_regexp,
a.mapping_app_name,
b.app_id as app_index,
a.app_type,
b.type_code as type_index,
a.behavior,
case 
when behavior = '修改密码' then '010000'
when behavior = '找回密码' then '020000'
when behavior = '交易' then '030000'
when behavior = '登录' then '040000'
when behavior = '注册' then '050000'
when behavior = '账号异常' then '060000'
when behavior = '异地登录' then '070000'
when behavior = '手机绑定' then '080000'
when behavior = '触达' then '090000'
when behavior = '验证' then '100000'
when behavior = '更换手机号' then '110000'
end as behavior_index,
a.virtual_id,
a.version_no,
a.the_date
from
profile${label}.dws_app_regexp_20211030_mid_6 a
left join 
profile.dws_app_index_20211030 b
on trim(a.mapping_app_name) = trim(b.app_name);

"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
