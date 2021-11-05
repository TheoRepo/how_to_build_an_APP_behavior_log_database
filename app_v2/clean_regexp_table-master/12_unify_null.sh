source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2


sql_part="

insert overwrite table profile${label}.dws_app_regexp_20211030 partition(version_no, the_date)
select 
case when app_name = '' or lower(app_name) = 'null' then Null
else app_name
end as app_name,
case when suspected_app_name = '' or lower(suspected_app_name) = 'null' then Null
else suspected_app_name
end as suspected_app_name,
case when posi_regexp = '' or lower(posi_regexp) = 'null' then Null
else posi_regexp
end as posi_regexp,
case when nege_regexp = '' or lower(nege_regexp) = 'null' then Null
else nege_regexp
end as nege_regexp,
case when mapping_app_name = '' or lower(mapping_app_name) = 'null' then Null
else mapping_app_name
end as mapping_app_name,
case when app_index = '' or lower(app_index) = 'null' then Null
else app_index
end as app_index,
case when app_type = '' or lower(app_type) = 'null' then Null
else app_type
end as app_type,
case when type_index = '' or lower(type_index) = 'null' then Null
else type_index
end as type_index,
case when behavior = '' or lower(behavior) = 'null' then Null
else behavior
end as behavior,
case when behavior_index = '' or lower(behavior_index) = 'null' then Null
else behavior_index
end as behavior_index,
case when virtual_id = '' or lower(virtual_id) = 'null' then Null
else virtual_id
end as virtual_id,
case when version_no = '' or lower(version_no) = 'null' then Null
else version_no
end as version_no,
case when the_date = '' or lower(the_date) = 'null' then Null
else the_date
end as the_date
from profile${label}.dws_app_regexp_20211030_mid_7;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成
