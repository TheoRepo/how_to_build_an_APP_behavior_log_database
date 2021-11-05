source /etc/profile
source  ~/.bash_profile
version_no=$1
spark_sql_path=$2
label=$3
source_table=$4
regexp_table=$5
notice_ids="18801036793"



sql_part="
insert overwrite table profile${label}.dwd_app_20211030 partition(the_date,version_no)
select 
row_key,
mobile_id,
app_name,
app_type,
case when length(behavior) > 0 then behavior end as behavior,
start_time,
case when length(virtual_id) > 0 then virtual_id end as virtual_id,
the_date,version_no
from
(
    select 
    row_key,
    mobile_id,
    app_name,
    app_type,
    max(behavior) behavior,
    start_time,
    max(virtual_id) virtual_id,
    the_date,version_no
    from
    (
        select /*+ mapjoin(b)*/
        a.row_key,
        a.mobile_id,
        b.app_index as app_name,
        b.type_index as app_type,
        b.behavior_index as behavior,
        a.event_time as start_time,
        case when b.virtual_id regexp '已开通花呗' then regexp_extract(a.msg,b.virtual_id,2)
             when b.virtual_id<>'' then regexp_extract(a.msg,b.virtual_id,1)
        else null
        end as virtual_id,
        the_date,
        version_no
        from
            (
                select
                row_key,
                mobile_id,
                event_time,
                app_name,
                suspected_app_name,
                msg,
                the_date
                from ${source_table} where the_date >= date_sub(current_date(),15)
            )a
        inner join
            (
                select
                app_name,
                suspected_app_name,
                posi_regexp,
                nege_regexp,
                mapping_app_name,
                behavior,
                virtual_id,
                app_type,
                version_no,
                app_index,
                type_index,
                behavior_index
                from ${regexp_table} 
                where the_date='2021-06-10' and version_no='${version_no}'
            )b on trim(a.app_name) = trim(b.app_name)
        where if(b.posi_regexp<>'',a.msg regexp b.posi_regexp,1=1) 
        and if(b.nege_regexp<>'',a.msg not regexp b.nege_regexp,1=1)
    ) z
    group by row_key,mobile_id,app_name,app_type,start_time,the_date,version_no
) d
distribute by rand()
"
bash /home/${spark_sql_path}/spark_sql.sh "appv2" "$sql_part"

if [[ $? != 0 ]];then
    msg="定时调度【${version_no}版本规则对最近15天的短文本的APP行为信息抽取】1_extract_0610_update.sh脚本跑数失败了55555"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

msg2="定时调度【${version_no}版本规则对最近15天的短文本的APP行为信息抽取】跑数成功~！！！"
curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg2}'", "mobiles":"'${notice_ids}'" }' "http://10.10.15.52:8888/conch/notice/ding"


exit 0
