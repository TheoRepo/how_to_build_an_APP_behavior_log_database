source /etc/profile
source  ~/.bash_profile
the_date=$1
version_no=$2
spark_sql_path=$3
label=$4
source_table=$5
regexp_table=$6
notice_ids="18801036793"


sql_part="
Insert overwrite TABLE profile${label}.dwd_app_20211030 partition(the_date,version_no)
select
c.row_key,c.mobile_id,c.app_name,c.app_type,max(c.behavior),c.start_time,max(c.virtual_id),c.the_date,c.version_no from
(
    select
    /*+mapjoin(b)*/
    a.row_key,
    a.mobile_id,
    b.app_index as app_name,
    b.type_index as app_type,
    b.behavior_index as behavior,
    a.event_time as start_time,
    case
    when b.virtual_id is not null then regexp_extract(a.msg,b.virtual_id,1)
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
    from ${source_table} where the_date='${the_date}' and app_name<>'未识别' )a
inner join
(
    select
    app_name,
    app_type,
    suspected_app_name,
    posi_regexp,
    nege_regexp,
    mapping_app_name,
    behavior,
    virtual_id,
    version_no,
    app_index,
    type_index,
    behavior_index
    from ${regexp_table} where the_date='2021-05-12' and version_no='${version_no}'
)b on a.app_name = b.app_name
where if(b.suspected_app_name is not null,a.suspected_app_name = b.suspected_app_name,1=1) and if(b.posi_regexp is not null,a.msg regexp b.posi_regexp,1=1) and if(b.nege_regexp is not null,a.msg not regexp b.nege_regexp,1=1)
)c
group by c.row_key,c.mobile_id,c.app_name,c.app_type,c.start_time,c.the_date,c.version_no
distribute by rand();
"
bash /home/${spark_sql_path}/spark_sql.sh "appv2" "$sql_part"


if [[ $? != 0 ]];then
    msg="定时调度【${version_no}版本规则对${the_date}短文本的APP行为信息抽取】1_extract_0512.sh脚本跑数失败了55555"
    curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg}'", "mobiles":"'${notice_ids}'"}' "http://10.10.15.52:8888/conch/notice/ding"
    exit 1
fi

msg2="定时调度【${version_no}版本规则对${the_date}短文本的APP行为信息抽取】跑数成功~！！！"
curl -X POST -H 'Content-Type: application/json' -d '{"accessToken": "faed9fc12a3a47bae96b94a068e2066642d0a366b752565d80465fe2a7203b9a","content": "'${msg2}'", "mobiles":"'${notice_ids}'" }' "http://10.10.15.52:8888/conch/notice/ding"


exit 0

