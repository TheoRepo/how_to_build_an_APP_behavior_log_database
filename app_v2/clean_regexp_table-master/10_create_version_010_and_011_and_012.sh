source /etc/profile
source  ~/.bash_profile


spark_sql_path=$1
label=$2


sql_part="

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_2 where version_no ='001';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_2 where version_no ='002';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_2 where version_no ='003';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_2 where version_no ='004';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_2 where version_no ='005';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_2 where version_no ='006';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_2 where version_no ='007';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_1 where version_no ='008';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select * from profile${label}.dws_app_regexp_20211030_mid_1 where version_no ='009';

insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select 
app_name,
suspected_app_name,
posi_regexp,
nege_regexp,
mapping_app_name,
app_index,
app_type,
type_index,
behavior,
behavior_index,
virtual_id,
'010' as version_no,
the_date
from 
(select *, row_number() over (partition by 1 order by mapping_app_name) as rnk from profile${label}.dws_app_regexp_20211030_mid_5 where version_no = '010') a
where a.rnk >= 1 and a.rnk <= 2517;


insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select 
app_name,
suspected_app_name,
posi_regexp,
nege_regexp,
mapping_app_name,
app_index,
app_type,
type_index,
behavior,
behavior_index,
virtual_id,
'011' as version_no,
the_date
from 
(select *, row_number() over (partition by 1 order by mapping_app_name) as rnk from profile${label}.dws_app_regexp_20211030_mid_5 where version_no = '010') a
where a.rnk >= 2518 and a.rnk <= 4852;


insert overwrite table profile${label}.dws_app_regexp_20211030_mid_6 partition(version_no, the_date)
select 
app_name,
suspected_app_name,
posi_regexp,
nege_regexp,
mapping_app_name,
app_index,
app_type,
type_index,
behavior,
behavior_index,
virtual_id,
'012' as version_no,
the_date
from
(select *, row_number() over (partition by 1 order by mapping_app_name) as rnk from profile${label}.dws_app_regexp_20211030_mid_5 where version_no = '010') a
where a.rnk >= 4853 and a.rnk <= 5866;
"

cd /home/${spark_sql_path}/ && bash spark_sql.sh "app" "$sql_part"

if [[ $? != 0 ]];then
    echo "sql 运行失败！！！！！！"
    exit 1
fi
echo 分区 '${pt}'数据写入完成