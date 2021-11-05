# 使用dataworks导入数据
# csv文件保存的时候一定是UTF-8,这个非常重要，不然会在hive表中引入无法识别的乱码！！！
# 在datawork上，将csv的内容写入一张没有分区的普通临时表
drop table if exists ga.interface_app_strategy_20211030_temp;
create table ga.interface_app_strategy_20211030_temp 
(
index string comment '索引',
app_name string comment 'app名称',
app_package string comment '已知安装包名',
app_strategy string comment 'app分类策略',
event_time string comment '数据入库时间'
)
comment 'app初始化策略表'
lifecycle 365;

load data '/user/datawork/users/qianyu/qianyu_upload/app_list_all.csv' table tdl_app_list_all options(delimiter=',',header='true');
insert overwrite table ga.interface_app_strategy_20211030_temp select * from tdl_app_list_all;

compress table ga.interface_app_strategy_20211030_temp options(type='snappy')