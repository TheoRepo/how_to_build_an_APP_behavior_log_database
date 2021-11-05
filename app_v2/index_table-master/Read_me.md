# 脚本重难点分析
## 对于脚本2_initial_index_table.sh
这个脚本对每一个app_name赋予了一个唯一的id
实现方法row_number() over(partition by 1 order by app_name desc) AS app_id
代码逻辑是在表格中新增一个新的列，这一列全部由1组成，然后根据1组成的这一列
做partition by，因此选中了全量数据，然后对全量数据，做1，2，3，4，5...的排序。
### 重点
profile${label}.dws_app_index_mid2表中的三个字段，app_name, app_type, app_package
必须是一对一的关系，然后这三个字段要根据group by去重，最终保证app_name的唯一性。

当然第二种办法就是，做一张中间表，只取app_name，然后对app_name做group by，
然后让索引表和上面这张中间表，left join, 取b.app_index,也可以保证app_index的唯一性。

## 对于脚本1_collect_app_name.sh易错点
从逻辑上来讲，索引表的所有app_name都来自于策略表和正则表
而策略表的app_name对应的app_type都是“重点类”
正则表的app_name和app_type是一一对应的关系，而且都属于九大类中的一种
所以，索引表中的app_type不应该存在Null值

但是，写case when的时候，如果不些else的条件，对于没有匹配的数据，内容都是null
呈现到索引表的结果就是，app_type出现大量的null
正确的处理方式如下：  
case
when b.app_name is not null then '重点类'
else Null
end as app_type  
case
when b.app_name is not null then b.app_type
else a.app_type
end as app_type

## 对于脚本5_update_status_paoshuqiewushuju.sh
这个脚本，对跑数无数据的数据状态的更新逻辑是，已开发 = 跑数有数据 + 跑数且无数据
所以，当3_update_status_yikaifa.sh，4_update_status_paoshuyoushuju.sh两个脚本跑完以后，
5_update_status_paoshuqiewushuju.sh做的工作是，跑数且无数据 = 已开发 - 跑数有数据