#***************************************************************************************************
# ** 文件名称： dm_mdl_data_index_monitor.sql
# ** 功能描述： 用于检测数据覆盖及内容是否正常
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-09-18
#***************************************************************************************************

---检测dm_mdl_food_name_normalize_day表有正则化名称的食物的覆盖率
select sum(case when normalize_food_name is not null and normalize_food_name!='' then 1 else 0 end)/count(*)
from dm.dm_mdl_food_name_normalize_day
where dt='${day}';

