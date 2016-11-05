#***************************************************************************************************
# **  文件名称： dm_ups_food_item_info_trade.sql
# **  功能描述： 食品画像，销量情况插入
# **  创建者： jiahao.dong
# **  创建日期： 2016-09-18
#***************************************************************************************************

drop table temp.temp_food_order_join_table;
create table temp.temp_food_order_join_table as
select entity_id as food_id, name, order_id, quantity, created_at
from dw.dw_trd_order_item 
where dt='${day}' and datediff('${day}', created_at)<=30 and entity_category_id=1;

insert overwrite table dm.dm_ups_food_item_info partition(dt='${day}', flag='trade')
select food_id, 'trade' as top_category, 'order_cnt_30' as attr_key, 
    sum(case when datediff('${day}',created_at)<=30 then 1 else 0 end) as attr_value, 
    '0' as is_json, '${day}' as update_time
from temp.temp_food_order_join_table
group by food_id
union all 
select food_id, 'trade' as top_category, 'order_cnt_7' as attr_key, 
    sum(case when datediff('${day}',created_at)<=7 then 1 else 0 end) as attr_value, 
    '0' as is_json, '${day}' as update_time
from temp.temp_food_order_join_table
group by food_id
union all
select food_id, 'trade' as top_category, 'cnt_30' as attr_key, 
    sum(case when datediff('${day}',created_at)<=30 then quantity else 0 end) as attr_value, 
    '0' as is_json, '${day}' as update_time
from temp.temp_food_order_join_table
group by food_id
union all 
select food_id, 'trade' as top_category, 'cnt_7' as attr_key, 
    sum(case when datediff('${day}',created_at)<=7 then quantity else 0 end) as attr_value, 
    '0' as is_json, '${day}' as update_time
from temp.temp_food_order_join_table
group by food_id;

insert overwrite table dm.dm_ups_food_item_info partition(dt='3000-12-31', flag='trade')
select food_id, top_category, attr_key, attr_value, is_json, update_time
from dm.dm_ups_food_item_info
where dt='${day}' and flag='trade';