#***************************************************************************************************
# **  文件名称： dm_ups_food_item_info_base.sql
# **  功能描述： 食品画像
# **  创建者： jiahao.dong
# **  创建日期： 2016-09-14
#***************************************************************************************************

drop table temp.temp_ups_food_basic_info;
create table temp.temp_ups_food_basic_info as
select food_id, 'base' as top_category, 
    split(item,'=')[0] as attr_key, 
    split(item,'=')[1] as attr_value, 
    '0' as is_json,
    '${day}' as update_time
from(
    select id as food_id, 
        array(
    	    concat('name=',name),
    	    concat('restaurant_id=',restaurant_id),
            concat('price_original=',original_price),
            concat('price_current=',price),
            concat('price_changed_at=',price_changed_at),
            concat('description=',description),
            concat('is_new=',is_new),
            concat('is_featured=',is_featured),
            concat('is_gum=',is_gum),
            concat('has_activity=',has_activity),
            concat('sold_out=',sold_out),
            concat('packing_fee=',packing_fee)
            ) as info_array
    from dw.dw_prd_food
    where dt='${day}' and is_valid=1
) t
lateral view explode(t.info_array) tmp as item;

insert overwrite table dm.dm_ups_food_item_info partition(dt='${day}', flag='base')
select *
from temp.temp_ups_food_basic_info;


insert overwrite table dm.dm_ups_food_item_info partition(dt='3000-12-31', flag='base')
select *
from temp.temp_ups_food_basic_info;