#***************************************************************************************************
# ** 文件名称： dm_ups_restaurant_info.sql
# ** 功能描述： 餐厅画像拼接json
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-10-24
#***************************************************************************************************


-- sub task 1: import data
insert overwrite table dm.dm_ups_restaurant_info partition(dt='${day}')
select t.restaurant_id, concat('{', concat_ws(',', collect_set(t.category_value)),'}') profile_json 
from 
(
	select restaurant_id, top_category,
	    concat('\"', top_category, '\":{', 
	        concat_ws(',', collect_set(concat('\"', attr_key, '\":', case when is_json=1 then attr_value else concat('\"', attr_value, '\"') end))
	        	),'}'
	        ) category_value 
	from dm.dm_ups_restaurant_item_info 
	where dt='3000-12-31'
	group by restaurant_id, top_category
) t 
group by t.restaurant_id;

insert overwrite table dm.dm_ups_restaurant_info partition(dt='3000-12-31') 
select restaurant_id, profile_json from dm.dm_ups_restaurant_info where dt='${day}';

