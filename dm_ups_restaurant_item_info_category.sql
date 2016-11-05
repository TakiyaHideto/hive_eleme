#***************************************************************************************************
# **  Profile Service @ dt.rec
#
# **  文件名称：dm_ups_restaurant_item_info_category.sql
# **  功能描述：导入餐厅分类
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-09-21
#
#***************************************************************************************************


drop table temp.temp_restaurant_rank_info_from_category_info;
create table temp.temp_restaurant_rank_info_from_category_info as
select t.restaurant_id, 'category' as top_category, split(item,'=')[0] as attr_key, split(item,'=')[1] as attr_value, 0 as is_json, '${day}' as update_time
from(
    select restaurant_id,
        array(
          concat('cat0_name=',cat0_name),
          concat('cat1_name=',cat1_name)
          ) as info_array
    from rec.rec_prf_restaurant_category_info
    where dt='${day}'
) t
lateral view explode(t.info_array) tmp as item
where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0;

insert overwrite table dm.dm_ups_restaurant_item_info partition(dt='${day}', flag='category')
select *
from temp.temp_restaurant_rank_info_from_category_info;

insert overwrite table dm.dm_ups_restaurant_item_info partition(dt='3000-12-31', flag='category')
select *
from temp.temp_restaurant_rank_info_from_category_info;