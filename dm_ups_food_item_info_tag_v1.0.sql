#***************************************************************************************************
# **  文件名称： dm_ups_food_item_info_tag_v1.0.sql
# **  功能描述： 食品画像，tag插入
# **  创建者： jiahao.dong
# **  创建日期： 2017-04-06
#***************************************************************************************************

use dm;
ALTER TABLE dm_ups_food_item_info DROP IF EXISTS PARTITION(dt='{day}', flag='tag');
ALTER TABLE dm_ups_food_item_info DROP IF EXISTS PARTITION(dt='3000-12-31', flag='tag');

-- sub task 1:
    -- load tag info data from dm.dm_ups_food_tag_item_info into dm_ups_food_item_info_tag
insert overwrite table dm.dm_ups_food_item_info partition(dt='${day}', flag='tag_v1.0')
    select 
        food_id, 
        'tag' as top_category, 
        tag_name as attr_key, 
        tag_value as attr_value, 
        '1' as is_json,
        '${day}' as update_time
    from(
        select
            food_id,
            tag_name,
            concat('[', concat_ws(',', collect_set(concat('"', tag_value, '"'))), ']') as tag_value
        from
            dm.dm_ups_food_tag_item_info
        where 
            dt='3000-12-31'
        group by 
            food_id,
            tag_name
        ) t
;

insert overwrite table dm.dm_ups_food_item_info partition(dt='3000-12-31', flag='tag_v1.0')
    select 
        food_id, 
        top_category, 
        attr_key, 
        attr_value, 
        '1' as is_json,
        '${day}' as update_time
    from
        dm.dm_ups_food_item_info
    where 
        dt='{day}' and
        flag='tag'
;

