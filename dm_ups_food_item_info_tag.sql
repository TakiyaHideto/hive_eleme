#***************************************************************************************************
# **  文件名称： dm_ups_food_item_info_tag.sql
# **  功能描述： 食品画像，标签tag插入
# **  创建者： jiahao.dong
# **  创建日期： 2016-09-18
#***************************************************************************************************

drop table temp.temp_ups_food_tag_info_normalize_name;
create table temp.temp_ups_food_tag_info_normalize_name as
    select 
        food_id, 
        normalize_food_name as normalize_name 
    from 
        dm.dm_mdl_food_name_normalize_day
    where 
        dt='${day}' and 
        normalize_food_name is not null and 
        normalize_food_name!=''
;


drop table temp.temp_ups_food_tag_info_category_fine;
create table temp.temp_ups_food_tag_info_category_fine as 
    select 
        t1.food_id, 
        t2.category
    from(
        select 
            *
        from 
            dm.dm_mdl_food_name_normalize_day
        where 
            dt='${day}'
        ) t1
    join(
        select
            *
        from 
            dim.dim_mdl_food_tag_classification
        where 
            part='class1_2'
        ) t2
    on (
        t1.normalize_food_name=t2.food_name
        )
    group by 
        t1.food_id,t2.category
;


drop table temp.temp_ups_food_tag_info_category_coarse;
create table temp.temp_ups_food_tag_info_category_coarse as 
    select 
        t1.food_id, 
        t2.category
    from(
        select 
            *
        from 
            dm.dm_mdl_food_name_normalize_day
        where 
            dt='${day}' 
        ) t1
    join(
        select 
            *
        from 
            dim.dim_mdl_food_tag_classification
        where 
            part='class1_3'
        ) t2
    on (
        t1.normalize_food_name=t2.food_name
        )
    group by 
        t1.food_id,
        t2.category
;


drop table temp.temp_ups_food_tag_info_flavor;
create table temp.temp_ups_food_tag_info_flavor as
    select t1.food_id, t2.flavor
    from(
        select 
            *
        from 
            dm.dm_mdl_food_name_normalize_day
        where 
            dt='${day}'
        ) t1
    join(
        select 
            *
        from 
            dim.dim_mdl_food_tag_classification
        where 
            part='class1_3'
        ) t2
    on (
        t1.normalize_food_name=t2.food_name
        )
    group by 
        t1.food_id,
        t2.flavor
;


drop table temp.temp_ups_food_tag_info_cooking_method;
create table temp.temp_ups_food_tag_info_cooking_method as 
    select 
        t1.food_id, 
        t2.method
    from(
        select 
            *
        from 
            dm.dm_mdl_food_name_normalize_day
        where 
            dt='${day}'
        ) t1
    join(
        select 
            *
        from 
            dim.dim_mdl_food_tag_classification
        where 
            part='class1_3'
        ) t2
    on (
        t1.normalize_food_name=t2.food_name
        )
    group by 
        t1.food_id,
        t2.method
;


drop table temp.temp_ups_food_tag_info_tag_function;
create table temp.temp_ups_food_tag_info_tag_function as 
    select 
        food_id, 
        tag_function
    from 
        dm.dm_mdl_food_name_normalize_day
    where 
        dt='${day}';

drop table temp.temp_ups_food_tag_info_tag_scene;
create table temp.temp_ups_food_tag_info_tag_scene as
    select 
        food_id, 
        tag_scene
    from 
        dm.dm_mdl_food_name_normalize_day
    where 
        dt='${day}';

insert overwrite table dm.dm_ups_food_item_info partition(dt='${day}', flag='tag')
    select 
        food_id, 
        'tag' as top_category, 
        'cat_0' as attr_key, 
        normalize_name as attr_value, 
        '0' as is_json, 
        '${day}' as update_time
    from 
        temp.temp_ups_food_tag_info_normalize_name
    
    union all
    select 
        food_id, 
        'tag' as top_category, 
        'cat_1' as attr_key, 
        concat('[',concat_ws(',' ,collect_set(concat('"', category, '"'))),']') as attr_value, 
        '1' as is_json, 
        '${day}' as update_time
    from 
        temp.temp_ups_food_tag_info_category_fine
    group by 
        food_id
    
    union all
    select 
        food_id, 
        'tag' as top_category, 
        'cat_2' as attr_key, 
        concat('[',concat_ws(',' ,collect_set(concat('"', category, '"'))),']') as attr_value, 
        '1' as is_json, 
        '${day}' as update_time
    from 
        temp.temp_ups_food_tag_info_category_coarse
    group by 
        food_id

    union all
    select 
        food_id, 
        'tag' as top_category, 
        'flavor' as attr_key, 
        concat('[',concat_ws(',' ,collect_set(concat('"', flavor, '"'))),']') as attr_value, 
        '1' as is_json, 
        '${day}' as update_time
    from 
        temp.temp_ups_food_tag_info_flavor
    group by 
        food_id

    union all
    select 
        food_id, 
        'tag' as top_category, 
        'cooking_method' as attr_key, 
        concat('[',concat_ws(',' ,collect_set(concat('"', method, '"'))),']') as attr_value,  
        '1' as is_json, 
        '${day}' as update_time
    from 
        temp.temp_ups_food_tag_info_cooking_method
    group by 
        food_id
    
    union all 
    select 
        food_id, 
        'tag' as top_category, 
        'tag_function' as attr_key, 
        max(tag_function) as attr_value,  
        '0' as is_json, 
        '${day}' as update_time
    from 
        temp.temp_ups_food_tag_info_tag_function
    group by 
        food_id
    
    union all
    select 
        food_id, 
        'tag' as top_category, 
        'tag_scene' as attr_key, 
        max(tag_scene) as attr_value,  
        '0' as is_json, 
        '${day}' as update_time
    from 
        temp.temp_ups_food_tag_info_tag_scene
    group by 
        food_id
;

insert overwrite table dm.dm_ups_food_item_info partition(dt='3000-12-31', flag='tag')
    select 
        food_id, 
        top_category, 
        attr_key, 
        attr_value, 
        is_json, 
        update_time
    from 
        dm.dm_ups_food_item_info
    where 
        dt='${day}' and 
        flag='tag'
;