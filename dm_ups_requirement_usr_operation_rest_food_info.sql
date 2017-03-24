#***************************************************************************************************
# **  User Profile Service: offline data requirement
#
# **  文件名称： dm_ups_requirement_usr_operation_rest_food_info.sql
# **  功能描述：
#        1. 用户运营组餐厅和食品的交叉数据
#        2. 包括 餐厅id，餐厅名，菜品id，菜品名，菜品分类，菜品时段标签（正餐，下午茶，夜宵），菜品是否有折扣，折扣力度
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2017-02-21
# 
#***************************************************************************************************

drop table temp.temp_dm_ups_requirement_usr_operation_rest_food_info_1;
create table temp.temp_dm_ups_requirement_usr_operation_rest_food_info_1 as
    select
        t1.restaurant_id as restaurant_id,
        t1.name as restaurant_name, 
        t2.food_id as food_id,
        t2.name as food_name,
        t2.cat_0 as food_cat_0,
        t2.cat_1 as food_cat_1,
        t2.cat_2 as food_cat_2,
        t2.has_activity
    from (
        select
            restaurant_id,
            parse_json_object(profile_json,'base.name') as name
        from
            dm.dm_ups_restaurant_info
        where 
            dt='3000-12-31'
        ) t1
    join (
        select
            food_id,
            parse_json_object(profile_json,'base.restaurant_id') as restaurant_id,
            parse_json_object(profile_json,'base.name') as name,
            parse_json_object(profile_json,'tag.cat_0') as cat_0,
            parse_json_object(profile_json,'tag.cat_1') as cat_1,
            parse_json_object(profile_json,'tag.cat_2') as cat_2,
            parse_json_object(profile_json,'base.has_activity') as has_activity
        from
            dm.dm_ups_food_info
        where
            dt='3000-12-31'
        ) t2
    on (
        t1.restaurant_id = t2.restaurant_id
        )
;


drop table temp.temp_dm_ups_requirement_usr_operation_rest_food_info_2;
create table temp.temp_dm_ups_requirement_usr_operation_rest_food_info_2 as
    select
        t1.restaurant_id as restaurant_id,
        restaurant_name, 
        food_id,
        food_name,
        food_cat_0,
        food_cat_1,
        food_cat_2,
        if(t2.is_afternoon_tea=1, 1, 0) as is_afternoon_tea,
        if(t2.is_night_snack=1, 1, 0) as is_night_snack,
        has_activity
    from 
        temp.temp_dm_ups_requirement_usr_operation_rest_food_info_1 t1
    left outer join(
            select 
                restaurant_id, 
                case when cat1_id in (240,241,242,243,249,245) then 1 
                    else 0 end as is_afternoon_tea,
                case when cat1_id in (234,235,236,237,238,214,218) then 1 
                    else 0 end as is_night_snack
            from 
                rec.rec_prf_restaurant_category_info
            where 
                dt='${day}' and 
                cat1_id in (240,241,242,243,249,245,234,235,236,237,238,214,218) 
        ) t2
    on (
        t1.restaurant_id = t2.restaurant_id
        )
;

insert overwrite table dm.dm_ups_requirement_usr_operation_rest_food_info partition(dt='${day}')
    select
        *
    from 
        temp.temp_dm_ups_requirement_usr_operation_rest_food_info_2
;
