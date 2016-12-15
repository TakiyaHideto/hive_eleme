#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_rec_model.sql
# ** 功能描述： 热卖美食特征迁移
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-23
#***************************************************************************************************


-- sub task 1: 提取用户相关信息
drop table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_user_info;
create table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_user_info as 
    select 
        user_id,
        attr_key,
        max(attr_value) as attr_value
    from
        rec.rec_usr_hotfood_rec_userinfo
    where 
        dt='${day}' and
        attr_key in ('base','tag_prefer','cat_prefer','cat_profile')
    group by 
        user_id,
        attr_key
;



-- sub task 2: 提取用户订单信息
drop table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_order_sub1;
create table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_order_sub1 as 
    select
        user_id,
        count(distinct order_id) order_cnt,
        round(avg(total), 4) order_amt,
        round(avg(food_amt), 4) food_amt,
        min(order_date) order_date_min,
        max(order_date) order_date_max
    from 
        rec.rec_prf_order_wide_day_inc
    where 
        dt between date_sub('${day}',74) and '${day}'
    group by 
        user_id
;

-- sub task 2.1: 用户订单信息，用户最近下单餐厅和二级类目
drop table temp.temp_mdl_last_restaurant_order_info_sub2;
create table temp.temp_mdl_last_restaurant_order_info_sub2 as 
    select
        a.user_id,
        max(a.restaurant_id) as last_order_shop_id,
        concat('[',concat_ws(',',collect_set(concat('\"',b.cat1_name,'\"'))),']') as last_order_category_id
    from(
        select
            t.user_id,
            t.restaurant_id,
            t.last_order_date,
            row_number() over (partition by t.user_id order by t.last_order_date) as rno
        from(
            select
                user_id,
                restaurant_id,
                max(order_date) as last_order_date
            from
                dw.dw_trd_order_wide_day
            where 
                dt>=get_date('${day}',-59) and
                dt<='${day}' and
                order_status=1
            group by 
                user_id,
                restaurant_id
            ) t
        ) a
    join (
        select
            restaurant_id,
            cat1_name
        from 
            rec.rec_prf_restaurant_category_info
        where 
            dt='${day}'
        ) b
    on(
        a.restaurant_id=b.restaurant_id
        )
    where 
        a.rno=1
    group by 
        a.user_id
;




-- sub task 3: 生成用户订单信息
drop table if exists temp.temp_mdl_rec_usr_hotfood_rec_userinfo_user_order;
create table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_user_order as
    SELECT 
        t1.user_id,
        concat('{', 
               concat_ws(',', 
                   concat('"order_cnt"', ':', order_cnt), 
                   concat('"order_amt"', ':', order_amt), 
                   concat('"food_amt"', ':',food_amt), 
                   concat('"order_date_min"', ':', '"', order_date_min, '"'), 
                   concat('"order_date_max"', ':', '"', order_date_max, '"'),
                   concat('"last_order_shop_id"',':','"',last_order_shop_id,'"'),
                   concat('"last_order_category_id"',':',last_order_category_id)), 
               '}') as user_order,
        concat_ws(',', 
               concat('order_cnt', ':', order_cnt), 
               concat('order_amt', ':', order_amt), 
               concat('food_amt', ':', food_amt), 
               concat('order_date_min', ':', order_date_min),
               concat('order_date_max', ':', order_date_max),
               concat('last_order_shop_id',':',last_order_shop_id),
               concat('last_order_category_id',':',last_order_category_id)
               ) as contents
    from
        temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_order_sub1 t1
    join
        temp.temp_mdl_last_restaurant_order_info_sub2 t2
    on(
        t1.user_id=t2.user_id
        )
;



-- sub task 4: 生成用户商户信息
drop table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_order;
create table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_order as
    select 
        user_id,
        concat('{', concat_ws(',', collect_set(concat('"',restaurant_id,'"',':', content))), '}') as shop_order
    from(
        select 
            t1.user_id,
            restaurant_id,
            concat('{', 
                concat_ws(',', 
                    concat('"order_cnt"',':', t2.order_cnt), 
                    concat('"order_amt"', ':', t2.order_amt), 
                    concat('"food_amt"', ':', t2.food_amt), 
                    concat('"order_date_min"', ':', '"', t2.order_date_min, '"'), 
                    concat('"order_date_max"', ':', '"', t2.order_date_max, '"'),
                    concat('"order_rate"', ':', '"', round(t2.order_cnt/t1.order_cnt,2), '"')
                    ), 
                '}') content
        from 
            temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_order_sub1 t1
        join(
            select
                user_id,
                restaurant_id,
                count(distinct order_id) order_cnt,
                round(avg(total), 4) order_amt,
                round(avg(food_amt), 4) food_amt,
                min(order_date) order_date_min,
                max(order_date) order_date_max
            from 
                rec.rec_prf_order_wide_day_inc
            where 
                dt between date_sub('${day}',74) and '${day}'
            group by 
                user_id,
                restaurant_id
            ) t2
        on(
            t1.user_id=t2.user_id
            )
        
        ) a
    group by 
        user_id
;



-- sub task 5: 生成用户餐厅收藏列表
drop table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_favored;
create table temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_favored as
    select
        user_id,
        concat('{',
            concat_ws(',',collect_set(concat('"',restaurant_id,'"',':','"',created_at,'"'))),
            '}') as rst_favored
    from
        dw.dw_com_favored_restaurant
    where 
        dt between date_sub('${day}',74) and '${day}'
    group by
        user_id
;




-- sub task 6: import data into ups
insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_model')
    select 
        user_id, 
        'rec' as top_category, 
        attr_key, 
        attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_rec_usr_hotfood_rec_userinfo_user_info

    union all
    select
        user_id,
        'rec' as top_category,
        'user_order' as attr_key,
        user_order as attr_value,
        '1' as is_json,
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_rec_usr_hotfood_rec_userinfo_user_order

    union all
    select
        user_id,
        'rec' as top_category,
        'rest_behavior' as attr_key,
        shop_order as attr_value,
        '1' as is_json,
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_order

    union all
    select
        user_id,
        'rec' as top_category,
        'shop_favored' as attr_key,
        rst_favored as attr_value,
        '1' as is_json,
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_rec_usr_hotfood_rec_userinfo_shop_favored
;


insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_model')
    select
        user_id,
        top_category,
        attr_key,
        attr_value,
        is_json,
        update_time
    from
        dm.dm_ups_user_item_info
    where 
        dt='${day}' and 
        flag='rec_model'
;
