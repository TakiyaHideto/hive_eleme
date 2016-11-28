#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_rec_fea.sql
# ** 功能描述： 加入用户画像推荐目录下的特征
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-15
#***************************************************************************************************


-- -- sub task 1: 用户餐厅一级分类偏好
-- drop table temp.temp_mdl_user_restaurant_category_0_order;
-- create table temp.temp_mdl_user_restaurant_category_0_order as 
--     select 
--         t1.user_id,
--         t2.cat0_name,
--         count(distinct t1.order_id) as order_cnt
--     from(
--         select 
--             id as order_id,
--             restaurant_id,
--             user_id
--         from    
--             dw.dw_trd_order_wide_day
--         where
--             dt>get_date(-61) and
--             order_status=1
--         ) t1
--     join(
--         select
--             restaurant_id,
--             cat0_name,
--             cat1_name
--         from
--             rec.rec_prf_restaurant_category_info
--         where
--             dt='2016-11-23'
--         ) t2
--     on(
--         t1.restaurant_id=t2.restaurant_id
--         )
--     group by 
--         t1.user_id,
--         t2.cat0_name
-- ;

-- -- sub task 1.1: 用户一级餐厅分类订单订单最大值最小值
-- drop table temp.temp_mdl_user_restaurant_category_0_preference_min_max;
-- create table temp.temp_mdl_user_restaurant_category_0_preference_min_max as 
--     select
--         user_id,
--         max(order_cnt) as order_max,
--         min(order_cnt) as order_min
--     from
--         temp.temp_mdl_user_restaurant_category_0_order
--     group by 
--         user_id
-- ;

-- -- sub task 1.2: 用户一级餐厅分类偏好分数
-- drop table temp.temp_mdl_user_restaurant_category_0_preference;
-- create table temp.temp_mdl_user_restaurant_category_0_preference as 
--     select
--         t1.user_id,
--         cat0_name,
--         case when t2.order_max-t2.order_min!=0 then
--                 bound_data((t1.order_cnt-t2.order_min)/(t2.order_max-t2.order_min)+0.1,0,1.0)
--             else
--                 1.0 
--             end as cat0_score 
--     from 
--         temp.temp_mdl_user_restaurant_category_0_order t1
--     join 
--         temp.temp_mdl_user_restaurant_category_0_preference_min_max t2
--     on 
--         t1.user_id=t2.user_id
-- ;


-- -- sub task 2: 用户餐厅二级分类偏好
-- drop table temp.temp_mdl_user_restaurant_category_1_order;
-- create table temp.temp_mdl_user_restaurant_category_1_order as 
--     select 
--         t1.user_id,
--         t2.cat1_name,
--         count(distinct t1.order_id) as order_cnt
--     from(
--         select 
--             id as order_id,
--             restaurant_id,
--             user_id
--         from    
--             dw.dw_trd_order_wide_day
--         where
--             dt>get_date(-61) and
--             order_status=1
--         ) t1
--     join(
--         select
--             restaurant_id,
--             cat0_name,
--             cat1_name
--         from
--             rec.rec_prf_restaurant_category_info
--         where
--             dt='2016-11-23'
--         ) t2
--     on(
--         t1.restaurant_id=t2.restaurant_id
--         )
--     group by 
--         t1.user_id,
--         t2.cat1_name
-- ;


-- -- sub task 2.1: 用户二级餐厅分类订单订单最大值最小值
-- drop table temp.temp_mdl_user_restaurant_category_1_preference_min_max;
-- create table temp.temp_mdl_user_restaurant_category_1_preference_min_max as 
--     select
--         user_id,
--         max(order_cnt) as order_max,
--         min(order_cnt) as order_min
--     from
--         temp.temp_mdl_user_restaurant_category_1_order
--     group by 
--         user_id
-- ;


-- -- sub task 2.2: 用户二级餐厅分类偏好分数
-- drop table temp.temp_mdl_user_restaurant_category_1_preference;
-- create table temp.temp_mdl_user_restaurant_category_1_preference as 
--     select
--         t1.user_id,
--         cat1_name,
--         case when t2.order_max-t2.order_min!=0 then
--                 bound_data((t1.order_cnt-t2.order_min)/(t2.order_max-t2.order_min)+0.1,0,1.0)
--             else
--                 1.0 
--             end as cat1_score 
--     from 
--         temp.temp_mdl_user_restaurant_category_1_order t1
--     join 
--         temp.temp_mdl_user_restaurant_category_1_preference_min_max t2
--     on 
--         t1.user_id=t2.user_id
-- ;


-- sub task 3: 红包门槛与订单金额差值均值
drop table temp.temp_mdl_user_hongbao_thres_order_price_gap_sub1;
create table temp.temp_mdl_user_hongbao_thres_order_price_gap_sub1 as 
    select
        user_id,
        round(avg(case when sum_condition is not null then 
                    total-sum_condition
                else
                    total-0
                end
            ),2) as hongbao_price_thres_gap_avg,
        count(distinct order_id) as order_cnt
    from(
        select
            user_id,
            total,
            id as order_id,
            hongbao_id
        from
            dw.dw_trd_order_wide_day
        where
            dt>get_date(-61) and
            order_status=1
        ) t1
    left outer join(
        select
            id as hongbao_id,
            max(sum_condition) as sum_condition
        from
            dw.dw_trd_hongbao
        where
            dt='2016-11-23' and
            datediff('2016-11-23',created_at)<61
        group by
            id
        ) t2
    on(
        t1.hongbao_id=t2.hongbao_id
        )
    group by
        user_id
;

-- sub task 3.1
drop table temp.temp_mdl_user_hongbao_thres_order_price_gap;
create table temp.temp_mdl_user_hongbao_thres_order_price_gap as 
    select
        t1.user_id,
        hongbao_price_thres_gap_avg,
        (hongbao_price_thres_gap_avg-hongbao_price_thres_gap_avg_min)/(hongbao_price_thres_gap_avg_max-hongbao_price_thres_gap_avg_min) as hongbao_price_thres_gap_avg_normal,
        order_cnt
    from
        temp.temp_mdl_user_hongbao_thres_order_price_gap_sub1 t1
    join(
        select
            user_id,
            max(hongbao_price_thres_gap_avg) as hongbao_price_thres_gap_avg_max,
            min(hongbao_price_thres_gap_avg) as hongbao_price_thres_gap_avg_min
        from
            temp.temp_mdl_user_hongbao_thres_order_price_gap_sub1
        group by 
            user_id
        ) t2
    on(
        t1.user_id=t2.user_id
        )
;


-- sub task 4: 红包门槛与未被红包优惠订单金额差值均值
drop table temp.temp_mdl_user_hongbao_thres_order_payment_gap_sub1;
create table temp.temp_mdl_user_hongbao_thres_order_payment_gap_sub1 as 
    select
        user_id,
        round(avg(case when sum_condition is not null then 
                    total-sum_condition
                else
                    total-0
                end
            ),2) as hongbao_payment_thres_gap_avg,
        count(distinct order_id) as order_cnt
    from(
        select
            user_id,
            total-eleme_subsidy-restaurant_subsidy as total,
            id as order_id,
            hongbao_id,
        from
            dw.dw_trd_order_wide_day
        where
            dt>get_date(-61) and
            order_status=1
        ) t1
    left outer join(
        select
            id as hongbao_id,
            max(sum_condition) as sum_condition
        from
            dw.dw_trd_hongbao
        where
            dt='2016-11-23' and
            datediff('2016-11-23',created_at)<61
        group by
            id
        ) t2
    on(
        t1.hongbao_id=t2.hongbao_id
        )
    group by
        user_id
;


-- sub task 4.1
drop table temp.temp_mdl_user_hongbao_thres_order_payment_gap;
create table temp.temp_mdl_user_hongbao_thres_order_payment_gap as 
    select
        t1.user_id,
        hongbao_payment_thres_gap_avg,
        (hongbao_payment_thres_gap_avg-hongbao_payment_thres_gap_avg_min)/(hongbao_payment_thres_gap_avg_max-hongbao_payment_thres_gap_avg_min) as hongbao_payment_thres_gap_avg_normal,
        order_cnt
    from
        temp.temp_mdl_user_hongbao_thres_order_payment_gap_sub1 t1
    join(
        select
            user_id,
            max(hongbao_payment_thres_gap_avg) as hongbao_payment_thres_gap_avg_max,
            min(hongbao_payment_thres_gap_avg) as hongbao_payment_thres_gap_avg_min
        from
            temp.temp_mdl_user_hongbao_thres_order_payment_gap_sub1
        group by 
            user_id
        ) t2
    on(
        t1.user_id=t2.user_id
        )
;






-- sub task 3: import data into ups
insert overwrite table dm.dm_ups_user_item_info partition(dt='2016-11-23', flag='rec_fea_jiahao')
    select 
            t.user_id, 
            'rec' as top_category, 
            split(item,'=')[0] as attr_key, 
            split(item,'=')[1] as attr_value, 
            '0' as is_json, 
            '2016-11-23' as update_time
        from(
            select 
                user_id,
                array(
                    concat('cat0_score=', max(round(cat0_score,2)))
                ) as info_array
            from 
                temp.temp_mdl_user_restaurant_category_0_preference
            group by 
                user_id
        ) t
        lateral view explode(t.info_array) tmp as item
        where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0
    
    union all
        select 
            t.user_id, 
            'rec' as top_category, 
            split(item,'=')[0] as attr_key, 
            split(item,'=')[1] as attr_value, 
            '0' as is_json, 
            '2016-11-23' as update_time
        from(
            select 
                user_id,
                array(
                    concat('cat1_score=', max(round(cat1_score,2)))
                ) as info_array
            from 
                temp.temp_mdl_user_restaurant_category_1_preference
            group by 
                user_id
        ) t
        lateral view explode(t.info_array) tmp as item
        where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0

    union all
    	select 
            t.user_id, 
            'rec' as top_category, 
            split(item,'=')[0] as attr_key, 
            split(item,'=')[1] as attr_value, 
            '0' as is_json, 
            '2016-11-23' as update_time
        from(
            select 
                user_id,
                array(
                    concat('hongbao_price_thres_gap_avg=', max(round(hongbao_price_thres_gap_avg,2))),
                    concat('hongbao_price_thres_gap_avg_normal=', max(round(hongbao_price_thres_gap_avg_normal,2)))
                ) as info_array
            from 
                temp.temp_mdl_user_hongbao_thres_order_price_gap
            group by 
                user_id
        ) t
        lateral view explode(t.info_array) tmp as item
        where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0

    union all
    	select 
            t.user_id, 
            'rec' as top_category, 
            split(item,'=')[0] as attr_key, 
            split(item,'=')[1] as attr_value, 
            '0' as is_json, 
            '2016-11-23' as update_time
        from(
            select 
                user_id,
                array(
                    concat('hongbao_payment_thres_gap_avg=', max(round(hongbao_payment_thres_gap_avg,2))),
                    concat('hongbao_payment_thres_gap_avg_normal=', max(round(hongbao_payment_thres_gap_avg_normal,2)))
                ) as info_array
            from 
                temp.temp_mdl_user_hongbao_thres_order_payment_gap
            group by
                user_id
        ) t
        lateral view explode(t.info_array) tmp as item
        where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0
;



insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_fea_jiahao')
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
        dt='2016-11-23' and 
        flag='rec_fea_jiahao'
;

