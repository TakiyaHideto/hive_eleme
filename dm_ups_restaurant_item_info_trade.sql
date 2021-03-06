#***************************************************************************************************
# **  Profile Service @ dt.rec
# **
# **  文件名称：dm_ups_restaurant_item_info_trade.sql
# **  功能描述：导入餐厅销量情况
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-10-20
# **  
#***************************************************************************************************



-- sub task 1.1: 统计7天内多次下单的user数量
drop table temp.temp_mdl_restaurant_recent_7_returning_customer;
create table temp.temp_mdl_restaurant_recent_7_returning_customer as
    select 
        t.restaurant_id, 
        t.user_id, 
        t.order_cnt
    from(
        select 
            restaurant_id, 
            user_id,
            count(distinct id) as order_cnt
        from 
            dw.dw_trd_order_wide_day
        where 
            dt>=get_date('${day}',-8) and 
            order_status=1 and
            user_id<>886
        group by 
            restaurant_id, 
            user_id
    ) t
    where 
        t.order_cnt>=1
;



-- sub task 1.2: 统计30天内多次下单的user数量
drop table temp.temp_mdl_restaurant_recent_30_returning_customer;
create table temp.temp_mdl_restaurant_recent_30_returning_customer as
    select 
        t.restaurant_id, 
        t.user_id, 
        t.order_cnt
    from(
        select 
            restaurant_id, 
            user_id,
            count(distinct id) as order_cnt
        from 
            dw.dw_trd_order_wide_day
        where 
            dt>=get_date('${day}',-31) and 
            order_status=1 and
            user_id<>886
        group by 
            restaurant_id, 
            user_id
    ) t
    where 
        t.order_cnt>=1
;



-- sub task 2: 统计餐厅最近7天的交易信息
    --包括：最近7天订单量、最近7天下单用户数、最近7天下单用户数与订单数比值、最近7天回头客占比
drop table temp.temp_mdl_restaurant_recent_7_trade;
create table temp.temp_mdl_restaurant_recent_7_trade as
    select 
        t1.restaurant_id, 
        count(distinct t1.id) as recent_7_order_cnt,
        count(distinct t1.user_id) as recent_7_user_cnt,
        round(count(distinct t1.id)/count(distinct t1.user_id),2) as recent_7_ord_usr_ratio,
        count(distinct t2.user_id)/count(distinct t1.user_id) as recent_7_returned_customer_scale,
        round(sum(t1.total),2) as recent_7_order_amt
    from (
        select *
        from 
            dw.dw_trd_order_wide_day
        where  
            dt>=get_date('${day}',-8) and 
            order_status=1 and 
            user_id<>886
    ) t1
    left outer join 
        temp.temp_mdl_restaurant_recent_7_returning_customer t2
    on(
        t1.restaurant_id=t2.restaurant_id and 
        t1.user_id=t2.user_id
        )
    group by 
        t1.restaurant_id
;



-- sub task 3: 统计餐厅最近30天的交易信息
drop table temp.temp_mdl_restaurant_recent_30_trade;
create table temp.temp_mdl_restaurant_recent_30_trade as
    select 
        t1.restaurant_id, 
        count(distinct t1.id) as recent_30_order_cnt,
        count(distinct t1.user_id) as recent_30_user_cnt,
        round(count(distinct t1.id)/count(distinct t1.user_id),2) as recent_30_ord_usr_ratio,
        count(distinct t2.user_id)/count(distinct t1.user_id) as recent_30_returned_customer_scale,
        round(sum(t1.total),2) as recent_30_order_amt,
        percentile_approx(total,0.5) as recent_30_order_price_median,
        stddev_samp(total) as recent_30_order_price_stddev,
        sum(t1.eleme_order_total) as recent_30_payment_amt,
        percentile_approx(eleme_order_total,0.5) as recent_30_payment_median,
        stddev_samp(eleme_order_total) as recent_30_payment_stddev,
        sum(eleme_subsidy) as recent_30_eleme_subsidy_amt,
        percentile_approx(eleme_subsidy,0.5) as recent_30_eleme_subsidy_median,
        stddev_samp(eleme_subsidy) as recent_30_eleme_subsidy_stddev,
        sum(case when status_code=3 then 1 else 0 end)/count(id) as recent_30_order_chargeback_scale,
        sum(case when status_code=-5 then 1 else 0 end) as recent_30_order_unpaid_cnt,
        sum(case when status_code=-5 then 1 else 0 end)/count(id) as recent_30_order_unpaid_scale,
        avg(hongbao_amount) as recent_30_order_hongbao_preferential_avg,
        stddev_samp(hongbao_amount) as recent_30_order_hongbao_preferential_stddev
    from (
        select 
            *
        from 
            dw.dw_trd_order_wide_day
        where 
            dt>=get_date('${day}',-30) and 
            order_status=1 and
            user_id<>886
        ) t1
    left outer join temp.temp_mdl_restaurant_recent_30_returning_customer t2
    on(
        t1.restaurant_id=t2.restaurant_id and 
        t1.user_id=t2.user_id
        )
    group by 
        t1.restaurant_id
;



-- sub task 4.1: 统计餐厅长期交易信息 sub 1
drop table temp.temp_mdl_restaurant_long_term_trade;
create table temp.temp_mdl_restaurant_long_term_trade as
    select 
        restaurant_id,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=7 then 1 else 0 end) as sun_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=1 then 1 else 0 end) as mon_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=2 then 1 else 0 end) as tues_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=3 then 1 else 0 end) as wed_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=4 then 1 else 0 end) as thur_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=5 then 1 else 0 end) as fri_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=6 then 1 else 0 end) as sat_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=7 then total else 0 end) as sun_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=1 then total else 0 end) as mon_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=2 then total else 0 end) as tues_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=3 then total else 0 end) as wed_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=4 then total else 0 end) as thur_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=5 then total else 0 end) as fri_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')=6 then total else 0 end) as sat_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')>=1 and from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')<=5 then 1 else 0 end) as workday_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')>=6 and from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')<=7 then 1 else 0 end) as weekend_order_cnt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')>=1 and from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')<=5 then total else 0 end) as workday_sales_amt,
        sum(case when from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')>=6 and from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'),'u')<=7 then total else 0 end) as weekend_sales_amt,
        sum(case when substr(created_at,12,5)>='10:00' and substr(created_at,12,5)<'14:00' then 1 else 0 end) as lunch_order_cnt,
        sum(case when substr(created_at,12,5)>='14:00' and substr(created_at,12,5)<'16:30' then 1 else 0 end) as tea_order_cnt,
        sum(case when substr(created_at,12,5)>='16:30' and substr(created_at,12,5)<'20:00' then 1 else 0 end) as supper_order_cnt,
        sum(case when substr(created_at,12,5)>='20:00' and substr(created_at,12,5)<'24:00' then 1 else 0 end) as night_order_cnt,
        sum(case when substr(created_at,12,5)>='00:00' and substr(created_at,12,5)<'10:00' then 1 else 0 end) as other_order_cnt,
        sum(case when substr(created_at,12,5)>='10:00' and substr(created_at,12,5)<'14:00' then total else 0 end) as lunch_sales_amt,
        sum(case when substr(created_at,12,5)>='14:00' and substr(created_at,12,5)<'16:30' then total else 0 end) as tea_sales_amt,
        sum(case when substr(created_at,12,5)>='16:30' and substr(created_at,12,5)<'20:00' then total else 0 end) as supper_sales_amt,
        sum(case when substr(created_at,12,5)>='20:00' and substr(created_at,12,5)<'24:00' then total else 0 end) as night_sales_amt,
        sum(case when substr(created_at,12,5)>='00:00' and substr(created_at,12,5)<'10:00' then total else 0 end) as other_sales_amt
    from 
        dw.dw_trd_order_wide_day
    where 
        dt>=get_date('${day}',-60) and 
        order_status=1 and
        user_id<>886
    group by 
        restaurant_id
;



-- sub task 4.2: 统计餐厅长期交易信息(排序信息) sub 2
drop table temp.temp_mdl_restaurant_long_term_trade_rank;
create table temp.temp_mdl_restaurant_long_term_trade_rank as
select t.restaurant_id,
    concat('[',concat_ws(',',collect_set(cast(t.food_id as string))),']') as food_top_10
from (
    select t3.restaurant_id, t3.food_id,
        t3.food_sales_cnt,
        row_number() over(partition by t3.restaurant_id order by t3.food_sales_cnt desc) as food_rank
    from (
        select t1.restaurant_id, t2.entity_id as food_id,
            count(distinct t2.order_id) as food_sales_cnt
        from (
            select 
                restaurant_id, id
            from 
                dw.dw_trd_order_wide_day
            where 
                dt>=get_date('${day}',-30) and 
                order_status=1
        ) t1
        join (
            select order_id, entity_id
            from dw.dw_trd_order_item
            where dt='${day}' and entity_id>0
        ) t2
        on t1.id=t2.order_id
        group by t1.restaurant_id, t2.entity_id
    ) t3 
) t
where t.food_rank<=10
group by t.restaurant_id;




-- sub task 5: 汇总，形成json
drop table temp.temp_restaurant_trade_info_from_trade_info;
create table temp.temp_restaurant_trade_info_from_trade_info as
select t.restaurant_id, 'trade' as top_category, split(item,'=')[0] as attr_key, 
    split(item,'=')[1] as attr_value, 
    0 as is_json, 
    '${day}' as update_time
from(
    select restaurant_id,
        array(
            concat('recent_7_order_cnt=', recent_7_order_cnt),
            concat('recent_7_user_cnt=', recent_7_user_cnt),
            concat('recent_7_ord_usr_ratio=', round(recent_7_ord_usr_ratio),3),
            concat('recent_7_returned_customer_scale=', round(recent_7_returned_customer_scale),3), 
            concat('recent_7_order_amt=', recent_7_order_amt)
        ) as info_array
    from temp.temp_mdl_restaurant_recent_7_trade
) t
lateral view explode(t.info_array) tmp as item
where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0

union all
select t.restaurant_id, 'trade' as top_category, split(item,'=')[0] as attr_key, 
    split(item,'=')[1] as attr_value, 
    0 as is_json, 
    '${day}' as update_time
from(
    select restaurant_id,
        array(
            concat('recent_30_order_cnt=', max(recent_30_order_cnt)),
            concat('recent_30_user_cnt=', max(recent_30_user_cnt)),
            concat('recent_30_ord_usr_ratio=', max(round(recent_30_ord_usr_ratio,3))),
            concat('recent_30_returned_customer_scale=', max(round(recent_30_returned_customer_scale,3))),
            concat('recent_30_order_amt=', max(recent_30_order_cnt)),
            concat('recent_30_order_price_median=', max(round(recent_30_order_price_median,3))),
            concat('recent_30_order_price_stddev=', max(round(recent_30_order_price_stddev,3))),
            concat('recent_30_payment_amt=', max(recent_30_payment_amt)),
            concat('recent_30_payment_median=', max(round(recent_30_payment_median,3))),
            concat('recent_30_payment_stddev=', max(round(recent_30_payment_stddev,3))),
            concat('recent_30_eleme_subsidy_amt=', max(recent_30_eleme_subsidy_amt)),
            concat('recent_30_eleme_subsidy_median=', max(round(recent_30_eleme_subsidy_median,3))),
            concat('recent_30_eleme_subsidy_stddev=', max(round(recent_30_eleme_subsidy_stddev,3))),
            concat('recent_30_order_chargeback_scale=', max(round(recent_30_order_chargeback_scale,3))),
            concat('recent_30_order_unpaid_cnt=', max(recent_30_order_unpaid_cnt)),
            concat('recent_30_order_unpaid_scale=', max(round(recent_30_order_unpaid_scale,3))),
            concat('recent_30_order_hongbao_preferential_avg=', max(round(recent_30_order_hongbao_preferential_avg,3))),
            concat('recent_30_order_hongbao_preferential_stddev=', max(round(recent_30_order_hongbao_preferential_stddev,3)))
        ) as info_array
    from temp.temp_mdl_restaurant_recent_30_trade
    group by 
        restaurant_id
) t
lateral view explode(t.info_array) tmp as item
where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0

union all
select t.restaurant_id, 'trade' as top_category, split(item,'=')[0] as attr_key, 
    split(item,'=')[1] as attr_value, 
    1 as is_json, 
    '${day}' as update_time
from(
    select restaurant_id,
        array(
            concat('order_date_prefer={',concat(
                                                '\"workday_order_cnt\":',workday_order_cnt,',',
                                                '\"weekend_order_cnt\":',weekend_order_cnt,',',
                                                '\"workday_sales_amt\":',round(workday_sales_amt,2),',',
                                                '\"weekend_sales_amt\":',round(weekend_sales_amt,2),',',
                                                '\"workday_order_scale\":',round(workday_order_cnt/(workday_order_cnt+weekend_order_cnt),3),',',
                                                '\"weekend_order_scale\":',round(weekend_order_cnt/(workday_order_cnt+weekend_order_cnt),3),',',
                                                '\"workday_sales_scale\":',round(workday_sales_amt/(workday_sales_amt+weekend_sales_amt),3),',',
                                                '\"weekend_sales_scale\":',round(weekend_sales_amt/(workday_sales_amt+weekend_sales_amt),3)),'}'),

            concat('order_time_prefer={',concat(
                                                '\"lunch_order_cnt\":',lunch_order_cnt,',',
                                                '\"tea_order_cnt\":',tea_order_cnt,',',
                                                '\"supper_order_cnt\":',supper_order_cnt,',',
                                                '\"night_order_cnt\":',night_order_cnt,',',
                                                '\"other_order_cnt\":',other_order_cnt,',',
                                                '\"lunch_sales_amt\":',round(lunch_sales_amt,2),',',
                                                '\"tea_sales_amt\":',round(tea_sales_amt,2),',',
                                                '\"supper_sales_amt\":',round(supper_sales_amt,2),',',
                                                '\"night_sales_amt\":',round(night_sales_amt,2),',',
                                                '\"other_sales_amt\":',round(other_sales_amt,2),',',
                                                '\"lunch_order_scale\":',round(lunch_order_cnt/(lunch_order_cnt+tea_order_cnt+supper_order_cnt+night_order_cnt+other_order_cnt),3),',',
                                                '\"tea_order_scale\":',round(lunch_order_cnt/(lunch_order_cnt+tea_order_cnt+supper_order_cnt+night_order_cnt+other_order_cnt),3),',',
                                                '\"supper_order_scale\":',round(lunch_order_cnt/(lunch_order_cnt+tea_order_cnt+supper_order_cnt+night_order_cnt+other_order_cnt),3),',',
                                                '\"nigth_order_scale\":',round(lunch_order_cnt/(lunch_order_cnt+tea_order_cnt+supper_order_cnt+night_order_cnt+other_order_cnt),3),',',
                                                '\"other_order_scale\":',round(lunch_order_cnt/(lunch_order_cnt+tea_order_cnt+supper_order_cnt+night_order_cnt+other_order_cnt),3),',',
                                                '\"lunch_sales_scale\":',round(lunch_sales_amt/(lunch_sales_amt+tea_sales_amt+supper_sales_amt+night_sales_amt+other_sales_amt),3),',',
                                                '\"tea_sales_scale\":',round(tea_sales_amt/(lunch_sales_amt+tea_sales_amt+supper_sales_amt+night_sales_amt+other_sales_amt),3),',',
                                                '\"supper_sales_scale\":',round(supper_sales_amt/(lunch_sales_amt+tea_sales_amt+supper_sales_amt+night_sales_amt+other_sales_amt),3),',',
                                                '\"night_sales_scale\":',round(night_sales_amt/(lunch_sales_amt+tea_sales_amt+supper_sales_amt+night_sales_amt+other_sales_amt),3),',',
                                                '\"other_sales_scale\":',round(other_sales_amt/(lunch_sales_amt+tea_sales_amt+supper_sales_amt+night_sales_amt+other_sales_amt),3)),'}')
        ) as info_array
    from temp.temp_mdl_restaurant_long_term_trade
) t
lateral view explode(t.info_array) tmp as item
where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0

union all
select 
    t.restaurant_id, 
    'trade' as top_category, 
    split(item,'=')[0] as attr_key, 
    split(item,'=')[1] as attr_value, 
    1 as is_json, 
    '${day}' as update_time
from(
    select 
        restaurant_id,
        array(
            concat('food_top_10=',food_top_10)
        ) as info_array
    from 
        temp.temp_mdl_restaurant_long_term_trade_rank
) t
lateral view explode(t.info_array) tmp as item
where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0
; 




-- sub task : import data into ups
insert overwrite table dm.dm_ups_restaurant_item_info partition(dt='${day}', flag='trade')
select *
from temp.temp_restaurant_trade_info_from_trade_info;

insert overwrite table dm.dm_ups_restaurant_item_info partition(dt='3000-12-31', flag='trade')
select *
from temp.temp_restaurant_trade_info_from_trade_info;