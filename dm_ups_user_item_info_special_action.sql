#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_special_action.sql
# ** 功能描述： 下单未支付的用户信息
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-10-17
#***************************************************************************************************


-- sub task 1: 计算每个用户下单未支付情况
drop table temp.temp_mdl_user_order_non_payment;
create table temp.temp_mdl_user_order_non_payment as 
    select 
        user_id,
        concat('{',
            concat_ws(',',
                concat('"order_date_last":', '"', order_date_last, '"'),
                concat('"order_non_payment_cnt":', '"', order_non_payment_cnt, '"'),
                concat('"eleme_order_total_max":', '"', eleme_order_total_max, '"'),
                concat('"eleme_order_total_avg":', '"', eleme_order_total_avg, '"'),
                concat('"total_max":', '"', total_max, '"'),
                concat('"total_avg":', '"', total_avg, '"'),
                concat('"cut_money_max":', '"', cut_money_max, '"'),
                concat('"cut_money_avg":', '"', cut_money_avg, '"')
                ),
            '}'
            ) as non_payment_info
    from(
        select
            user_id,
            max(order_date) as order_date_last,
            count(distinct id) as order_non_payment_cnt,
            max(eleme_order_total) as eleme_order_total_max,
            avg(eleme_order_total) as eleme_order_total_avg,
            max(total) as total_max,
            avg(total) as total_avg,
            max(cut_money) as cut_money_max,
            avg(cut_money) as cut_money_avg
        from 
            dw.dw_trd_order_wide
        where 
            dt='${day}' and 
            status_code=-5
        group by 
            user_id
    ) t
;



-- sub task 2: 最近未下单的天数
drop table temp.temp_mdl_user_order_last_date;
create table temp.temp_mdl_user_order_last_date as
    select
        user_id,
        datediff('${day}',last_order_date) as unpaid_period
    from (
        select
            user_id,
            max(order_date) as last_order_date
        from 
            dw.dw_trd_order_wide
        where 
            dt='${day}' and
            order_status=1
        group by 
            user_id
        ) t
;


-- sub task 3: 餐厅专注度
drop table temp.temp_mdl_user_order_restaurant_concentration;
create table temp.temp_mdl_user_order_restaurant_concentration as 
    select
        user_id,
        1-t.ratio as rst_concentration_index
    from(
        select
            user_id,
            count(distinct restaurant_id)/count(distinct id) as ratio
        from
            dw.dw_trd_order_wide
        where 
            dt='${day}' and
            order_status=1
        group by 
            user_id
    ) t
;


-- sub task 4: 用户下单实际付款价格浮动指数
drop table temp.temp_mdl_user_order_payment_price_flactuation_index;
create table temp.temp_mdl_user_order_payment_price_flactuation_index as
    select
        user_id,
        stddev(if(datediff('${day}',order_date)<8,eleme_order_total,0)) as flac_index_7,
        stddev(if(datediff('${day}',order_date)<31,eleme_order_total,0)) as flac_index_30
    from
        dw.dw_trd_order_wide
    where 
        dt='${day}' and
        order_status=1
    group by 
        user_id
;


--sub task 5: 用户接受配送的均值以及浮动指数
drop table temp.temp_mdl_user_order_deliver_time;
create table temp.temp_mdl_user_order_deliver_time as 
    select
        user_id,
        t.deliver_time_avg/60 as deliver_time_index_avg,
        t.deliver_time_stddev/60 as deliver_time_index_stddev
    from(
        select
            user_id,
            avg(unix_timestamp(settled_at)-unix_timestamp(active_at)) as deliver_time_avg,
            stddev(unix_timestamp(settled_at)-unix_timestamp(active_at)) as deliver_time_stddev
        from 
            dw.dw_trd_order_wide
        where
            dt='${day}' and
            order_status=1
        group by
            user_id
            ) t
;



-- sub task 6: import data into ups
insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='trd_info_jiahao')
    select 
        user_id, 
        'trd' as top_category, 
        'order_non_payment' as attr_key, 
        non_payment_info as attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_user_order_non_payment

    union all
    select
        user_id,
        'trd' as top_category,
        'unpaid_period' as attr_key,
        unpaid_period as attr_value,
        '0' as is_json,
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_user_order_last_date

    union all
    select
        user_id,
        'trd' as top_category,
        'rst_concentration_index' as attr_key,
        round(rst_concentration_index,2) as attr_value,
        '0' as is_json,
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_user_order_restaurant_concentration

    union all
    select 
        t.user_id, 
        'trd' as top_category, 
        split(item,'=')[0] as attr_key, 
        split(item,'=')[1] as attr_value, 
        '0' as is_json, 
        '${day}' as update_time
    from(
        select 
            user_id,
            array(
                concat('flac_index_7=', round(flac_index_7,2)),
                concat('flac_index_30=', round(flac_index_30,2))
            ) as info_array
        from temp.temp_mdl_user_order_payment_price_flactuation_index
    ) t
    lateral view explode(t.info_array) tmp as item
    where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0

    union all
    select 
        t.user_id, 
        'trd' as top_category, 
        split(item,'=')[0] as attr_key, 
        split(item,'=')[1] as attr_value, 
        '0' as is_json, 
        '${day}' as update_time
    from(
        select 
            user_id,
            array(
                concat('deliver_time_index_avg=', round(deliver_time_index_avg,2)),
                concat('deliver_time_index_stddev=', round(deliver_time_index_stddev,2))
            ) as info_array
        from temp.temp_mdl_user_order_deliver_time
    ) t
    lateral view explode(t.info_array) tmp as item
    where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0
;


insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='trd_info_jiahao')
    select 
        user_id, 
        top_category, 
        attr_key, 
        attr_value, 
        is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        dm.dm_ups_user_item_info
    where 
        dt='${day}' and
        flag='trd_info_jiahao'
;


