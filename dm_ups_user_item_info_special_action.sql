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


-- sub task 2: import data into ups
insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='order_action')
    select 
        user_id, 
        'trd' as top_category, 
        'order_non_payment' as attr_key, 
        non_payment_info as attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_user_order_non_payment
;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='order_action')
    select 
        user_id, 
        'trd' as top_category, 
        'order_non_payment' as attr_key, 
        non_payment_info as attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_mdl_user_order_non_payment
;


