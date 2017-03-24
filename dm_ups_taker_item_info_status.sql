
#***************************************************************************************************
# ** 文件名称： dm_ups_taker_item_info_status.sql
# ** 功能描述： 骑手画像
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-03-15
#***************************************************************************************************


-- 有效成单订单定义:
-- -- shipping_state = 1 and is_valid = 1 
-- 时段
-- -- 10~14 and 16~20

-- 
drop table temp.temp_ups_taker_order_cnt_info_part_1;
create table temp.temp_ups_taker_order_cnt_info_part_1 as
    select
        taker_id,
        round(avg(t.day_peak_ontime_order_cnt),1) as day_peak_order_cnt_avg,
        round(avg(t.night_peak_ontime_order_cnt),1) as night_peak_order_cnt_avg,
        round(stddev(t.day_peak_ontime_order_cnt),1) as day_peak_order_cnt_std,
        round(stddev(t.night_peak_ontime_order_cnt),1) as night_peak_order_cnt_std,
        round(sum(day_peak_ontime_order_cnt)/sum(day_peak_order_cnt),3) as day_peak_ontime_rate,
        round(sum(night_peak_ontime_order_cnt)/sum(night_peak_order_cnt),3) as night_peak_ontime_rate
    from(
        select
            taker_id,
            dt,
            sum(if(hour(created_at)>10 and hour(created_at)<14, 1, 0)) as day_peak_order_cnt,
            sum(if(hour(created_at)>16 and hour(created_at)<20, 1, 0)) as night_peak_order_cnt,
            sum(if(hour(created_at)>10 and hour(created_at)<14 and is_overtime=0, 1, 0)) as day_peak_ontime_order_cnt,
            sum(if(hour(created_at)>16 and hour(created_at)<20 and is_overtime=0, 1, 0)) as night_peak_ontime_order_cnt
        from 
            dm.dm_tms_apollo_waybill_wide_detail
        where 
            dt>=get_date('2017-03-16',-30) and
            carrier_id=5 and
            taker_id!=0 and 
            taker_id is not null and
            shipping_state=40 and
            is_valid=1
        group by 
            taker_id,
            dt
        having 
            day_peak_order_cnt>0 and
            night_peak_order_cnt>0
            ) t
    group by
        taker_id
;

drop table temp.temp_ups_taker_order_cnt_info_part_2;
create table temp.temp_ups_taker_order_cnt_info_part_2 as
    select
        taker_id,
        count(distinct order_date) as date_cnt,
        sum(if(is_overtime=0 , 1, 0)) as ontime_order_cnt,
        count(*) as order_cnt,
        round(sum(if(is_overtime=0 , 1, 0))/count(*),3) as ontime_order_rate
    from
        dm.dm_tms_apollo_waybill_wide_detail
    where
        dt>=get_date('${day}',-30) and
        -- carrier_id=5 and
        taker_id != 0 and 
        taker_id is not null and
        shipping_state=40 and
        is_valid=1
    group by
        taker_id
;

insert overwrite table dm.dm_ups_taker_item_info partition(dt='${day}', flag='status_001')
    select
        taker_id,
        'status' as top_category,
        split(item,'=')[0] as attr_key, 
        split(item,'=')[1] as attr_value, 
        '0' as is_json,
        '${day}' as update_time
    from(
        select
            taker_id,
            array(
                concat('day_peak_order_cnt_avg=', day_peak_order_cnt_avg),
                concat('night_peak_order_cnt_avg=', night_peak_order_cnt_avg),
                concat('day_peak_order_cnt_std=', day_peak_order_cnt_std),
                concat('night_peak_order_cnt_std=', night_peak_order_cnt_std)
                ) as info_array
        from
            temp.temp_ups_taker_order_cnt_info_part_1
        ) t
    lateral view explode(t.info_array) tmp as item
    where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0

    union all
    select
        taker_id,
        'status' as top_category,
        split(item,'=')[0] as attr_key, 
        split(item,'=')[1] as attr_value, 
        '0' as is_json,
        '${day}' as update_time
    from(
        select
            taker_id,
            array(
                concat('ontime_order_rate=', ontime_order_rate),
                concat('recent_30_order_cnt=', order_cnt),
                concat('recent_30_date_cnt=', date_cnt)
                ) as info_array
        from
            temp.temp_ups_taker_order_cnt_info_part_2
        ) t
    lateral view explode(t.info_array) tmp as item
    where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0
;

insert overwrite table dm.dm_ups_taker_item_info partition(dt='3000-12-31', flag='status_001')
    select
        taker_id,
        top_category,
        attr_key,
        attr_value,
        is_json,
        update_time
    from
        dm.dm_ups_taker_item_info
    where 
        dt='${day}' and 
        flag='status_001'
;




