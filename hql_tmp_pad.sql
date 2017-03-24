


select
        user_id,
        parse_json_object(profile_json,'base.email',false) as email,
        parse_json_object(profile_json,'base.gender',false) as gender,
        parse_json_object(profile_json,'base.nick_name',false) as nick_name,
        parse_json_object(profile_json,'base.pre_phone',false) as pre_phone,
        parse_json_object(profile_json,'base.create_time',false) as create_time,
        parse_json_object(profile_json,'base.source',false) as source,
        parse_json_object(profile_json,'device.brand',false) as brand,
        parse_json_object(profile_json,'device.os_platform',false) as os_platform,
        parse_json_object(profile_json,'device.os_version',false) as os_version,
        parse_json_object(profile_json,'device.model',false) as model,
        parse_json_object(profile_json,'device.id',false) as id,
        parse_json_object(profile_json,'device.resolution',false) as resolution,
        parse_json_object(profile_json,'rec.category_prefer',false) as category_prefer,
        parse_json_object(profile_json,'rec.hongbao_sensitive',false) as hongbao_sensitive,
        parse_json_object(profile_json,'rec.price_sensitive',false) as price_sensitive,
        parse_json_object(profile_json,'rec.style_prefer',false) as style_prefer,
        parse_json_object(profile_json,'rec.flavor_prefer',false) as flavor_prefer,
        parse_json_object(profile_json,'rec.food_prefer',false) as food_prefer,
        parse_json_object(profile_json,'log.visit_address',false) as visit_address,
        parse_json_object(profile_json,'log.last_visit_time',false) as last_visit_time,
        parse_json_object(profile_json,'log.visit_time_prefer',false) as visit_time_prefer,
        parse_json_object(profile_json,'log.visit_date_prefer',false) as visit_date_prefer,
        parse_json_object(profile_json,'log.visit_province_id',false) as visit_province_id,
        parse_json_object(profile_json,'log.first_visit_time',false) as first_visit_time,
        parse_json_object(profile_json,'log.visit_city_id',false) as visit_city_id,
        parse_json_object(profile_json,'trd.order_subsidy',false) as order_subsidy,
        parse_json_object(profile_json,'trd.order_address',false) as order_address,
        parse_json_object(profile_json,'trd.first_order_time',false) as first_order_time,
        parse_json_object(profile_json,'trd.last_order_time',false) as last_order_time,
        parse_json_object(profile_json,'trd.order_date_prefer',false) as order_date_prefer,
        parse_json_object(profile_json,'trd.order_cnt',false) as order_cnt,
        parse_json_object(profile_json,'trd.consume_level',false) as consume_level,
        parse_json_object(profile_json,'trd.order_avg_fee',false) as order_avg_fee,
        parse_json_object(profile_json,'trd.order_province_id',false) as order_province_id,
        parse_json_object(profile_json,'trd.order_hongbao',false) as order_hongbao,
        parse_json_object(profile_json,'trd.order_bu_flag',false) as order_bu_flag,
        parse_json_object(profile_json,'trd.order_amt',false) as order_amt,
        parse_json_object(profile_json,'trd.point',false) as point,
        parse_json_object(profile_json,'trd.order_time_prefer',false) as order_time_prefer,
        parse_json_object(profile_json,'trd.order_city_id',false) as order_city_id,
        parse_json_object(profile_json,'speciality.is_sia',false) as is_sia,
        parse_json_object(profile_json,'base.phone_number',false) as phone_number,
        parse_json_object(profile_json,'trd.delivery_type_prefer',false) as delivery_type_prefer,
        parse_json_object(profile_json,'trd.recent_30_avg_delivery_fee',false) as recent_30_avg_delivery_fee,
        parse_json_object(profile_json,'trd.collect_rest_list',false) as collect_rest_list,
        parse_json_object(profile_json,'trd.click_rest_list',false) as click_rest_list,
        parse_json_object(profile_json,'trd.order_rest_list',false) as order_rest_list,
        parse_json_object(profile_json,'trd.score_rest_list',false) as score_rest_list,
        parse_json_object(profile_json,'trd.order_again_rest_list',false) as order_again_rest_list,
        parse_json_object(profile_json,'trd.recent_30_active_day',false) as recent_30_active_day,
        parse_json_object(profile_json,'trd.recent_30_play_time',false) as recent_30_play_time,
        parse_json_object(profile_json,'trd.recent_30_order_cnt',false) as recent_30_order_cnt,
        parse_json_object(profile_json,'trd.recent_30_order_amt',false) as recent_30_order_amt,
        parse_json_object(profile_json,'speciality.delivery_priority',false) as delivery_priority,

        parse_json_object(profile_json,'base.is_vip',false) as is_vip,
        parse_json_object(profile_json,'base.phone_city_id',false) as phone_city_id,
        parse_json_object(profile_json,'base.phone_city_name',false) as phone_city_name,
        parse_json_object(profile_json,'base.phone_province_id',false) as phone_province_id,
        parse_json_object(profile_json,'base.phone_province_name',false) as phone_province_name,
        parse_json_object(profile_json,'trd.hongbao_balance',false) as hongbao_balance,
        parse_json_object(profile_json,'trd.recent_7_reminder_order_num',false) as recent_7_reminder_order_num,
        parse_json_object(profile_json,'trd.recent_7_reminder_order_rate',false) as recent_7_reminder_order_rate,
        parse_json_object(profile_json,'trd.recent_7_withdraw_order_num',false) as recent_7_withdraw_order_num,
        parse_json_object(profile_json,'trd.recent_7_withdraw_order_rate',false) as recent_7_withdraw_order_rate,
        parse_json_object(profile_json,'trd.rest_distance_avg',false) as rest_distance_avg,
        parse_json_object(profile_json,'trd.order_discount_rate',false) as order_discount_rate,
        parse_json_object(profile_json,'trd.order_manjian_rate',false) as order_manjian_rate,
        parse_json_object(profile_json,'trd.recent_90_click_premium_rest_rate',false) as recent_90_click_premium_rest_rate,
        parse_json_object(profile_json,'trd.recent_90_visit_dish_per_rest_avg',false) as recent_90_visit_dish_per_rest_avg,
        parse_json_object(profile_json,'trd.recent_30_is_new',false) as recent_30_is_new,
        parse_json_object(profile_json,'trd.order_interval_avg',false) as order_interval_avg,
        parse_json_object(profile_json,'trd.order_interval_min',false) as order_interval_min,
        parse_json_object(profile_json,'trd.order_delivery_fee_rate',false) as order_delivery_fee_rate,
        parse_json_object(profile_json,'trd.click_rest_sale_avg',false) as click_rest_sale_avg,
        parse_json_object(profile_json,'trd.click_rest_open_time_avg',false) as click_rest_open_time_avg,
        parse_json_object(profile_json,'trd.click_rest_safety_level_avg',false) as click_rest_safety_level_avg,
        parse_json_object(profile_json,'trd.click_rest_score_avg',false) as click_rest_score_avg,
        parse_json_object(profile_json,'trd.click_rest_desc_length_avg',false) as click_rest_desc_length_avg,
        parse_json_object(profile_json,'trd.click_rest_delivery_time_avg',false) as click_rest_delivery_time_avg,
        parse_json_object(profile_json,'trd.click_rest_has_picture_rate',false) as click_rest_has_picture_rate,
        parse_json_object(profile_json,'trd.click_rest_discount_avg',false) as click_rest_discount_avg,
        parse_json_object(profile_json,'rec.rest_order_cat0_prefer',false) as rest_order_cat0_prefer,
        parse_json_object(profile_json,'rec.rest_order_cat1_prefer',false) as rest_order_cat1_prefer,
        parse_json_object(profile_json,'rec.rest_behavior',false) as rest_behavior,
        parse_json_object(profile_json,'rec.cat_profile',false) as cat_profile,
        parse_json_object(profile_json,'rec.rest_prefer',false) as rest_prefer
  from
         dm.dm_ups_user_info
        where
            dt='3000-12-31' and user_id=44099856
    ;

select 
    sum(case when length(parse_json_object(profile_json, '$.base.create_time')) > 8 then 0 else 1 end) as error_cnt
from 
    dm.dm_ups_user_info
where 
    dt = '3000-12-31'
;


select 
    user_id,
    profile_json,
    sum(case when length(parse_json_object(profile_json, '$.base.create_time')) > 8 then 1 else 0 end) as num
from
    dm.dm_ups_user_info
where 
    dt = '3000-12-31'
group by 
    user_id,
    profile_json
having 
    num = 0
limit 
    10
;

select
    attr_value,
    regexp_replace(attr_value, '\"|\t|\\\\', '')
from 
    dm.dm_ups_user_item_info
where 
    dt='3000-12-31' and
    user_id=15076227 and 
    attr_key='nick_name'
;

select
                user_id,
                count(*) as cnt
            from
                dw.dw_log_app_pv_day_inc
            where
                dt>=get_date('${day}',-8) and
                user_id is not null
            group by
                user_id
            sort by
                cnt desc




select
    count(*) as total_cnt,
    sum(case when t2.user_id is not null then 1 else 0 end) as hit_cnt,
    sum(case when t2.user_id is not null then 1 else 0 end)/count(*) as hit_rate
from(
    select
        user_id
    from
        dw.dw_log_app_pv_day_inc
    where
        dt='2017-02-15'
    ) t1
left outer join(
    select
        t.user_id
    from(
        select
            user_id,
            sum(100/datediff('2017-02-15', dt)) as cnt
        from
            dw.dw_log_app_pv_day_inc
        where
            dt>=get_date('2017-02-14',-10) and
            dt<'2017-02-15' and
            user_id is not null
        group by
            user_id
        sort by
            cnt desc
        ) t
    limit 
        6000000
    ) t2
on (
    t1.user_id = t2.user_id
    )
;




select
    count(*) as total_cnt,
    count(distinct t1.user_id) as dist_user_cnt,
    count(distinct t2.user_id) as 
    sum(case when t2.user_id is not null then 1 else 0 end) as hit_cnt,
    sum(case when t2.user_id is not null then 1 else 0 end)/count(*) as hit_rate
from(
    select
        user_id
    from
        dw.dw_log_app_pv_day_inc
    where
        dt='2017-02-17'
    ) t1
left outer join(
    select
        t.user_id
    from(
        select
            user_id,
            count(*) as cnt
        from
            dw.dw_log_app_pv_day_inc
        where
            dt>=get_date('2017-02-16',-1) and
            dt<'2017-02-17' and
            user_id is not null
        group by
            user_id
        sort by
            cnt desc
        ) t
    limit
        6000000
    ) t2
on (
    t1.user_id = t2.user_id
    )
;




select
    count(*) as total_cnt,
    sum(case when t2.user_id is not null then 1 else 0 end) as hit_cnt,
    sum(case when t2.user_id is not null then 1 else 0 end)/count(*) as hit_rate
from(
    select
        user_id
    from
        dw.dw_log_app_pv_day_inc
    where
        dt='2017-02-15'
    ) t1
left outer join(
    select
        user_id,
        datediff(attr_value,'2017-02-15') as cnt
    from
        dm.dm_ups_user_item_info
    where 
        dt='2017-02-15' and 
        attr_key='last_order_time' 
    sort by 
        cnt 
    limit 
        6000000
    ) t2
on (
    t1.user_id = t2.user_id
    )
;






select 
    count(t1.user_id) as total,
    sum(case when t2.user_id is not null then 1 else 0 end) as hit,
    sum(case when t2.user_id is not null then 1 else 0 end)/count(t1.user_id) as hit_rate
from (
    select 
        distinct user_id
    from 
        dw.dw_log_app_pv_day_inc
    where 
        dt = '2017-02-19'
) t1
join (
    select
        distinct user_id
    from 
        dm.dm_ups_user_info
    where
        dt = '2017-02-17'
) t2
on(
    t1.user_id = t2.user_id
)
;



select
    taker_id,
    attr_key,
    count(*) as num
from
    dm.dm_ups_taker_item_info
where 
    dt='3000-12-31'
group by 
    taker_id,
    attr_key
having 
    num>1
limit 10;




SELECT
    a.user_id AS id,
    'dm_ups_food_item_info' AS dq_table,
    '${day}' AS date_at,
    'jiahao.dong' AS dq_man,
    'multi-food_id:top_category:attr_key' AS dq_type,
    'food_id, top_category, attr_key' AS dq_column,
    1 AS is_error,
    'multi-food_id:top_category:attr_key exists' AS  error_info,
    'error' AS error_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
FROM
(
    SELECT
        food_id, top_category, attr_key, COUNT(*) AS number
    FROM
        dm.dm_ups_food_item_info
    WHERE
        dt = '3000-12-31'
    GROUP BY
        food_id, top_category, attr_key
) a
WHERE
    a.number > 1
LIMIT 1;



select *
from dm.dm_ups_restaurant_info
where dt='3000-12-31' and parse_json_object(profile_json,'base.create_time',false) is null
limit 100;


select sum(case when parse_json_object(profile_json,'base.create_time',false) is null then 1 else 0 end)
from dm.dm_ups_restaurant_info
where dt='3000-12-31'
;


select * from dm.dm_ups_user_info where parse_json_object(profile_json,'base.phone_number',false)=18721780518 and dt='3000-12-31';


select sum(if (merchant_customer_walk_distance>0 or merchant_customer_walk_distance is not null, 1, 0)), count(*) from dm.dm_tms_apollo_waybill_wide_detail where dt='2017-03-15';

select merchant_customer_walk_distance from dm.dm_tms_apollo_waybill_wide_detail where dt='2017-03-15';

CREATE TABLE rec.dm_ups_taker_info (
    taker_id bigint PRIMARY KEY,
    city_id int,
    created_at text,
    delivery_mode text,
    geo_name text,
    max_rst_count_per_deliver int,
    team_id bigint,
    team_name text,
    org_id int,
    team_created_at text,
    team_load int,
    team_status int,

    period_deliver_distance_avg text,
    period_fetch_distance_avg text,
    period_deliver_time_avg text,
    period_deliver_speed_avg text,
    recent_14_deliver_speed_trend text,
    fetch_speed_weighted_avg float,
    team_deliver_speed_rank int,

    rest_level_distribution text,
    recent_30_order_cnt int,
    recent_30_date_cnt int,
    day_peak_order_cnt_avg float,
    day_peak_order_cnt_std float,
    night_peak_order_cnt_avg float,
    night_peak_order_cnt_std float,
    ontime_order_rate float,
    day_peak_ontime_rate float,
    night_peak_ontime_rate float,
    recent_14_day_peak_order_cnt_trend text,
    recent_14_night_peak_order_cnt_trend text,
    recent_14_ontime_trend text
    
) WITH bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 172800
    AND gc_grace_seconds = 1800
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
    

insert overwrite local directory '/home/dev/jiahao.dong'
    select
        concat(
        concat('restaurant_id','\003',if(restaurant_id is not null, restaurant_id ,'null')),'\001',
        concat('restaurant_id','\003',if(restaurant_id is not null, restaurant_id ,'null')),'\002',
        concat('latitude','\003',if(latitude is not null, latitude ,'null')),'\002',
        concat('longitude','\003',if(longitude is not null, longitude ,'null')),'\002',
        concat('geohash','\003',if(geohash is not null, geohash ,'null')),'\002',
        concat('city_id','\003',if(city_id is not null, city_id ,'null')),'\002',
        concat('cat0_name','\003',if(cat0_name is not null, cat0_name ,'null')),'\002',
        concat('recent_7_order_amt','\003',if(recent_7_order_amt is not null, recent_7_order_amt ,'null')),'\002',
        concat('food_price_avg','\003', if(food_price_avg is not null, food_price_avg, 'null'))
        )
    from(
        select
            case when restaurant_id is not null then concat('2:', restaurant_id) else null end as restaurant_id,
            parse_json_object(profile_json,'base.latitude',false) as latitude,
            parse_json_object(profile_json,'base.longitude',false) as longitude,
            parse_json_object(profile_json,'base.geohash',false) as geohash,
            parse_json_object(profile_json,'base.city_id',false) as city_id,
            parse_json_object(profile_json,'category.cat0_name',false) as cat0_name,
            parse_json_object(profile_json,'trade.recent_7_order_amt',false) as recent_7_order_amt,
            parse_json_object(profile_json,'base.food_price_avg',false) as food_price_avg
        from
            dm.dm_ups_restaurant_info
        where 
            dt='3000-12-31' and
            restaurant_id is not null
    ) t
    where recent_7_order_amt is not null
    limit 1000
;



