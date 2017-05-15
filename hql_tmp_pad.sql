


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
    1000
;

select 
    user_id,
    profile_json,
    parse_json_object(profile_json, '$.trd.new_retail_type') as new_retail_type
from
    dm.dm_ups_user_info
where 
    dt = '3000-12-31'
LIMIT
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



insert overwrite local directory '/home/etl/jiahao.dong'
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


insert overwrite local directory '/home/etl/jiahao.dong'
    select
        concat(
            concat('user_id','\003',if(user_id is not null, user_id ,'null')),'\001',
            concat('user_id','\003',if(user_id is not null, user_id ,'null')),'\002',
            concat('retail_user_type','\003',if(retail_user_type is not null, retail_user_type ,'null')),'\002',
            concat('workday_order_address_geohash','\003',if(workday_order_address_geohash is not null, workday_order_address_geohash ,'null')),'\002',
            concat('weekend_order_address_geohash','\003',if(weekend_order_address_geohash is not null, weekend_order_address_geohash ,'null')),'\002',
            concat('order_date_prefer','\003',if(order_date_prefer is not null, order_date_prefer ,'null'))
        )
    from(
        select
            user_id,
            parse_json_object(profile_json,'trd.retail_user_type',false) as retail_user_type,
            parse_json_object(profile_json,'trd.order_address.workday.geohash',false) as workday_order_address_geohash,
            parse_json_object(profile_json,'trd.order_address.weekend.geohash',false) as weekend_order_address_geohash,
            transfer_json_kv_format(parse_json_object(profile_json,'trd.order_date_prefer')) as order_date_prefer
        from
            dm.dm_ups_user_info_inc
        where 
            dt='3000-12-31' and
            user_id is not null
        -- limit
        --     1000
    ) t
    -- where last_order_time is not null
    limit 1000
;




curl -i -XGET 'http://192.168.114.66:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty'

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/36497412?pretty'

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_mapping'

curl -i -XDELETE 'http://10.200.3.143:9200/bdi_eco_data_index/' 


curl -i -XGET 'http://192.168.114.66:9200/bdi_eco_data_index_1/test_1/_mapping?pretty'
curl -i -XGET 'http://192.168.114.66:9200/bdi_eco_data_index/_stats'
curl -i -PUT 'http://192.168.114.66:9200/bdi_eco_data_index_1/_mapping/test_1' -d '
{
    "properties": {
    "order_date_prefer": {
      "type": "nested", 
      "properties": {
        "key": { "type": "string"  },
        "value": { "type": "double" }
      }
    }
  }
}
'


curl -i -PUT 'http://192.168.114.66:9200/bdi_eco_data_index_1/' -d '
{
    "settings" : {
        "index" : {
            "number_of_shards" : 3,
            "number_of_replicas" : 2
        }
    }
}
'

curl -i -XDELETE 'http://192.168.114.66:9200/bdi_eco_data_index_1/' 

curl -i -XDELETE 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_query' -d '{
    "query" : { 
        "match_all" : {}
    }
}
'

curl -i -PUT 'http://10.200.3.143:9200/bdi_eco_data_index/_mapping/eco_user_type' -d '
{
  "properties": {
    "order_date_prefer": {
      "type": "nested", 
      "properties": {
        "key": { "type": "string"  },
        "value": { "type": "double" }
      }
    }
  }
}
' 


curl -i -PUT 'http://10.200.3.143:9200/bdi_eco_data_index_test' -d '
{
  "mappings": {
    "eco_user_type_test": {
      "properties": {
        "order_date_prefer": {
          "type": "nested", 
          "properties": {
            "key":    { "type": "string"  },
            "value":     { "type": "double" }
          }
        }
      }
    }
  }
}
' 

curl -i -XGET 'http://192.168.114.66:9200/bdi_eco_data_index_test/eco_user_type_test/_mapping?pretty'

curl -i -PUT 'http://192.168.114.66:9200/bdi_eco_data_index_test' -d '
{
  "mappings": {
    "eco_user_type_test": {
      "properties": {
        "order_date_prefer": {
          "type": "nested", 
          "properties": {
            "key":    { "type": "string"  },
            "value":     { "type": "double" }
          }
        }
      }
    }
  }
}
' 

curl -i -PUT 'http://192.168.114.66:9200/bdi_eco_data_index_test/_mapping/eco_user_type_test' -d '
{
  "properties": {
    "retail_user_type": {
      "type": "long"
    }
  }
}
' 

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_count?pretty' -d '
{
    "query":{
        "bool":{
            "must":{
                "match":{"order_city_name":"NULL"}
            }
        }
    }
}
'

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_count?pretty' -d '
{
    "query":{
        "bool":{
            "must":{
                "match":{"retail_user_type":"0"}
            }
        }
    }
}
'


curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_count?pretty' -d '
{
    "query": {
        "nested":{
            "path" : "order_date_prefer",
            "query" : {
                "bool" : {
                    "must" : [
                        {
                            "match" : {"order_date_prefer.key" : "weekend"}
                        },
                        {
                            "range" : {"order_date_prefer.value" : {"gt" : 0.1}}
                        }
                    ]
                }
            }
        }
    }
}
' 

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_search' -d '
{
    "query":{
        "filtered" : {
            "filter" : {
                 "limit" : {"value" : 100}
             }
        }
    }
}
'


curl -i -XGET 'http://10.200.3.143:9200/dongjiahao_index/dongjiahao_type/1'



curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty'

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_mapping?pretty'
curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index_test/eco_user_type_test/_mapping?pretty'

curl -i -PUT 'http://10.200.3.143:9200/bdi_eco_data_index_test/_mapping/eco_user_type_test' -d '
{
  "properties": {
    "order_date_prefer": {
      "type": "nested", 
      "properties": {
        "key": { "type": "string"  },
        "value": { "type": "double" }
      }
    }
  }
}
' 

curl -i -PUT 'http://10.200.3.143:9200/bdi_eco_data_index_test/_mapping/eco_user_type_test' -d '
{
  "properties": {
    "retail_user_type": {
      "type": "long"
    }
  }
}
' 

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty' -d '
{
    "query": {  
        "bool" : {
            "must" : [
                {
                    "match" : {"retail_user_type" : 3}
                }
            ]
        }
    }
}
'


curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty' -d '
{
    "query": {
        "nested":{
            "path" : "order_date_prefer",
            "query" : {
                "bool" : {
                    "must" : [
                        {
                            "match" : {"order_date_prefer.key" : "weekend"}
                        },
                        {
                            "range" : {"order_date_prefer.value" : {"gt" : 0.9}}
                        }
                    ]
                }
            }
        }
    }
}
' 




curl -i -XGET 'http://192.168.114.66:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty' -d '
{
    "query": {
        "nested":{
            "path" : "order_date_prefer",
            "query" : {
                "bool" : {
                    "must" : [
                        {
                            "match" : {"order_date_prefer.key" : "weekend"}
                        },
                        {
                            "range" : {"order_date_prefer.value" : {"gt" : 0.5}}
                        },
                        {
                            "range" : {"order_date_prefer.value" : {"lt" : 1.0}}
                        }
                    ]
                }
            }
        }
    }
}
'


curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty' -d '
{
    "query": {
        "nested":{
            "path" : "order_date_prefer",
            "query" : {
                "bool" : {
                    "must" : [
                        {
                            "match" : {"order_date_prefer.key" : "weekend"}
                        },
                        {
                            "range" : {"order_date_prefer.value" : {"gt" : 0.5}}
                        },
                        {
                            "range" : {"order_date_prefer.value" : {"lt" : 1.0}}
                        }
                    ]
                }
            }
        }
    }
}
'



curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty'

curl -i -XGET 'http://192.168.114.66:9200/bdi_eco_data_index_test/eco_user_type_test/_mapping?pretty'

curl -i -PUT 'http://10.200.3.143:9200/bdi_eco_data_index_test/_mapping/eco_user_type_test' -d '
{
  "properties": {
    "order_date_prefer": {
      "type": "nested", 
      "properties": {
        "key": { "type": "string"  },
        "value": { "type": "double" }
      }
    }
  }
}
' 

curl -i -PUT 'http://10.200.3.143:9200/bdi_eco_data_index_test/_mapping/eco_user_type_test' -d '
{
  "properties": {
    "retail_user_type": {
      "type": "long"
    }
  }
}
' 

curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index/eco_user_type/_count?pretty' -d '
{
    "query": {  
        "bool" : {
            "must" : [
                {
                    "match" : {"retail_user_type" : 1}
                }
            ]
        }
    }
}
'


curl -i -XGET 'http://10.200.3.143:9200/bdi_eco_data_index_test/eco_user_type_test/_count?pretty' -d '
{
    "query": {
        "nested":{
            "path" : "order_date_prefer",
            "query" : {
                "bool" : {
                    "must" : [
                        {
                            "match" : {"order_date_prefer.key" : "weekend"}
                        },
                        {
                            "range" : {"order_date_prefer.value" : {"gt" : 0.9}}
                        }
                    ]
                }
            }
        }
    }
}
' 






select 
    * 
from 
    dm.dm_ups_restaurant_info 
where 
    dt='3000-12-31' and 
    parse_json_object(profile_json,'trade.recent_30_user_cnt',false) >0 
limit 100
;






select 
    dom_id,
    to_value as dianping_poi_id
from
    rec.rec_dom_poi_change_record
where 
    dt = '2017-03-30' and
    field_name = 'dianping_poi_id' and
    to_value !=0 and 
    to_value is not null
;

 





SELECT
    t.restaurant_id,
    t.address,
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        t.address, '道口|大街|大道|路|街|道|巷|胡同|条|里', '^街道\\|'
                                        ), '大厦|广场|饭店|中心|大楼|场|广场|酒店|宾馆|市场|招待所|商铺|综合楼|soho|SOHO|Soho', '^POI\\|'
                                   ), '层', '^楼层\\|'
                                ), '小区|号院|园|村|坊|庄|居|苑|公寓|寓|墅|弄|公寓|花园', '^住宅\\|'
                            ), '号楼|楼|宿舍|斋|馆|堂', '^楼牌\\|'
                        ), '号|号铺', '^门牌\\|'
                    ), '区', '^行政区\\|'
                ), '市', '^行政市\\|'
            ), '省', '^行政省\\|')
FROM(
    SELECT
        restaurant_id,
        parse_json_object(profile_json, "base.address") as address
    FROM
        dm.dm_ups_restaurant_info
    WHERE
        dt='3000-12-31'
    ) t
LIMIT
    2000
;

SELECT
    t.restaurant_id,
    t.address,
    normalize_address(t.address)
FROM(
    SELECT
        restaurant_id,
        parse_json_object(profile_json, "base.address") as address
    FROM
        dm.dm_ups_restaurant_info
    WHERE
        dt='3000-12-31'
    ) t
LIMIT
    2000
;


SELECT
    a.ele_id,
    a.dianping_poi_id,
    a.geohash,
    b.restaurant_id as candidate_ele_id
FROM(
    SELECT
        t1.ele_id,
        t1.dianping_poi_id,
        t2.geohash
    FROM(
        SELECT
            ele_id,
            dianping_poi_id
        FROM
            test.rec_dom_input_data
        WHERE
            dt = '2017-03-30' and
            version = '+' and
            dianping_poi_id is not null and
            dianping_poi_id != 0
        ) t1
    JOIN(
        SELECT
            restaurant_id,
            substr(parse_json_object(profile_json, 'base.geohash', false), 0, 7) as geohash
        FROM
            dm.dm_ups_restaurant_info
        WHERE
            dt = '3000-12-31'
        ) t2
    ON(
        t1.ele_id = t2.restaurant_id
        )
    ) a 
JOIN(
    SELECT
        restaurant_id,
        substr(parse_json_object(profile_json, 'base.geohash', false), 0, 7) as geohash
    FROM
        dm.dm_ups_restaurant_info
    WHERE
        dt = '3000-12-31'
    ) b
ON(
    a.geohash = b.geohash
    )
SORT BY
    a.ele_id
LIMIT
    1000
;



source1                 int                     餐厅1来源
id1                     string                  餐厅1ID
source2                 int                     餐厅2来源
id2                     string                  餐厅2ID
distance                double                  餐厅经纬度直线距离
is_valid                int                     餐厅均有效：1，其他：0
phone_match             int                     餐厅电话有匹配：1，无匹配：0
name_simi_l             double                  餐厅名称levenshtein相似度，0~1
name_simi_c             double                  餐厅名称contain_match相似度，0~1
address_equal           int                     餐厅地址完全相同：1，其他：0
address_road_match      int                     餐厅地址XX路相同：1，其他：0
address_num_match       int                     餐厅地址门牌号相同：1，其他：0
dt                      string
version



INSERT OVERWRITE TABLE test.rec_dom_features partition (dt='2017-03-30',version='neg_dianping_test')
select 
    source1,
    id1,
    source2,
    id2,
    if(distance is null, 0, distance) as distance,
    if(is_valid is null, 0, is_valid) as is_valid,
    if(phone_match is null, 0, phone_match) as phone_match,
    if(name_simi_l is null, 0, name_simi_l) as name_simi_l,
    if(name_simi_c is null, 0, name_simi_c) as name_simi_c,
    if(address_equal is null, 0, address_equal) as address_equal,
    if(address_road_match is null, 0, address_road_match) as address_road_match,
    if(address_num_match is null, 0, address_num_match) as address_num_match
from
    test.rec_dom_features
where 
    dt='2017-03-30' and 
    version='neg_dianping'
;




DROP TABLE test.rec_dom_poi_change_record;
CREATE TABLE test.rec_dom_poi_change_record AS
select 
    dom_id,
    to_value as dianping_poi_id
from
    rec.rec_dom_poi_change_record
where 
    dt = '2017-03-31' and
    dom_id is not null and 
    dom_id > 0 and
    from_value is not null and
    from_value >0 and
    field_name = 'dianping_poi_id' and
    to_value !=0 and 
    to_value is not null
group by 
    dom_id,
    to_value
;

 


select
    tp,
    fp,
    fn,
    (tp+tn)/(tp+tn+fp+fn) as accuracy,
    tp/(tp+fp) as precision,
    tp/(tp+fn) as recall,
    tp/(tp+fp+fn) as boom,
    (tp-20000)/(tp+fp) as precision_limit,
    (tp-20000)/(tp-20000+fn) as recall_limit
from(
    select
        sum(if(label = 1 and is_match = 1, 1, 0)) as tp,
        sum(if(label = 0 and is_match = 1, 1, 0)) as fp,
        sum(if(label = 1 and is_match = 0, 1, 0)) as fn,
        sum(if(label = 0 and is_match = 0, 1, 0)) as tn
    from(
        select 
            a.ele_id,
            a.dianping_poi_id,
            a.label,
            b.is_match
        from(
            select
                ele_id,
                dianping_poi_id,
                label
            from
                test.rec_dom_match_generate_dianping_test_set
            ) a 
        join(
            select
                ele_id,
                shop_id as dianping_poi_id,
                cast(is_match as int) as is_match
            from
                test.100w_neg_check
            ) b
        on(
            a.ele_id = b.ele_id and 
            a.dianping_poi_id = b.dianping_poi_id
            )
        ) t
    ) t
;




drop table temp.temp_test_rec_dom_match_generate_dianping_test_set;
create table temp.temp_test_rec_dom_match_generate_dianping_test_set as
select
    ele_id,
    dianping_poi_id,
    label,
    ele_name,
    dianping_name,
    max(if(ele_name is null or dianping_name is null, 0, is_similar_poi(ele_name, dianping_name, 2))) as is_intersect
from
    test.rec_dom_match_generate_dianping_test_set
group by
    ele_id,
    dianping_poi_id,
    label,
    ele_name,
    dianping_name
having
    is_intersect > 0
;






DROP TABLE test.rec_dom_poi_change_record;
CREATE TABLE test.rec_dom_poi_change_record AS
select 
    dom_id,
    to_value as dianping_poi_id
from
    rec.rec_dom_poi_change_record
where 
    dt = '2017-03-31' and
    dom_id is not null and 
    dom_id > 0 and
    field_name = 'dianping_poi_id' and
    to_value !=0 and 
    to_value is not null
group by 
    dom_id,
    to_value
;

 



DROP TABLE test.temp_normalize_address_table;
CREATE TABLE test.temp_normalize_address_table AS
SELECT
    t.restaurant_id,
    t.address,
    normalize_address(t.address)
FROM(
    SELECT
        restaurant_id,
        parse_json_object(profile_json, "base.address") as address
    FROM
        dm.dm_ups_restaurant_info
    WHERE
        dt='3000-12-31'
    ) t
;


DROP TABLE test.test_edit_distance_table;
CREATE TABLE test.temp_normalize_address_table AS


select edit_distance(parse_json_object(normalize_address('江苏省镇江市宁杭南路'), '街道'),parse_json_object(normalize_address('句容市宁杭路交叉口金鑫商厦5号'), '街道'))
;

from(
    select
        normalize_address('繁昌县繁阳镇中心花园') as ele_address,
        normalize_address('繁昌县中心花园1-14#') as can_address
)
;




INSERT OVERWRITE TABLE test.rec_dom_geohash_candidates partition (dt='2017-03-30',version='new_dianping') 
SELECT t2.*, 
            d.name AS name2, 
            concat(d.sheng, 
            d.shiqu, 
            d.xian, 
            d.address) AS address2, 
            d.telphone AS phone2, 
            d.lat AS latitude2, 
            d.lng AS longitude2, 
            (CASE 
      WHEN d.crawler_flag==1 THEN 
      0 
      ELSE 1 END) AS is_valid2 
FROM 
      (SELECT '1' AS source1, t.id AS id1, t.name AS name1, t.address AS address1, t.phone AS phone1, t.latitude AS latitude1, t.longitude AS longitude1,t.is_valid AS is_valid1, t.geohash, '4' AS source2, c.id AS id2 
      FROM 
            (SELECT a.ele_id AS id, 
            b.name, 
            b.address_text AS address, 
            concat(b.phone, 
            ' ', b.mobile, ' ', b.keeper_phone) AS phone, b.latitude, b.longitude,b.is_valid, substr(geohash_of_latlng(b.latitude, b.longitude), 0, 6) AS geohash 
            FROM 
                  (SELECT * 
                  FROM test.rec_dom_input_data 
                  WHERE dt='2017-03-30' 
                              AND version='new') a 
                  LEFT JOIN ods.ods_restaurant b 
                        ON a.ele_id = b.id 
                  WHERE b.dt='2017-03-30' ) t 
                  LEFT JOIN rec.rec_dom_dianping_geohash c 
                        ON t.geohash = c.geohash 
                  WHERE c.dt='2017-03-30' ) t2 
            LEFT JOIN dw.dw_ext_meituan_tuangou_restaurant d 
            ON d.id = t2.id2 
WHERE d.dt='2017-03-30';










select food_id, attr_key, count(*) as num from dm.dm_ups_food_item_info where dt='3000-12-31' group by food_id, attr_key having num>1 limit 100;





select 
    restaurant_id,
    top_category,
    attr_key,
    count(*) as num
from 
    dm.dm_ups_restaurant_item_info
where
    dt='3000-12-31'
group by 
    restaurant_id,
    top_category,
    attr_key
having
    num > 1
limit 
    100
;



select
    restaurant_id,
    count(*) as num
from 
    temp.temp_dw_info_sr_shop_rand_indicatior
group by 
    restaurant_id
having 
    num > 1
;



select
    distinct(parse_json_object(profile_json, '$trd.retail_user_type')) as retail_user_type
from 
    dm.dm_ups_user_info_inc 
where 
    dt='3000-12-31' 
;





select
    parse_json_object(profile_json,'trd.retail_user_type') as retail_user_type
from 
    dm.dm_ups_user_info_inc 
where
    dt='3000-12-31'
limit 
    10
;


drop table temp.temp_dm_ups_user_info_inc;
create table temp.temp_dm_ups_user_info_inc as
    select
        *
    from
        dm.dm_ups_user_info_inc
    where 
        dt='3000-12-31'
    limit 
        1000
;




select 
    user_id,
    parse_json_object(profile_json,'$.rec.order_price_degree') as order_price_degree,
    parse_json_object(profile_json,'$.rec.order_price_interval') as order_price_interval,
    parse_json_object(profile_json,'$.rec.workday_time_price') as workday_time_price,
    parse_json_object(profile_json,'$.rec.weekend_time_price') as weekend_time_price,
    cast(parse_json_object(profile_json,'$.rec.order_rst_cat_disperse') as double) as order_rst_cat_disperse
from dm.dm_ups_user_info_inc 
where dt='2017-05-09' 
and batch_id>=0 and batch_id<2
limit 10
;






