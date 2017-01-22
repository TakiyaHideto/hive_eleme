#***************************************************************************************************
# **  文件名称： dm_ups_user_info_h2c_prod_bjc1.sql
# **  功能描述： 用户画像导入Cassandra 线上环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-12-01
# **
# **  ChangeLog
#***************************************************************************************************



set mapred.max.split.size=128000000;
insert into table dm_test.dm_ups_user_info_h2c_prod_bjc1 
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
parse_json_object(profile_json,'trd.delivery_type_prefer') as delivery_type_prefer,
parse_json_object(profile_json,'trd.recent_30_avg_delivery_fee') as recent_30_avg_delivery_fee,
parse_json_object(profile_json,'trd.collect_rest_list') as collect_rest_list,
parse_json_object(profile_json,'trd.click_rest_list') as click_rest_list,
parse_json_object(profile_json,'trd.order_rest_list') as order_rest_list,
parse_json_object(profile_json,'trd.score_rest_list') as score_rest_list,
parse_json_object(profile_json,'trd.order_again_rest_list') as order_again_rest_list,
parse_json_object(profile_json,'trd.recent_30_active_day') as recent_30_active_day,
parse_json_object(profile_json,'trd.recent_30_play_time') as recent_30_play_time,
parse_json_object(profile_json,'trd.recent_30_order_cnt') as recent_30_order_cnt,
parse_json_object(profile_json,'trd.recent_30_order_amt') as recent_30_order_amt,
parse_json_object(profile_json,'speciality.delivery_priority') as delivery_priority,

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

parse_json_object(profile_json,'rec.bs_user_rest',false) as bs_user_rest,

parse_json_object(profile_json,'rec.rest_order_cat0_prefer') as rest_order_cat0_prefer,
parse_json_object(profile_json,'rec.rest_order_cat1_prefer') as rest_order_cat1_prefer,
parse_json_object(profile_json,'rec.rest_behavior') as rest_behavior,
parse_json_object(profile_json,'rec.cat_profile') as cat_profile,
parse_json_object(profile_json,'rec.rest_prefer') as rest_prefer
from dm.dm_ups_user_info_inc 
where dt='3000-12-31' and 
batch_id=0;