#***************************************************************************************************
# ** 文件名称： dm_ups_user_info_inc_h2c_q.sql
# ** 功能描述： 生成dm_ups_user_info_inc表 Cassandra
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-08
#***************************************************************************************************


set mapred.max.split.size=128000000;
insert into table dm_ups_user_info_inc_h2c_q
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
parse_json_object(profile_json,'trd.order_city_id',false) as order_city_id
from dm.dm_ups_user_info_inc where dt='3000-12-31'