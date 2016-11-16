#***************************************************************************************************
# ** 文件名称： dm_ups_user_info_inc_cassandra.sql
# ** 功能描述： 生成dm_ups_user_info_inc表 Cassandra
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-08
#***************************************************************************************************


CREATE TABLE dm_ups_user_info_inc (
  user_id bigint,
  email text,
  gender text,
  nick_name text,
  pre_phone text,
  create_time text,
  source int,
  brand text,
  os_platform text,
  os_version float,
  model text,
  id text,
  resolution text,
  category_prefer text,
  hongbao_sensitive float,
  price_sensitive float,
  style_prefer text,
  flavor_prefer text,
  food_prefer text,
  visit_address text,
  last_visit_time text,
  visit_time_prefer text,
  visit_date_prefer text,
  visit_province_id bigint,
  first_visit_time text,
  visit_city_id int,
  order_subsidy float,
  order_address text,
  first_order_time text,
  last_order_time text,
  order_date_prefer text,
  order_cnt int,
  consume_level float,
  order_avg_fee float,
  order_province_id bigint,
  order_hongbao float,
  order_bu_flag text,
  order_amt float,
  point bigint,
  order_time_prefer text,
  order_city_id int,
  PRIMARY KEY (user_id)
)
with gc_grace_seconds=432000 and default_time_to_live=604800;

