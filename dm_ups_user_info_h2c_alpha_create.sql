#***************************************************************************************************
# **  文件名称： dm_ups_user_info_h2c_alpha_create.sql
# **  功能描述： 创建用户画像Cassandra链接表 alpha环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-12-01
# **
# **  ChangeLog
#***************************************************************************************************




DROP TABLE dm_test.dm_ups_user_info_h2c_alpha;
CREATE EXTERNAL TABLE dm_test.dm_ups_user_info_h2c_alpha
( 
  user_id bigint, 
  email string, 
  gender string, 
  nick_name string,
  pre_phone string,
  create_time string,
  source int,
  brand string,
  os_platform string,
  os_version string,
  model string,
  device_id string,
  resolution string,
  category_prefer string,
  hongbao_sensitive float,
  price_sensitive float,
  style_prefer string,
  flavor_prefer string,
  food_prefer string,
  visit_address string,
  last_visit_time string,
  visit_time_prefer string,
  visit_date_prefer string,
  visit_province_id bigint,
  first_visit_time string,
  visit_city_id int,
  order_subsidy float,
  order_address string,
  first_order_time string,
  last_order_time string,
  order_date_prefer string,
  order_cnt int,
  consume_level float,
  order_avg_fee float,
  order_province_id bigint,
  order_hongbao float,
  order_bu_flag string,
  order_amt float,
  point bigint,
  order_time_prefer string,
  order_city_id int,
  is_sia int,
  phone_number bigint,
  delivery_type_prefer string,
  recent_30_avg_delivery_fee float,
  collect_rest_list string,
  click_rest_list string,
  order_rest_list string,
  score_rest_list string,
  order_again_rest_list string,
  recent_30_active_day int,
  recent_30_play_time int,
  recent_30_order_cnt int,
  recent_30_order_amt float,
  delivery_priority int,
  is_vip int,
  phone_city_id int,
  phone_city_name string,
  phone_province_id int,
  phone_province_name string,
  hongbao_balance double,
  recent_7_reminder_order_num int,
  recent_7_reminder_order_rate double,
  recent_7_withdraw_order_num int,
  recent_7_withdraw_order_rate double,
  rest_distance_avg double,
  order_discount_rate double,
  order_manjian_rate double,
  recent_90_click_premium_rest_rate double,
  recent_90_visit_dish_per_rest_avg int,
  recent_30_is_new int,
  order_interval_avg int,
  order_interval_min int,
  order_delivery_fee_rate double,
  click_rest_sale_avg double,
  click_rest_open_time_avg int,
  click_rest_safety_level_avg double,
  click_rest_score_avg double,
  click_rest_desc_length_avg int,
  click_rest_delivery_time_avg int,
  click_rest_has_picture_rate double,
  click_rest_discount_avg double,
  bs_user_rest string,
  rest_order_cat0_prefer string,
  rest_order_cat1_prefer string,
  rest_behavior string,
  cat_profile string,
  rest_prefer string
) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
WITH SERDEPROPERTIES ("yangdi" = "22222222222","cassandra.host" = "192.168.115.180,192.168.115.169","cassandra.port"="9042","cassandra.ks.name" = "rec","cassandra.table.name"="dm_ups_user_info","cassandra.username"="cassandra","cassandra.password"="cassandra") TBLPROPERTIES("cassandra.batchmutate.size"="5","zyy_test"="8888888","cassandra.consistency.level"="QUORUM");








