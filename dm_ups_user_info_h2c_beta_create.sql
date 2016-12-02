#***************************************************************************************************
# **  文件名称： dm_ups_user_info_h2c_beta_create.sql
# **  功能描述： 创建用户画像Cassandra链接表 beta环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-12-01
# **
# **  ChangeLog
#***************************************************************************************************



DROP TABLE dm_test.dm_ups_user_info_h2c_beta;
CREATE EXTERNAL TABLE dm_test.dm_ups_user_info_h2c_beta
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
  delivery_priority int) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
WITH SERDEPROPERTIES ("yangdi" = "22222222222","cassandra.host" = "192.168.106.175,192.168.106.60","cassandra.port"="9042","cassandra.ks.name" = "rec","cassandra.table.name"="dm_ups_user_info","cassandra.username"="cassandra","cassandra.password"="cassandra") TBLPROPERTIES("cassandra.batchmutate.size"="5","zyy_test"="8888888","cassandra.consistency.level"="QUORUM");