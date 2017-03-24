#***************************************************************************************************
# **  文件名称： dm_ups_taker_info_h2c_beta_create.sql
# **  功能描述： 创建骑手画像Cassandra链接表 beta环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2017-03-20
# **
# **  ChangeLog
#***************************************************************************************************



DROP TABLE dm_test.dm_ups_taker_info_h2c_beta;
CREATE EXTERNAL TABLE dm_test.dm_ups_taker_info_h2c_beta
( 
  taker_id bigint,
  city_id int,
  created_at string,
  delivery_mode string,
  geo_name string,
  max_rst_count_per_deliver int,
  team_id bigint,
  team_name string,
  org_id int,
  team_created_at string,
  team_load int,
  team_status int,
  period_deliver_distance_avg string,
  period_fetch_distance_avg string,
  period_deliver_time_avg string,
  period_deliver_speed_avg string,
  recent_14_deliver_speed_trend string,
  fetch_speed_weighted_avg float,
  team_deliver_speed_rank int,
  rest_level_distribution string,
  recent_30_order_cnt int,
  recent_30_date_cnt int,
  day_peak_order_cnt_avg float,
  day_peak_order_cnt_std float,
  night_peak_order_cnt_avg float,
  night_peak_order_cnt_std float,
  ontime_order_rate float,
  day_peak_ontime_rate float,
  night_peak_ontime_rate float,
  recent_14_day_peak_order_cnt_trend string,
  recent_14_night_peak_order_cnt_trend string,
  recent_14_ontime_trend string
  
) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
WITH SERDEPROPERTIES ("yangdi" = "22222222222","cassandra.host" = "192.168.106.175,192.168.106.60","cassandra.port"="9042","cassandra.ks.name" = "rec","cassandra.table.name"="dm_ups_taker_info","cassandra.username"="cassandra","cassandra.password"="cassandra") TBLPROPERTIES("cassandra.batchmutate.size"="5","zyy_test"="8888888","cassandra.consistency.level"="QUORUM");





