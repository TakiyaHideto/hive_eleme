#***************************************************************************************************
# **  文件名称： dm_ups_restaurant_info_h2c_prod_bjc1_create.sql
# **  功能描述： 创建餐厅画像Cassandra链接表 线上环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-12-05
# **
# **  ChangeLog
#***************************************************************************************************



DROP TABLE dm_test.dm_ups_restaurant_info_h2c_prod_bjc1;
CREATE EXTERNAL TABLE dm_test.dm_ups_restaurant_info_h2c_prod_bjc1
( 
  restaurant_id bigint,
  name string,
  address string,
  restaurant_type string,
  latitude string,
  longitude string,
  geohash string,
  city_id int,
  city_name string,
  min_deliver_amt float,
  bu_flag string,
  time_ensure_spent int,
  time_ensure_discount string,
  deliver_type string,
  food_number int,
  has_license int,
  has_service_license int,
  is_exclusive int,
  is_sia int,

  cat0_name string,
  cat1_name string,

  recent_7_open_days int,
  recent_30_open_days int,
  recent_7_open_hours float,
  recent_30_open_hours float,
  recent_7_order_complain_cnt int,
  recent_30_order_complain_cnt int,
  recent_7_order_remind_cnt int,
  recent_30_order_remind_cnt int,
  recent_7_user_refuse_order_cnt int,
  recent_30_user_refuse_order_cnt int,
  recent_7_rst_refuse_order_cnt int,
  recent_30_rst_refuse_order_cnt int,

  star_rating_5_cnt int,
  star_rating_4_cnt int,
  star_rating_3_cnt int,
  star_rating_2_cnt int,
  star_rating_1_cnt int,

  recent_7_order_cnt int,
  recent_30_order_cnt int,
  recent_7_user_cnt int,
  recent_30_user_cnt int,
  recent_7_ord_usr_ratio float,
  recent_30_ord_usr_ratio float,
  recent_7_returned_customer_scale float,
  recent_30_returned_customer_scale float,
  recent_7_order_amt float,
  recent_30_order_amt float,
  recent_30_order_price_median float,
  recent_30_order_price_stddev float,
  recent_30_payment_amt float,
  recent_30_payment_median float,
  recent_30_payment_stddev float,
  recent_30_eleme_subsidy_amt float,
  recent_30_eleme_subsidy_median float,
  recent_30_eleme_subsidy_stddev float,
  recent_30_order_chargeback_scale float,
  recent_30_order_unpaid_cnt float,
  recent_30_order_unpaid_scale float,
  recent_7_order_hongbao_preferential_avg float,
  recent_7_order_hongbao_preferential_stddev float,
  order_time_prefer string,
  order_date_prefer string,
  order_month_distribution string,
  food_top_10 string
  ) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
WITH SERDEPROPERTIES ("yangdi" = "22222222222","cassandra.host" = "10.0.45.125,10.0.45.155,10.0.45.170","cassandra.port"="9042","cassandra.ks.name" = "rec","cassandra.table.name"="dm_ups_restaurant_info","cassandra.username"="rec2","cassandra.password"="yxpDKmfsjjwred46") TBLPROPERTIES("cassandra.batchmutate.size"="5","zyy_test"="8888888","cassandra.consistency.level"="QUORUM");





