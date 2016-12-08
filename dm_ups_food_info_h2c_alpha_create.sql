#***************************************************************************************************
# **  文件名称： dm_ups_food_info_h2c_alpha_create.sql
# **  功能描述： 创建食品画像Cassandra链接表 alpha环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-12-05
# **
# **  ChangeLog
#***************************************************************************************************



DROP TABLE dm_test.dm_ups_food_info_h2c_alpha;
CREATE EXTERNAL TABLE dm_test.dm_ups_food_info_h2c_alpha
( 
  food_id bigint,
  name string,
  restaurant_id bigint,
  price_original float,
  price_current float,
  price_changed_at  string,
  description string,
  is_new  int,
  is_featured int,
  is_gum  int,
  is_spicy  int,
  has_activity  int,
  sold_out  int,
  packing_fee float,            
  order_cnt_7 int,
  order_cnt_30 int,
  cnt_7 int,
  cnt_30 int,
  cat_0 string,
  cat_1 string,
  cat_2 string,
  flavor string,
  cooking_method  string,
  tag_function  string,
  tag_scene  string) STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
WITH SERDEPROPERTIES ("yangdi" = "22222222222","cassandra.host" = "192.168.115.180,192.168.115.169","cassandra.port"="9042","cassandra.ks.name" = "rec","cassandra.table.name"="dm_ups_food_info","cassandra.username"="cassandra","cassandra.password"="cassandra") TBLPROPERTIES("cassandra.batchmutate.size"="5","zyy_test"="8888888","cassandra.consistency.level"="QUORUM");





