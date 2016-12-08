#***************************************************************************************************
# ** 文件名称： dm_ups_food_info_inc_cassandra.sql
# ** 功能描述： 生成dm_ups_food_info_inc表 Cassandra
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-06
#***************************************************************************************************


CREATE TABLE dm_ups_food_info (
  food_id bigint,
  name text,
  restaurant_id bigint,
  price_original float,
  price_current float,
  price_changed_at  text,
  description text,
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
  cat_0 text,
  cat_1 text,
  cat_2 text,
  flavor text,
  cooking_method  text,
  tag_function  text,
  tag_scene  text
  PRIMARY KEY (food_id)
)
with gc_grace_seconds=432000 and default_time_to_live=604800;

