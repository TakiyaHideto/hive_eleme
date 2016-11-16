#***************************************************************************************************
# ** 文件名称： dm_ups_user_info_inc_h2c_q.sql
# ** 功能描述： 生成dm_ups_user_info_inc表 Cassandra
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-08
#***************************************************************************************************


CREATE EXTERNAL TABLE dm_ups_user_info_inc_h2c_q(
user_id bigint,
email string,
gender string,
nick_name string,
pre_phone string,
create_time string,
source int,
brand string,
os_platform string,
os_version float,
model string,
id string,
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
order_city_id int) 
STORED BY 'org.apache.hadoop.hive.cassandra.cql.CqlStorageHandler'
WITH SERDEPROPERTIES ("yangdi" = "22222222222","cassandra.host" = "10.0.45.125,10.0.45.155,10.0.45.170","cassandra.port"="9042","cassandra.ks.name" = "rec","cassandra.table.name"="dm_ups_user_info_inc","cassandra.username"="cassandra","cassandra.password"="cassandra") 
TBLPROPERTIES("cassandra.batchmutate.size"="5","zyy_test"="8888888","cassandra.consistency.level"="QUORUM");


