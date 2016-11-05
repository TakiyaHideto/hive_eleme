#***************************************************************************************************
# ** 文件名称： dm_mdl_manipulation_test_by_jiahao_dong.sql
# ** 功能描述： 该script只为对一些非temp表分区进行删除操作
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-15
#***************************************************************************************************

-- drop table rec.rec_prd_restaurant_basic_info;
-- create table rec.rec_prd_restaurant_basic_info(
-- restaurant_id string,
-- agent_fee int,
-- min_deliver_amount int,
-- recent_food_popularity int,
-- food_cnt int,
-- image_cnt int,
-- base_algo_rank_score_without_random double
-- ) partitioned by (dt string)
-- row format delimited
-- fields terminated by '\t' 
-- stored as textfile;


drop table dm.dm_mdl_restaurant_eleme_subsidy_strategy;
create table dm.dm_mdl_restaurant_eleme_subsidy_strategy(
	restaurant_id string,
	strategy string,
	strategy_code string,
	is_multi_strategy string
) partitioned by (dt string)
row format delimited
fields terminated by '\t' 
stored as textfile;



-- drop table dm.dm_ups_restaurant_info;
-- create table dm.dm_ups_restaurant_info(
-- restaurant_id string,
-- profile_json string
-- ) partitioned by (dt string)
-- row format delimited
-- fields terminated by '\t' 
-- stored as textfile;



--

-- drop table dim.dim_ups_key_value_info;
-- create table dim.dim_ups_key_value_info(
-- top string,
-- key string,
-- json_key string,
-- value string
-- )
-- row format delimited
-- fields terminated by '\t' 
-- stored as textfile;

-- drop table dm.dm_mdl_user_food_prefer_click_based_score;

-- USE dm;
-- ALTER TABLE dm_ups_user_item_info DROP PARTITION(flag='rec_flavor',dt='2016-08-14');
-- USE dm;
-- ALTER TABLE dm_ups_user_item_info DROP PARTITION(flag='rec_flavor',dt='3000-12-31');

-- use dm;
-- ALTER TABLE dm_mdl_food_name_normalize_day ADD COLUMNS(tag_season STRING);
-- ALTER TABLE dm_mdl_food_name_normalize_day DROP PARTITION(dt='2016-09-05');
-- ALTER TABLE dm_mdl_food_name_normalize_day DROP PARTITION(dt='3000-12-31');

-- use dm;
-- ALTER TABLE dm_ups_user_item_info DROP PARTITION(flag='food_prefer', dt='2016-08-11');
-- ALTER TABLE dm_ups_user_item_info DROP PARTITION(flag='food_prefer', dt='2016-08-10');
-- ALTER TABLE dm_ups_user_item_info DROP PARTITION(flag='food_prefer', dt='3000-12-31');