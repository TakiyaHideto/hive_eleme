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
CREATE EXTERNAL TABLE `dm.dm_mdl_restaurant_eleme_subsidy_strategy`(
    restaurant_id bigint,
    name string,
    restaurant_tag string,
    geohash7 string COMMENT 'geo_hash前七位',
    cat0_name string COMMENT '一级餐厅品类',
    cat1_name string COMMENT '二级餐厅品类',
    strategy string COMMENT '满减策略',
    strategy_code string COMMENT '满减策略编码',
    strategy_detail string COMMENT '满减细则',
    lastest_subsidy string COMMENT '最近三次订单补贴情况',
    is_multi_strategy int COMMENT '是否为多个满减策略',
    cur_month_total double COMMENT '本月截止当前的销售额',
    lst_month_total double COMMENT '上一月销售额',
    subsidy_total_limit double COMMENT '餐厅补贴总金额上限',
    sales_ratio double COMMENT '本月截止当前的销售额与上月销售额比值'
    )
PARTITIONED BY (dt string)
LOCATION  '/data/external_table/dm/dm_mdl_restaurant_eleme_subsidy_strategy';

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