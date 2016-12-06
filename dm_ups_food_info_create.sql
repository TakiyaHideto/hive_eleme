#***************************************************************************************************
# ** 文件名称： dm_ups_food_info_create.sql
# ** 功能描述： 食品画像hive汇总表 创建
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-06
#***************************************************************************************************


drop table dm.dm_ups_food_info;
CREATE EXTERNAL TABLE `dm.dm_ups_food_info`(
    food_id bigint,
    profile_json string
    )
PARTITIONED BY (dt string)
LOCATION  '/data/external_table/dm/dm_ups_food_info';
