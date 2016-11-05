#***************************************************************************************************
# ** 文件名称： dm_hotfood_impression_feedback_info_day_inc.sql
# ** 功能描述： 热卖美食 食物曝光反馈信息 用于推荐算法的效果分析
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-04
#***************************************************************************************************


CREATE TABLE IF NOT EXISTS dm.dm_mdl_hotfood_impression_feedback_info_day_inc
(
session_id string,
user_id bigint,
restaurant_id bigint,
restaurant_index int,
food_id int,
food_name_origin string,
food_name_normalized string,
food_index string,
is_rec string,
is_click string
) PARTITIONED BY (dt string);

DROP TABLE temp.temp_hotfood_impression_food_name_mapping_${dt};
CREATE TABLE temp.temp_hotfood_impression_food_name_mapping_${dt} AS
SELECT t1.session_id, t1.user_id, t1.restaurant_id, t1.restaurant_index, t1.food_id, t2.food_name, t2.normalize_food_name, t1.food_index
FROM (SELECT session_id, user_id, restaurant_id, restaurant_index, food_id, food_index FROM dw.dw_log_hotfood_exposure_day_inc WHERE dt='${day}') t1
JOIN (SELECT food_id, food_name, normalize_food_name FROM dm.dm_mdl_food_name_normalize_day WHERE dt='${day}') t2
ON t1.food_id=t2.food_id;

DROP TABLE temp.temp_hotfood_user_rec_food_restaurant_mapping_${dt};
CREATE TABLE temp.temp_hotfood_user_rec_food_restaurant_mapping_${dt} AS
SELECT t.user_id, t.food_name_normalized, t.food_id, max(t.rec_flag) AS is_rec
FROM( 
	SELECT t1.user_id, t1.rec_food_name AS food_name_normalized, t2.food_id, 1 AS rec_flag 
		FROM (SELECT user_id, rec_food_name 
			  FROM (select * from rec.rec_hotfood_user_info where dt='${day}' and model='food_prefer') t
			  lateral view explode(get_json_keys(attr_value)) tmpTbl AS rec_food_name) t1
		JOIN (SELECT user_id, food_id, food_name, normalize_food_name FROM temp.temp_hotfood_impression_food_name_mapping_${dt}) t2
		ON t1.rec_food_name=t2.normalize_food_name and t1.user_id=t2.user_id
	UNION ALL
	SELECT user_id, normalize_food_name AS food_name_normalized, food_id, 0 AS rec_flag FROM temp.temp_hotfood_impression_food_name_mapping_${dt}
) t
GROUP BY t.user_id, t.food_name, t.food_id;

DROP TABLE temp.temp_hotfood_rec_food_click_flag_info_${dt};
CREATE TABLE temp.temp_hotfood_rec_food_click_flag_info_${dt} AS
SELECT t.session_id, t.food_name_origin, t.restaurant_id, max(t.click_flag) AS is_click
FROM (
SELECT session_id, split(parse_json_object(activity_param,'message'),'                    ')[0] AS food_name_origin, 
parse_json_object(activity_param,'restaurant_id') AS restaurant_id, 1 AS click_flag
FROM dw.dw_log_app_pv_day_inc WHERE dt='${day}' and activity_id=1399
UNION ALL 
SELECT session_id, food_name AS food_name_origin, restaurant_id, 0 AS click_flag FROM temp.temp_hotfood_impression_food_name_mapping_${dt}
) t
GROUP BY t.session_id, t.food_name_origin, t.restaurant_id;

DROP TABLE temp.temp_hotfood_impression_feedback_info_day_inc_merge_1;
CREATE TABLE temp.temp_hotfood_impression_feedback_info_day_inc_merge_1 AS
SELECT t1.session_id, t1.user_id, t1.restaurant_id, t1.restaurant_index, t1.food_id, t1.food_name as food_name_origin, t1.normalize_food_name, t1.food_index, t2.is_rec
FROM (SELECT * FROM temp.temp_hotfood_impression_food_name_mapping_${dt}) t1
JOIN (SELECT * FROM temp.temp_hotfood_user_rec_food_restaurant_mapping_${dt}) t2
ON t1.user_id=t2.user_id and t1.food_id=t2.food_id;

DROP TABLE temp.temp_hotfood_impression_feedback_info_day_inc_merge_2;
CREATE TABLE temp.temp_hotfood_impression_feedback_info_day_inc_merge_2 AS
SELECT t1.session_id, t1.user_id, t1.restaurant_id, t1.restaurant_index, t1.food_id, t2.food_name_origin, 
t1.normalize_food_name as food_name_normalized, t1.food_index, t1.is_rec, t2.is_click
FROM (SELECT * FROM temp.temp_hotfood_impression_feedback_info_day_inc_merge_1) t1
JOIN (SELECT * FROM temp.temp_hotfood_rec_food_click_flag_info_${dt}) t2
ON t1.session_id=t2.session_id and t1.restaurant_id=t2.restaurant_id and t1.food_name_origin=t2.food_name_origin;

DROP TABLE temp.temp_hotfood_rec_food_order_flag_info_${dt};
CREATE TABLE temp.temp_hotfood_rec_food_order_flag_info_${dt} AS
SELECT t.session_id, t.restaurant_id, t.food_name_origin, max(t.buy_flag) AS is_buy
FROM(
SELECT session_id, restaurant_id, name AS food_name_origin, 1 AS buy_flag
FROM (SELECT session_id, order_id FROM dw.dw_log_order_session_day_inc WHERE dt='${day}') t1
JOIN (SELECT order_id, restaurant_id, name FROM dw.dw_trd_order_item WHERE dt='${day}') t2
ON t1.order_id=t2.order_id
UNION ALL
SELECT session_id, restaurant_id, food_name AS food_name_origin, 0 AS buy_flag
FROM temp.temp_hotfood_impression_food_name_mapping_${dt}
) t
GROUP BY t.session_id, t.restaurant_id, t.food_name_origin;

DROP TABLE temp.temp_hotfood_impression_feedback_info_day_inc_merge_3;
CREATE TABLE temp.temp_hotfood_impression_feedback_info_day_inc_merge_3 AS
SELECT t1.session_id, t1.user_id, t1.restaurant_id, t1.restaurant_index, t1.food_id, t2.food_name_origin, 
t1.food_name_normalized, t1.food_index, t1.is_rec, t1.is_click, t2.is_buy
FROM(SELECT * FROM temp.temp_hotfood_impression_feedback_info_day_inc_merge_2) t1
JOIN(SELECT * FROM temp.temp_hotfood_rec_food_order_flag_info_${dt}) t2
ON t1.session_id=t2.session_id and t1.food_name_origin=t2.food_name_origin and t1.restaurant_id=t2.restaurant_id;

INSERT OVERWRITE TABLE dm.dm_mdl_hotfood_impression_feedback_info_day_inc PARTITION(dt='${day}')
SELECT *
FROM temp.temp_hotfood_impression_feedback_info_day_inc_merge_3
