#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_user_item_info_behavior.sql
# **  功能描述：
#         1. 基于流量组提供的用户行为标签数据，导入到用户画像中
#
# **  创建者：weihua.zheng@ele.me
# **  创建日期： 2016-12-19 16:00:00
# **
# **  ChangeLog：
#
#***************************************************************************************************

#### 得到用户手机归属地信息

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt = '${day}', flag = 'base_phone_info')
SELECT
	c.user_id,
	'base' AS top_category,
	SPLIT(item, '=')[0] AS attr_key,
	SPLIT(item, '=')[1] AS attr_value,
	0 AS is_json,
	'${day}' AS update_time
FROM
(
	SELECT /*+MAPJOIN(a)*/
		b.user_id,
		ARRAY(
		  CONCAT('phone_city_id=', MAX(b.phone_city_id)),
		  CONCAT('phone_city_name=', MAX(a.city_name)),
		  CONCAT('phone_province_id=', MAX(a.province_id)),
		  CONCAT('phone_province_name=', MAX(b.phone_province))
		) AS info_array
	FROM
	(
		SELECT
		    eleme_city_id AS city_id, city_name, province_id
		FROM
		    dim.dim_gis_city
	) a
	JOIN
	(
		SELECT
		    user_id, phone_city_id, phone_province
		FROM
		    st.st_bs_user_portrait
		WHERE
		    dt = '${day}' AND user_id > 0 AND phone_city_id > 0
	) b
	ON a.city_id = b.phone_city_id
	GROUP BY
	    b.user_id
) c
LATERAL VIEW
EXPLODE(c.info_array) mytable AS item;


##### 得到用户行为标签

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt = '${day}', flag = 'trd_behavior')
SELECT
    a.user_id,
    'trd' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM
(
    SELECT
        user_id,
        ARRAY(
          CONCAT('hongbao_balance=', user_hongbao_amt),
          CONCAT('recent_7_reminder_order_num=', user_reminder_order_num),
		  CONCAT('recent_7_reminder_order_rate=', ROUND(user_reminder_rate, 3)),
		  CONCAT('recent_7_withdraw_order_num=', user_withdraw_order_num),
		  CONCAT('recent_7_withdraw_order_rate=', ROUND(user_withdraw_rate, 3)),
		  CONCAT('rest_distance_avg=', ROUND(user_res_distance, 3)),
		  CONCAT('order_discount_rate=', ROUND(user_manjian_perf, 3)),
		  CONCAT('order_manjian_rate=', ROUND(user_discount_perf, 3)),
		  CONCAT('recent_90_click_premium_rest_rate=', ROUND(user_premium_amt, 3)),
		  CONCAT('recent_90_visit_dish_per_rest_avg=', user_avg_dishes),
		  CONCAT('recent_30_is_new=', is_new),
		  CONCAT('order_interval_avg=', order_interval_avg),
		  CONCAT('order_interval_min=', order_interval_min),
		  CONCAT('order_delivery_fee_rate=', ROUND(user_logistfee_rate, 3)),
		  CONCAT('click_rest_sale_avg=', user_res_avg_sales),
		  CONCAT('click_rest_open_time_avg=', user_res_avg_time),
		  CONCAT('click_rest_safety_level_avg=', user_res_safety),
		  CONCAT('click_rest_score_avg=', user_res_score),
		  CONCAT('click_rest_desc_length_avg=', user_res_avg_discrip),
		  CONCAT('click_rest_delivery_time_avg=', user_res_avgtime),
		  CONCAT('click_rest_has_picture_rate=', ROUND(user_res_picpct, 3)),
		  CONCAT('click_rest_discount_avg=', user_res_discountpct)
        ) AS info_array
    FROM
        st.st_bs_user_portrait
    WHERE
        dt = '${day}'
) a
LATERAL VIEW
EXPLODE(a.info_array) mytable AS item
WHERE
   SPLIT(item, '=')[1] > '0.0';


#### copty to dt=3000-12-31

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt = '3000-12-31', flag)
SELECT
    user_id, top_category, attr_key, attr_value, is_json, update_time, flag
FROM
    dm.dm_ups_user_item_info
WHERE
    dt = '${day}' AND flag IN ('base_phone_info', 'trd_behavior');