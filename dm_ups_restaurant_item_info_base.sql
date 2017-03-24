#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_restaurant_item_info_base.sql
# **  功能描述：
#         1. 得到餐厅的基础信息
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-09-12 15:30:00
# **
# **  ChangeLog：
#
#***************************************************************************************************

-- sub task: 1 
    -- info from platform_dw.sr_shop_rank_indicator
DROP TABLE temp.temp_dw_info_sr_shop_rand_indicatior;
CREATE TABLE temp.temp_dw_info_sr_shop_rand_indicatior AS
    SELECT
        restaurant_id,
        is_premium,
        is_hummer,
        is_picture,
        is_certification,
        is_payment,
        is_new_restaurant,
        is_royalty,
        is_gka,
        is_controlled_by_eleme,
        is_rescued
    FROM
        platform_dw.sr_shop_rank_indicator
    WHERE
        dt='${day}'
;


-- sub task: 2
    -- info from dw.dw_prd_food
DROP TABLE temp.temp_info_prd_food;
CREATE TABLE temp.temp_info_prd_food AS
    SELECT
        restaurant_id,
        AVG(price) AS food_price_avg,
        SUM(CASE WHEN NVL(TRIM(image_hash),'') <>'' THEN 1 ELSE 0 END) AS food_has_picture_cnt,
        SUM(CASE WHEN NVL(TRIM(image_hash),'') <>'' THEN 1 ELSE 0 END)/COUNT(id) AS food_has_picture_scale
    FROM
        dw.dw_prd_food
    WHERE
        dt='${day}' AND
        is_valid=1
    GROUP BY 
        restaurant_id
;


-- sub task: 3
    -- info from dw.dw_prd_restaurant_business_registration
DROP TABLE temp.temp_prd_restaurant_business_registration;
CREATE TABLE temp.temp_prd_restaurant_business_registration AS
    SELECT
        restaurant_id,
        CASE 
            WHEN last_check_result='良好' THEN 3
            WHEN last_check_result='一般' THEN 2
            WHEN last_check_result='较差' THEN 1
            ELSE 0 
        END AS security_level
    FROM
        dw.dw_prd_restaurant_business_registration
    WHERE
        dt='${day}'
;



##### sub task : 4
##### 从餐厅信息表中提取基础信息

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'base')
SELECT
    a.restaurant_id,
    'base' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM
(
    SELECT
        id AS restaurant_id,
        ARRAY(
          CONCAT('name=', TRIM(name)),
          CONCAT('address=', TRIM(regexp_replace(address_text, '\"|\t|\\\\', '') )),
          CONCAT('latitude=', ROUND(latitude, 3)),
          CONCAT('longitude=', ROUND(longitude, 3)),
          CONCAT('geohash=', GEOHASH_OF_LATLNG(latitude, longitude)),
          CONCAT('create_time=', created_at),
          CONCAT('min_deliver_amt=', min_deliver_amount),
          CONCAT('time_ensure_spent=', time_ensure_spent),
          CONCAT('time_ensure_discount=', time_ensure_discount),
          CONCAT('city_id=', city_id),
          CONCAT('deliver_type=', LOWER(deliver_type)),
          CONCAT('food_number=', restaurant_food_num),
          CONCAT('has_license=', is_license_no),
          CONCAT('has_service_license=', is_restaurant_service_licence_no),
          CONCAT('bu_flag=', LOWER(bu_flag)),
          CONCAT('is_sia=', is_sia),
          CONCAT('is_exclusive=', is_exclusive)
        ) AS info_array
    FROM
        dw.dw_prd_restaurant_wide
    WHERE
        ---- 类型100是测试餐厅，1是正常的
        ---- dt = '${day}' AND is_valid = 1 AND type != 100
        dt = '${day}' AND 
        type != 100

    UNION ALL
    SELECT
        restaurant_id,
        ARRAY(
            CONCAT('is_premium=', is_premium), 
            CONCAT('is_payment=', is_payment), 
            CONCAT('is_hummer=', is_hummer), 
            CONCAT('is_picture=', is_picture), 
            CONCAT('is_certification=', is_certification), 
            CONCAT('is_new=', is_new_restaurant), 
            CONCAT('is_royalty=', is_royalty),
            CONCAT('is_gka=', is_gka),
            CONCAT('is_controlled_by_eleme=', is_controlled_by_eleme),
            CONCAT('is_rescued=', is_rescued)
          ) AS info_array
    FROM
        temp.temp_dw_info_sr_shop_rand_indicatior

    UNION ALL
    SELECT
        restaurant_id,
        ARRAY(
            CONCAT('food_price_avg=', food_price_avg),
            CONCAT('food_has_picture_scale=', ROUND(food_has_picture_scale,3)),
            CONCAT('food_has_picture_cnt=', food_has_picture_cnt)
          ) AS info_array
    FROM
        temp.temp_info_prd_food

    UNION ALL
    SELECT
        restaurant_id,
        ARRAY(
            CONCAT('security_level=', security_level)
          ) AS info_array
    FROM
        temp.temp_prd_restaurant_business_registration
) a
LATERAL VIEW
EXPLODE(a.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0' AND 
    LENGTH(SPLIT(item, '=')[1]) > 0
;


##### sub task
##### copy newest data to dt = '3000-12-31'

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '3000-12-31', flag = 'base')
SELECT
    restaurant_id, top_category, attr_key, attr_value, is_json, update_time
FROM
    dm.dm_ups_restaurant_item_info
WHERE
    dt = '${day}' AND 
    flag = 'base' AND 
    attr_value != '0'
;

