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

##### sub task : 1
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
          CONCAT('address=', TRIM(address_text)),
          CONCAT('latitude=', ROUND(latitude, 3)),
          CONCAT('longitude=', ROUND(longitude, 3)),
          CONCAT('geohash=', GEOHASH_OF_LATLNG(latitude, longitude)),
          CONCAT('create_time=', created_at),
          CONCAT('min_deliver_amount=', min_deliver_amount),
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
        dt = '${day}' AND type != 100
) a
LATERAL VIEW
EXPLODE(a.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0' AND LENGTH(SPLIT(item, '=')[1]) > 0;


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
    dt = '${day}' AND flag = 'base' AND attr_value != '0';

