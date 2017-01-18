#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_restaurant_item_info_log.sql
# **  功能描述：
#     
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2017-01-18
# **
# **  ChangeLog：
#
#***************************************************************************************************

-- sub task 1: import data to ups 
INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'log_001')
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
        restaurant_id AS restaurant_id,
        ARRAY(
          CONCAT('recent_30_alltime_exp_cnt=', round(exposure_pv_rate,3)),
          CONCAT('recent_30_alltime_exp_ctr=', bad_rating_num),
          CONCAT('recent_30_meal_exp_ctr=', round(dinner_exposure_pv_rate,3)),
          CONCAT('recent_30_meal_exp_cnt=', dinner_exposure_pv),
          CONCAT('recent_30_aftertea_exp_ctr=', round(tea_exposure_pv_rate,3)),
          CONCAT('recent_30_aftertea_exp_cnt=', tea_exposure_pv),
          CONCAT('recent_30_nightsnack_exp_ctr=', round(night_exposure_pv_rate,3)),
          CONCAT('recent_30_nightsnack_exp_cnt=', night_exposure_pv)
          ) AS info_array
    FROM
        st.st_bs_shop_portrait
    WHERE
        dt = '${day}'
) a
LATERAL VIEW
EXPLODE(a.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0' AND LENGTH(SPLIT(item, '=')[1]) > 0;




##### sub task
##### copy newest data to dt = '3000-12-31'

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '3000-12-31', flag = 'log_001')
SELECT
    restaurant_id, top_category, attr_key, attr_value, is_json, update_time
FROM
    dm.dm_ups_restaurant_item_info
WHERE
    dt = '${day}' AND flag = 'log_001' AND attr_value != '0';


