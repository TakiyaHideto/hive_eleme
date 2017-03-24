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

-- sub task 1: 
    -- calculate ctr-related info from dm.dm_log_restaurant_exposure_click_rate_day_inc
drop table temp.temp_log_restaurant_exposure_click_rate_day_inc;
create table temp.temp_log_restaurant_exposure_click_rate_day_inc as
    select
        restaurant_id,
        sum(exposure_pv) as recent_30_alltime_exp_cnt,
        sum(click_pv)/sum(exposure_pv) as recent_30_alltime_exp_ctr,
        sum(dinner_exposure_pv) as recent_30_meal_exp_cnt,
        sum(dinner_click_pv)/sum(dinner_exposure_pv) as recent_30_meal_exp_ctr,
        sum(tea_exposure_pv) as recent_30_aftertea_exp_cnt,
        sum(tea_click_pv)/sum(tea_exposure_pv) as recent_30_aftertea_exp_ctr,
        sum(night_exposure_pv) as recent_30_nightsnack_exp_cnt,
        sum(night_click_pv)/sum(night_exposure_pv) as recent_30_nightsnack_exp_ctr
    from
        dm.dm_log_restaurant_exposure_click_rate_day_inc
    where 
        dt >= get_date('${day}', -30) and 
        dt <= '${day}'
    group by 
        restaurant_id
;

-- sub task 2: import data to ups 
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
          CONCAT('recent_30_alltime_exp_cnt=', recent_30_alltime_exp_cnt),
          CONCAT('recent_30_alltime_exp_ctr=', round(recent_30_alltime_exp_ctr,3)),
          CONCAT('recent_30_meal_exp_cnt=', recent_30_meal_exp_cnt),
          CONCAT('recent_30_meal_exp_ctr=', round(recent_30_meal_exp_ctr, 3)),
          CONCAT('recent_30_aftertea_exp_cnt=', recent_30_aftertea_exp_cnt),
          CONCAT('recent_30_aftertea_exp_ctr=', round(recent_30_aftertea_exp_ctr, 3)),
          CONCAT('recent_30_nightsnack_exp_cnt=', recent_30_nightsnack_exp_cnt),
          CONCAT('recent_30_nightsnack_exp_ctr=', round(recent_30_nightsnack_exp_ctr, 3))
          ) AS info_array
    FROM
        temp.temp_log_restaurant_exposure_click_rate_day_inc
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


