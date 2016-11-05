#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_speciality.sql
# ** 功能描述： 用户的特殊属性
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-01
#***************************************************************************************************


-- sub task 1: 抽取馔山项目下过单的用户
DROP TABLE temp.temp_mdl_sia_restaurant_order;
CREATE TABLE temp.temp_mdl_sia_restaurant_order AS
    SELECT
        t2.user_id,
        max(t1.is_sia) AS is_sia
    FROM (
        SELECT
            id AS restaurant_id,
            is_sia
        FROM
            dw.dw_prd_restaurant_wide
        WHERE 
             dt='${day}' and
             is_sia=1
        ) t1
    JOIN (
        SELECT
            id AS order_id,
            restaurant_id,
            user_id
        FROM
            dw.dw_trd_order_wide
        WHERE 
            dt='${day}' and
            user_id<>886
        ) t2
    ON (
        t1.restaurant_id=t2.restaurant_id
        )
    GROUP BY
        t2.user_id
;

 
 
-- sub task 2: import data to ups
INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt='${day}', flag='speciality_info')
    SELECT
        user_id,
        'speciality' AS top_category,
        'is_sia' AS attr_key,
        is_sia AS attr_value,
        0 AS is_json,
        '${day}' AS update_time
    FROM
        temp.temp_mdl_sia_restaurant_order
;

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt='3000-12-31', flag='speciality_info')
    SELECT
        user_id,
        'speciality' AS top_category,
        'is_sia' AS attr_key,
        is_sia AS attr_value,
        0 AS is_json,
        '${day}' AS update_time
    FROM
        temp.temp_mdl_sia_restaurant_order
;

