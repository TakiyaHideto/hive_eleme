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
            dw.dw_trd_order_wide_day
        WHERE 
            dt='${day}' and
            order_status=1 and
            user_id<>886
        ) t2
    ON (
        t1.restaurant_id=t2.restaurant_id
        )
    GROUP BY
        t2.user_id
;

-- sub task 1.2: 馔山项目下过单的用户
DROP TABLE temp.temp_mdl_sia_restaurant_order_history;
CREATE TABLE temp.temp_mdl_sia_restaurant_order_history as 
    SELECT
        *
    FROM
        dm.dm_ups_user_item_info
    WHERE
        dt='3000-12-31' AND 
        flag='speciality_info'
;



-- sub task 2: 物流有限配送用户
DROP TABLE IF EXISTS temp.weihua_zheng_delivery_1;
CREATE TABLE temp.weihua_zheng_delivery_1 AS
    SELECT
        a.user_id, 
        b.label, 
        b.label_name, 
        c.level
    FROM(
        SELECT
            user_id, 
            bd_phone
        FROM
            dw.dw_usr_wide
        WHERE
            dt = GET_DATE('${day}', -1) AND 
            bd_phone != 'NULL'
        ) a
    LEFT JOIN(
        SELECT
            phone, 
            label, 
            label_name
        FROM
            st.st_trd_hongbao_user_label
        WHERE
            dt = GET_DATE('${day}', -1) 
        ) b 
    ON( 
        a.bd_phone = b.phone
        )
    LEFT JOIN(
        SELECT
            user_id, 
            level
        FROM
            rec.rec_prf_user_profile_rfm
        WHERE
            dt = GET_DATE('${day}', -1)
        ) c
    ON (
        a.user_id = c.user_id
        )
;

 
-- sub task 3: import data to ups
INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt='${day}', flag='speciality_info')
    SELECT
        user_id,
        max(top_category) as top_category,
        attr_key,
        max(attr_value) as attr_value,
        0 AS is_json,
        '${day}' AS update_time
    FROM(
        SELECT
            user_id,
            'speciality' AS top_category,
            'is_sia' AS attr_key,
            is_sia AS attr_value,
            0 AS is_json,
            '${day}' AS update_time
        FROM
            temp.temp_mdl_sia_restaurant_order

        UNION ALL
        SELECT
            user_id,
            'speciality' AS top_category,
            'is_sia' AS attr_key,
            attr_value,
            0 AS is_json,
            '${day}' AS update_time
        FROM
            temp.temp_mdl_sia_restaurant_order_history
       ) t
    GROUP BY
        user_id,
        attr_key

    UNION ALL
    SELECT
        a.user_id,
        'speciality' AS top_category,
        'delivery_priority' AS attr_key,
        a.delivery_priority AS attr_value,
        0 AS is_json,
        '${day}' AS update_time
    FROM
    (
        SELECT
            user_id,
            CASE
              WHEN label IN (11, 21, 22, 121, 122) THEN 1
              WHEN label IN (321, 322, 323) THEN 2
              WHEN label = 31 AND level = 25 THEN 3
              WHEN label = 31 AND level <= 24 and level >= 22 THEN 4
              WHEN label = 31 AND (level <= 21 OR level IS NULL) THEN 5
              ELSE 1
            END as delivery_priority
        FROM
            temp.weihua_zheng_delivery_1
    ) a
;

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt='3000-12-31', flag='speciality_info')
    SELECT
        user_id,
        top_category,
        attr_key,
        attr_value,
        is_json,
        update_time
    FROM
        dm.dm_ups_user_item_info
    WHERE    
        dt='${day}' and
        flag='speciality_info'
;






