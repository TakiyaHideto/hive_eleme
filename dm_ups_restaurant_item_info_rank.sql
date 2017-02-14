#***************************************************************************************************
# **  文件名称： dm_ups_restaurant_item_info_rank.sql
# **  功能描述： 从rec rank info中提取出相关rank特征
#
# **  创建者：weihua.zheng@ele.me
# **  创建日期： 2016-10-19
# **
# **  ChangeLog
#     1. who when what
#
#***************************************************************************************************


##### 计算餐厅最近7天, 30天的营业信息

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'rank_open')
SELECT
    c.restaurant_id,
    'rank' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM
(
    SELECT
        a.restaurant_id,
        ARRAY(
          CONCAT('recent_7_open_days=', a.recent_7_open_days),
          CONCAT('recent_7_open_hours=', ROUND(a.recent_7_open_hours / 60, 2)),
          CONCAT('recent_30_open_days=', a.recent_30_open_days),
          CONCAT('recent_30_open_hours=', ROUND(a.recent_30_open_hours / 60, 2))
        ) AS info_array
    FROM
    (
        SELECT
            restaurant_id,
            COUNT(IF(DATEDIFF('${day}', date_dt) < 7 AND len_open_time > 0, 1, NULL)) AS recent_7_open_days,
            SUM(IF(DATEDIFF('${day}', date_dt) < 7, len_open_time, 0)) AS recent_7_open_hours,
            COUNT(IF(len_open_time > 0, 1, NULL)) AS recent_30_open_days,
            SUM(len_open_time) AS recent_30_open_hours
        FROM
            dw.dw_prd_restaurant_open_time
        WHERE
            dt = '${day}' AND date_dt > GET_DATE('${day}', -30)
        GROUP BY
            restaurant_id
    ) a
    LEFT SEMI JOIN
    (
        SELECT id AS restaurant_id
        FROM dw.dw_prd_restaurant_wide
        ---- 类型100是测试餐厅，1是正常的
        WHERE dt = '${day}' AND is_valid = 1 AND type != 100
    ) b
    ON a.restaurant_id = b.restaurant_id
) c
LATERAL VIEW
EXPLODE(c.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0';


##### 计算餐厅最近7天，30天的投诉量

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'rank_complaint')
SELECT
    c.restaurant_id,
    'rank' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM
(
    SELECT
        a.restaurant_id,
        ARRAY(
          CONCAT('recent_7_order_complain_cnt=',
            COUNT(IF(DATEDIFF('${day}', a.order_dt) < 7, b.order_id, NULL))
                ),
          CONCAT('recent_30_order_complain_cnt=',
            COUNT(b.order_id)
                )
        ) AS info_array
    FROM
    (
        SELECT id AS order_id, restaurant_id, dt AS order_dt
        FROM dw.dw_trd_order_wide_day
        WHERE dt > GET_DATE('${day}', -30) AND user_id != 886 AND order_status = 1
    ) a
    JOIN
    (
        SELECT order_id
        FROM rec.rec_prd_order_complaint_status
        WHERE dt > GET_DATE('${day}', -30) AND complaint_status = 1
        GROUP BY order_id
    ) b
    ON a.order_id = b.order_id
    GROUP BY
        a.restaurant_id
) c
LATERAL VIEW
EXPLODE(c.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0';


##### 计算餐厅最近7天，30天的催单数量

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'rank_remind')
SELECT
    c.restaurant_id,
    'rank' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM
(
    SELECT
        a.restaurant_id,
        ARRAY(
          CONCAT('recent_7_order_remind_cnt=',
            COUNT(IF(DATEDIFF('${day}', a.order_dt) < 7, b.order_id, NULL))
                ),
          CONCAT('recent_30_order_remind_cnt=',
            COUNT(b.order_id)
                )
        ) AS info_array
    FROM
    (
        SELECT id AS order_id, restaurant_id, dt AS order_dt
        FROM dw.dw_trd_order_wide_day
        WHERE dt > GET_DATE('${day}', -30) AND user_id != 886 AND order_status = 1
    ) a
    JOIN
    (
        SELECT order_id
        FROM dw.dw_trd_remind_order_record
        WHERE dt = '${day}' AND created_at > GET_DATE('${day}', -30)
        GROUP BY order_id
    ) b
    ON a.order_id = b.order_id
    GROUP BY
        a.restaurant_id
) c
LATERAL VIEW
EXPLODE(c.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0';


##### 计算餐厅最近7天，30天的退单数量

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'rank_refuse_order')
SELECT
    c.restaurant_id,
    'rank' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM
(
    SELECT
        a.restaurant_id,
        ARRAY(
          CONCAT('recent_7_user_refuse_order_cnt=',
            COUNT(DISTINCT IF(DATEDIFF('${day}', a.order_dt) < 7 AND b.process_group_new = '用户处理',
                              b.order_id, NULL))
            ),
          CONCAT('recent_7_rst_refuse_order_cnt=',
            COUNT(DISTINCT IF(DATEDIFF('${day}', a.order_dt) < 7 AND b.process_group_new = '餐厅处理',
                              b.order_id, NULL))
            ),
          CONCAT('recent_30_user_refuse_order_cnt=',
            COUNT(DISTINCT IF(b.process_group_new = '用户处理', b.order_id, NULL))
            ),
          CONCAT('recent_30_rst_refuse_order_cnt=',
            COUNT(DISTINCT IF(b.process_group_new = '餐厅处理', b.order_id, NULL))
            )
        ) AS info_array
    FROM
    (
        SELECT id AS order_id, restaurant_id, dt AS order_dt
        FROM dw.dw_trd_order_wide_day
        WHERE dt > GET_DATE('${day}', -30)
    ) a
    JOIN
    (
        SELECT
            order_id,
            CASE
              WHEN process_group = 1 AND from_status = 2 AND to_status = 3 THEN '用户处理'
              WHEN process_group in (2, 5, 8, 9, 11, 12, 13, 14)
                   AND from_status = 0 AND to_status = -1 THEN '餐厅处理'
              ELSE '其他'
            END AS process_group_new
        FROM
            dw.dw_trd_order_process_record
        WHERE
            dt = '${day}' and created_at > GET_DATE('${day}', -30)
    ) b
    ON a.order_id = b.order_id
    GROUP BY
        a.restaurant_id
) c
LATERAL VIEW
EXPLODE(c.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0';


INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'rank_001')
SELECT
    restaurant_id,
    'rank' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM(
    select
        restaurant_id,
        array(
            concat('rest_delivertime_avg=',res_avg_delivertime),
            concat('recent_30_is_rest_discount=',res_self_coupon),
            concat('recent_14_favor_cnt=',collection_num),
            concat('recent_14_favor_avg=',round(avg_collection_num,3)),
            concat('recent_14_returned_customer_cnt=',complaints_rate),
            concat('recent_30_rst_refuse_order_scale=',round(restaurant_withdraw_order_rate,3)),
            concat('recent_30_user_refuse_order_scale=',round(user_withdraw_order_rate,3)),
            concat('recent_30_order_remind_scale=',round(reminder_rate,3))
        ) as info_array
    from 
        st.st_bs_shop_portrait
    where
        dt='${day}'
) t
lateral view explode(t.info_array) tmp as item
where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0
UNION ALL
select
    restaurant_id,
    'rank' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
from(
    select
        restaurant_id,
        array(
            concat('rest_cost_distribution=',concat('"', res_range_of_cost, '"')),
            concat('rest_food_cate_distribution=',concat('"', res_newflavor_rate, '"'))
        ) as info_array
    from 
        st.st_bs_shop_portrait
    where
        dt='${day}'
) t
lateral view explode(t.info_array) tmp as item
where split(item,'=')[1]!='0' AND length(split(item,'=')[1])>0
;


##### sub task
##### copy newest data to dt = '3000-12-31'

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '3000-12-31', flag)
SELECT
    restaurant_id, top_category, attr_key, attr_value, is_json, update_time, flag
FROM
    dm.dm_ups_restaurant_item_info
WHERE
    dt = '${day}' AND flag IN ('rank_open', 'rank_complaint', 'rank_remind', 'rank_refuse_order', 'rank_001');