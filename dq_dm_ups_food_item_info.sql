INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt = '${day}', p_dq_table = 'dm_ups_food_item_info', p_id = 1)
SELECT
    a.food_id AS id,
    'dm_ups_food_item_info' AS dq_table,
    '${day}' AS date_at,
    'jiahao.dong' AS dq_man,
    'multi-food_id:top_category:attr_key' AS dq_type,
    'food_id, top_category, attr_key' AS dq_column,
    1 AS is_error,
    'multi-food_id:top_category:attr_key exists' AS  error_info,
    'error' AS error_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
FROM
(
    SELECT
        food_id, top_category, attr_key, COUNT(*) AS number
    FROM
        dm.dm_ups_food_item_info
    WHERE
        dt = '3000-12-31'
    GROUP BY
        food_id, top_category, attr_key
) a
WHERE
    a.number > 1
LIMIT 1;
