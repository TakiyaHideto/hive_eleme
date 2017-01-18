#***************************************************************************************************
# **  文件名称： dq_dm_ups_restaurant_info.sql
# **  功能描述：检查最新的dt是否有重复值，保证[user_id, top_category, attr_key]唯一
#
# **  创建者：weihua.zheng@ele.me
# **  创建日期： 2016-08-16 18：20：00
# **
# **  ChangeLog
#
#***************************************************************************************************

INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt = '${day}', p_dq_table = 'dm_ups_restaurant_item_info', p_id = 1)
SELECT
    a.restaurant AS id,
    'dm_ups_user_item_info' AS dq_table,
    '${day}' AS date_at,
    'jiahao.dong' AS dq_man,
    'multi-restauranr_id:top_category:attr_key' AS dq_type,
    'restaurant, top_category, attr_key' AS dq_column,
    1 AS is_error,
    'multi-restaurant:top_category:attr_key exists' AS  error_info,
    'error' AS error_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
FROM
(
    SELECT
        restaurant, top_category, attr_key, COUNT(*) AS number
    FROM
        dm.dm_ups_restaurant_item_info
    WHERE
        dt = '3000-12-31'
    GROUP BY
        restaurant, top_category, attr_key
) a
WHERE
    a.number > 1
LIMIT 1;

