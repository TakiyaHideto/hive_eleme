#***************************************************************************************************
# **  文件名称： dq_dm_ups_user_info_inc.sql
# **  功能描述：检查最新的dt是否有非法格式的json，保证[json各层级的key]唯一
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-08-16 18：20：00
# **
# **  ChangeLog
#
#***************************************************************************************************

INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt = '${day}', p_dq_table = 'dm_ups_user_info', p_id = 1)
SELECT
    a.user_id AS id,
    'dm_ups_user_info' AS dq_table,
    '${day}' AS date_at,
    '' AS dq_man,
    'multi-json_key:third_level' AS dq_type,
    'multi-json_key in third level' AS dq_column,
    1 AS is_error,
    'multi-json_key:third_level exists' AS  error_info,
    'error' AS error_type,
    FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
FROM
(
    SELECT
        user_id, 
        SUM(CASE WHEN length(parse_json_object(profile_json, '$.base.create_time')) > 8 THEN 1 ELSE 0 END) AS num
    FROM
        dm.dm_ups_user_item_info
    WHERE
        dt = '3000-12-31'
    GROUP BY
        user_id, top_category, attr_key
    HAVING
        num = 0
) a
WHERE
    a.number > 1
LIMIT 1;

