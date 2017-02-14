#***************************************************************************************************
# **  文件名称： dm_ups_user_info.sql
# **  功能描述：合并用户画像item
#
#***************************************************************************************************


##### 按照字母顺序对用户的信息进行合并json
---- 因为合并前会检测数据[user_id, top_category, attr_key]唯一，因此使用collect_list不会造成多值
---- 使用collect_list可以保证顺序

INSERT OVERWRITE TABLE dm.dm_ups_user_info PARTITION(dt = '${day}')
SELECT
    b.user_id,
    CONCAT('{', CONCAT_WS(',', COLLECT_LIST(b.category_value)), '}') AS profile_json
FROM
(
    SELECT
        a.user_id,
        a.top_category,
        CONCAT('\"', a.top_category, '\":{',
          CONCAT_WS(',',
             COLLECT_LIST(
                CONCAT('\"', a.attr_key, '\":',
                  CASE
                    WHEN a.is_json = 1 THEN attr_value
                    ELSE CONCAT('\"', a.attr_value, '\"')
                  END)
             )
          ), '}'
        ) AS category_value
    FROM
    (
        SELECT
            user_id, top_category, attr_key, attr_value, is_json
        FROM
            dm.dm_ups_user_item_info
        WHERE
            dt = '3000-12-31' AND 
            attr_value NOT IN ('-', '0', '0.0') AND 
            length(attr_value) > 0 AND
            attr_key NOT IN ('home_entry_statistic')
        DISTRIBUTE BY
            user_id
        SORT BY
            user_id, top_category, attr_key
    ) a
    GROUP BY
        a.user_id, a.top_category
) b
GROUP BY
    b.user_id;


INSERT OVERWRITE TABLE dm.dm_ups_user_info PARTITION(dt = '3000-12-31')
SELECT user_id, profile_json FROM dm.dm_ups_user_info WHERE dt = '${day}';

