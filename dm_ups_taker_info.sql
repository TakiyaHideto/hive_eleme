#***************************************************************************************************
# **  文件名称： dm_ups_taker_info.sql
# **  功能描述：合并骑手画像item
#
#***************************************************************************************************


##### 按照字母顺序对用户的信息进行合并json
---- 因为合并前会检测数据[taker_id, top_category, attr_key]唯一，因此使用collect_list不会造成多值
---- 使用collect_list可以保证顺序

INSERT OVERWRITE TABLE dm.dm_ups_taker_info PARTITION(dt = '${day}')
SELECT
    b.taker_id,
    CONCAT('{', CONCAT_WS(',', COLLECT_LIST(b.category_value)), '}') AS profile_json
FROM
(
    SELECT
        a.taker_id,
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
            taker_id, top_category, attr_key, attr_value, is_json
        FROM
            dm.dm_ups_taker_item_info
        WHERE
            dt = '3000-12-31' AND 
            attr_value NOT IN ('-', '0', '0.0') AND 
            length(attr_value) > 0
        DISTRIBUTE BY
            taker_id
        SORT BY
            taker_id, top_category, attr_key
    ) a
    GROUP BY
        a.taker_id, a.top_category
) b
GROUP BY
    b.taker_id;


INSERT OVERWRITE TABLE dm.dm_ups_taker_info PARTITION(dt = '3000-12-31')
SELECT taker_id, profile_json FROM dm.dm_ups_taker_info WHERE dt = '${day}';

