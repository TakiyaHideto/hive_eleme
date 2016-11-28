#***************************************************************************************************
# ** 文件名称： dm_ups_restaurant_info.sql
# ** 功能描述： 餐厅画像拼接json
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-10-24
#***************************************************************************************************


INSERT OVERWRITE TABLE dm.dm_ups_restaurant_info PARTITION(dt = '${day}')
SELECT
    b.restaurant_id,
    CONCAT('{', CONCAT_WS(',', COLLECT_LIST(b.category_value)), '}') AS profile_json
FROM
(
    SELECT
        a.restaurant_id,
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
            restaurant_id, top_category, attr_key, attr_value, is_json
        FROM
            dm.dm_ups_restaurant_item_info
        WHERE
            dt = '3000-12-31' AND attr_value NOT IN ('-', '0')
        DISTRIBUTE BY
            restaurant_id
        SORT BY
            restaurant_id, top_category, attr_key
    ) a
    GROUP BY
        a.restaurant_id, a.top_category
) b
GROUP BY
    b.restaurant_id;


INSERT OVERWRITE TABLE dm.dm_ups_restaurant_info PARTITION(dt = '3000-12-31') 
SELECT restaurant_id, profile_json FROM dm.dm_ups_restaurant_info WHERE dt = '${day}';

