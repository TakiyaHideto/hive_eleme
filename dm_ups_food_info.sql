#***************************************************************************************************
# ** 文件名称： dm_ups_food_info.sql
# ** 功能描述： 食品画像汇总
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-06
#***************************************************************************************************


INSERT OVERWRITE TABLE dm.dm_ups_food_info PARTITION(dt = '${day}')
    SELECT
        b.food_id,
        CONCAT('{', CONCAT_WS(',', COLLECT_LIST(b.category_value)), '}') AS profile_json
    FROM
    (
        SELECT
            a.food_id,
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
                food_id, 
                top_category, 
                attr_key, 
                attr_value, 
                is_json
            FROM
                dm.dm_ups_food_item_info
            WHERE
                dt = '3000-12-31' AND 
                attr_value NOT IN ('-', '0')
            DISTRIBUTE BY
                food_id
            SORT BY
                food_id, 
                top_category, 
                attr_key
        ) a
        GROUP BY
            a.food_id, a.top_category
    ) b
    GROUP BY
        b.food_id;


INSERT OVERWRITE TABLE dm.dm_ups_food_info PARTITION(dt = '3000-12-31') 
    SELECT 
        food_id, 
        profile_json 
    FROM 
        dm.dm_ups_food_item_info 
    WHERE 
        dt = '${day}'
;

