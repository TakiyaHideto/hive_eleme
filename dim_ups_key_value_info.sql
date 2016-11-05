#***************************************************************************************************
# ** 文件名称： dim_ups_key_value_info.sql
# ** 功能描述：
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-09-01
#***************************************************************************************************

DROP TABLE temp.temp_ups_user_json_key_extraction;
CREATE TABLE temp.temp_ups_user_json_key_extraction(
top_category STRING,
key STRING,
json_key STRING,
value STRING
) PARTITIONED BY (part STRING);

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_style')
SELECT t.top_category, t.attr_key AS key, value as json_key, null as value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_style' and attr_value!='\\{\\}'
) t
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
WHERE value not like '暂无'
GROUP BY t.top_category, t.attr_key, value;

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_food')
SELECT t.top_category, t.attr_key AS key, value as json_key, null as value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_food' and attr_value!='{}'
) t
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
WHERE value not like '暂无'
GROUP BY t.top_category, t.attr_key, value;

-- -- INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_cooking')
-- -- SELECT t.top_category, t.attr_key AS key, value as json_key, null as value
-- -- FROM(
-- -- SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
-- -- FROM dm.dm_ups_user_item_info
-- -- WHERE dt='3000-12-31' and flag='rec_cooking' and attr_value!='\\{\\}'
-- -- ) t
-- -- LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
-- -- GROUP BY t.top_category, t.attr_key, value;

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_flavor')
SELECT t.top_category, t.attr_key AS key, value as json_key, null as value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_flavor' and attr_value!='{}'
) t
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
WHERE value not like '暂无'
GROUP BY t.top_category, t.attr_key, value;

INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part='rec_category')
SELECT t.top_category, t.attr_key AS key, value as json_key, null as value
FROM(
SELECT top_category, attr_key, get_json_keys(attr_value) AS json_keys
FROM dm.dm_ups_user_item_info
WHERE dt='3000-12-31' and flag='rec_category' and attr_value!='{}'
) t
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
WHERE value not like '暂无'
GROUP BY t.top_category, t.attr_key, value;


INSERT OVERWRITE TABLE temp.temp_ups_user_json_key_extraction PARTITION(part = 'other')
SELECT
    t.top_category,
    t.attr_key,
    IF(t.is_json == 1, value, '') AS json_key,
    IF(t.is_json == 0, value, '') AS value
FROM
(
    SELECT
        top_category, attr_key,
        CASE
          WHEN is_json = 1 THEN get_json_keys(attr_value)
          ELSE ARRAY(attr_value)
        END AS json_keys,
        is_json
    FROM
        dm.dm_ups_user_item_info
    WHERE
        dt = '3000-12-31' AND flag IN ('device_info', 'log_visit_prf', 'trd_visit_prf') AND
        attr_key IN ('network_type', 'os_platform',
        'visit_date_prefer', 'visit_time_prefer',
        'order_date_prefer', 'order_time_prefer', 'order_bu_flag')
) t 
LATERAL VIEW EXPLODE(t.json_keys) tmp AS value
GROUP BY
    t.top_category, t.attr_key, IF(t.is_json == 1, value, ''), IF(t.is_json == 0, value, '');


INSERT OVERWRITE TABLE dim.dim_ups_key_value_info
SELECT
    a.top_category,
    CONCAT('$.', a.top_category, '.', a.key) AS key,
    a.json_key,
    a.value
FROM
(
    SELECT
        top_category, key, json_key, value
    FROM
        temp.temp_ups_user_json_key_extraction
    WHERE
        part is not null
    GROUP BY
        top_category, key, json_key, value
) a;

