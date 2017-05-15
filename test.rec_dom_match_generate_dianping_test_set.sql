#***************************************************************************************************
# ** 文件名称： test.rec_dom_match_generate_dianping_test_set.sql
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-03-31
#***************************************************************************************************

DROP TABLE temp.rec_dom_match_generate_dianping_test_set_part1;
CREATE TABLE temp.rec_dom_match_generate_dianping_test_set_part1 AS
    SELECT
        a.ele_id,
        a.dianping_poi_id,
        a.geohash,
        b.restaurant_id as false_ele_id,
        a.ele_name as true_ele_name,
        b.ele_name as false_ele_name,
        a.dianping_name
    FROM(
        SELECT
            t1.ele_id,
            t1.dianping_poi_id,
            t2.geohash,
            t1.dianping_name,
            t2.ele_name
        FROM(
            SELECT
                a.ele_id,
                a.dianping_poi_id,
                b.name as dianping_name
            FROM(
                SELECT
                    ele_id,
                    dianping_poi_id
                FROM
                    test.rec_dom_input_data
                WHERE
                    dt = '2017-03-30' and
                    version = '+' and
                    dianping_poi_id is not null and
                    dianping_poi_id != 0
                    ) a
            JOIN(
                SELECT
                    id,
                    name
                FROM
                    dw.dw_ext_meituan_tuangou_restaurant
                WHERE
                    dt = '2017-03-30'
                ) b
            ON(
                a.dianping_poi_id = b.id
                )
            ) t1
        JOIN(
            SELECT
                restaurant_id,
                parse_json_object(profile_json, 'base.name', false) as ele_name,
                substr(parse_json_object(profile_json, 'base.geohash', false), 0, 7) as geohash
            FROM
                dm.dm_ups_restaurant_info
            WHERE
                dt = '3000-12-31'
            ) t2
        ON(
            t1.ele_id = t2.restaurant_id
            )
        ) a 
    JOIN(
        SELECT
            restaurant_id,
            parse_json_object(profile_json, 'base.name', false) as ele_name,
            substr(parse_json_object(profile_json, 'base.geohash', false), 0, 7) as geohash
        FROM
            dm.dm_ups_restaurant_info
        WHERE
            dt = '3000-12-31'
        ) b
    ON(
        a.geohash = b.geohash
        )
;


DROP TABLE temp.temp_rec_dom_match_generate_dianping_test_set;
CREATE TABLE temp.temp_rec_dom_match_generate_dianping_test_set AS
    SELECT
        ele_id,
        dianping_poi_id,
        ele_name,
        dianping_name,
        max(label) as label
    FROM(
        SELECT
            ele_id,
            dianping_poi_id,
            true_ele_name as ele_name,
            dianping_name,
            1 as label
        FROM
            temp.rec_dom_match_generate_dianping_test_set_part1

        UNION ALL
        SELECT
            false_ele_id as ele_id,
            dianping_poi_id,
            false_ele_name as ele_name,
            dianping_name,
            0 as label
        FROM
            temp.rec_dom_match_generate_dianping_test_set_part1
        ) t

    GROUP BY
        ele_id,
        dianping_poi_id,
        ele_name,
        dianping_name
;


DROP TABLE test.rec_dom_match_generate_dianping_test_set;
CREATE TABLE test.rec_dom_match_generate_dianping_test_set AS
    select
        ele_id,
        dianping_poi_id,
        label,
        ele_name,
        dianping_name,
        max(if(ele_name is null or dianping_name is null, 0, is_similar_poi(ele_name, dianping_name, 2))) as is_intersect
    from
        temp.temp_rec_dom_match_generate_dianping_test_set
    group by
        ele_id,
        dianping_poi_id,
        label,
        ele_name,
        dianping_name
    having
        is_intersect > 0
;




DROP TABLE test.100w_neg_check;
CREATE  TABLE `test.100w_neg_check`(
  `ele_id` string,
  `shop_id` string,
  `score` double,
  `is_match` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://bipcluster/data/hive_warehouse/test.db/100w_neg_check'
TBLPROPERTIES (
  'numFiles'='1',
  'transient_lastDdlTime'='1491383580',
  'COLUMN_STATS_ACCURATE'='true',
  'totalSize'='38462315',
  'numRows'='0',
  'rawDataSize'='0')




load data local inpath '/home/etl/chao.lish/100w/test_2017-04-04_dianping_for_train/total_result' into table test.100w_neg_check;



