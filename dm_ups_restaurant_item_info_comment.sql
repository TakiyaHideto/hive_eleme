#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_restaurant_item_info_comment.sql
# **  功能描述：
#         1. 得到餐厅的星级评价信息
#         2. 得到餐厅的印象评价标签
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-10-19
# **
# **  ChangeLog：
#
#***************************************************************************************************


-- sub task 1: 获取餐厅评价星级信息
drop table temp.temp_ups_restaurant_ranking_star;
create table temp.temp_ups_restaurant_ranking_star as
select id as restaurant_id, 
    max(num_rating_5) as star_rating_5_cnt, max(num_rating_4) as star_rating_4_cnt, 
    max(num_rating_3) as star_rating_3_cnt, max(num_rating_2) as star_rating_2_cnt, 
    max(num_rating_1) as star_rating_1_cnt
from dw.dw_prd_restaurant
where dt='${day}'
group by id;


-- sub task 2: 获取用户印象评价标签

-- sub task 4: import data to ups 
INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '${day}', flag = 'comment')
SELECT
    a.restaurant_id,
    'comment' AS top_category,
    SPLIT(item, '=')[0] AS attr_key,
    SPLIT(item, '=')[1] AS attr_value,
    0 AS is_json,
    '${day}' AS update_time
FROM
(
    SELECT
        restaurant_id AS restaurant_id,
        ARRAY(
          CONCAT('star_rating_5_cnt=', star_rating_5_cnt),
          CONCAT('star_rating_4_cnt=', star_rating_4_cnt),
          CONCAT('star_rating_3_cnt=', star_rating_3_cnt),
          CONCAT('star_rating_2_cnt=', star_rating_2_cnt),
          CONCAT('star_rating_1_cnt=', star_rating_1_cnt)
        ) AS info_array
    FROM
        temp.temp_ups_restaurant_ranking_star

    UNION ALL
    SELECT
        restaurant_id AS restaurant_id,
        ARRAY(
          CONCAT('rating_score_avg=', like_score),
          CONCAT('recent_30_negtive_comment_cnt=', bad_rating_num),
          CONCAT('recent_30_negtive_comment_scale=', bad_rating_rate),
          CONCAT('comment_cnt=', comment_num)
          ) AS info_array
    FROM
        st.st_bs_shop_portrait
    WHERE
        dt = '${day}'
) a
LATERAL VIEW
EXPLODE(a.info_array) mytable AS item
WHERE
    SPLIT(item, '=')[1] != '0' AND LENGTH(SPLIT(item, '=')[1]) > 0;




##### sub task
##### copy newest data to dt = '3000-12-31'

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dm.dm_ups_restaurant_item_info PARTITION(dt = '3000-12-31', flag = 'comment')
SELECT
    restaurant_id, top_category, attr_key, attr_value, is_json, update_time
FROM
    dm.dm_ups_restaurant_item_info
WHERE
    dt = '${day}' AND flag = 'comment' AND attr_value != '0';


