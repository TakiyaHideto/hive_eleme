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


-- sub task 2: 
    -- info from platform_dw.sr_shop_rank_indicator
drop table temp.temp_shop_rank_indicator;
create table temp.temp_shop_rank_indicator as 
    select 
        restaurant_id,
        like_score as rating_score_avg
    from
        platform_dw.sr_shop_rank_indicator
    where
        dt='${day}'
;

-- sub task 3:
    -- info from dw.dw_com_eleme_order_rate
drop table temp.temp_com_eleme_order_rate;
create table temp.temp_com_eleme_order_rate as
    select
        a.restaurant_id,
        sum(b.is_bad_rated) recent_30_negtive_comment_cnt,
        sum(case when b.is_bad_rated is not null then 1 else 0 end) as comment_cnt,
        sum(b.is_bad_rated)/sum(case when b.is_bad_rated is not null then 1 else 0 end) as recent_30_negtive_comment_scale
    from(
        select 
            * 
        from 
            dw.dw_trd_order_wide_day
        where 
            dt>=get_date('${day}',-30) and 
            dt<='${day}' and 
            order_status=1
        ) a
    inner join(
        select
            order_id,
            max(case when service_rating<3 then 1 else 0 end) is_bad_rated
        from 
            dw.dw_com_eleme_order_rate
        where 
            dt='${day}' and 
            to_date(rated_at)>=date_add('${day}',-30) and 
            to_date(rated_at)<date_add('${day}',1)
        group by 
            order_id
        ) b
    on (
        a.id=b.order_id
        )
    group by 
        a.restaurant_id
;    


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
            CONCAT('recent_30_negtive_comment_cnt=', recent_30_negtive_comment_cnt),
            CONCAT('recent_30_negtive_comment_scale=', recent_30_negtive_comment_scale),
            CONCAT('comment_cnt=', comment_cnt)
          ) AS info_array
    FROM
        temp.temp_com_eleme_order_rate

    UNION ALL
    SELECT
        restaurant_id AS restaurant_id,
        ARRAY(
            CONCAT('rating_score_avg=', rating_score_avg)
          ) AS info_array
    FROM
        temp.temp_shop_rank_indicator
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


