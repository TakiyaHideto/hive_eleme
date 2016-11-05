#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_restaurant_item_info_comment.sql
# **  功能描述：
#         1. 得到餐厅的星级评价信息
#		  2. 得到餐厅的印象评价标签
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


-- sub task 3: 汇总各维度用户评价信息
drop table temp.temp_ups_restaurant_comment_info;
create table temp.temp_ups_restaurant_comment_info as 
select restaurant_id, 
    'comment' as top_category,
    'rank_star' as attr_key,
    concat('{',
        concat_ws(',',
            concat('"star_rating_5_cnt":', '"', star_rating_5_cnt, '"'),
            concat('"star_rating_4_cnt":', '"', star_rating_4_cnt, '"'),
            concat('"star_rating_3_cnt":', '"', star_rating_3_cnt, '"'),
            concat('"star_rating_2_cnt":', '"', star_rating_2_cnt, '"'),
            concat('"star_rating_1_cnt":', '"', star_rating_1_cnt, '"')
        ),
        '}'
    ) as comment_info
from temp.temp_ups_restaurant_ranking_star;


-- sub task 4: import data to ups 
insert overwrite table dm.dm_ups_restaurant_item_info PARTITION(dt='${day}', flag='comment')
select restaurant_id,
    top_category,
    attr_key, 
    comment_info as attr_value,
    '1' as is_json,
    '${day}' as update_time
from temp.temp_ups_restaurant_comment_info;

insert overwrite table dm.dm_ups_restaurant_item_info PARTITION(dt='3000-12-31', flag='comment')
select restaurant_id,
    top_category,
    attr_key, 
    comment_info as attr_value,
    '1' as is_json,
    '${day}' as update_time
from temp.temp_ups_restaurant_comment_info;