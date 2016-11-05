#***************************************************************************************************
# **  Profile Service @ dt.rec
#
# **  文件名称：rec_prd_restaurant_basic_info.sql
# **  功能描述：商铺基本信息属性
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-10-11
#
#***************************************************************************************************


drop table temp.temp_rec_prd_restaurant_basic_info;
create table temp.temp_rec_prd_restaurant_basic_info as
select t1.id as restaurant_id, 
    max(t1.agent_fee) as agent_fee, 
    max(t1.min_deliver_amount) as min_deliver_amount, 
    max(t1.recent_food_popularity) as recent_food_popularity, 
    max(t1.recent_food_popularity_limited) as recent_food_popularity_limited,
    max(t2.food_cnt) as food_cnt, 
    max(t2.image_cnt) as image_cnt,
    max(case when food_cnt=1 then 0.3 when food_cnt=2 then 0.6 when food_cnt>=3 then 1.0 end) as food_score,
    max(case when image_cnt=0 then 0.2 when image_cnt=1 then 0.4 when image_cnt=2 then 0.6 when image_cnt>=3 then 1 end) as image_score,
    max(case when min_deliver_amount<=35 then 1 else(case when (1-(min_deliver_amount-35)*0.02)>=0.3 then (1-(min_deliver_amount-35)*0.02) else 0.3 end) end) as min_deliver_amount_score,
    max(case when agent_fee<=10 then 1 else(case when (1-(agent_fee-10)*0.05)>=0.3 then (1-(agent_fee-10)*0.05) else 0.3 end) end) as agent_fee_score 
from(
    select id, agent_fee, min_deliver_amount, recent_food_popularity, bound_data(recent_food_popularity,0,250) as recent_food_popularity_limited
    from dw.dw_prd_restaurant
    where dt='${day}'
) t1
join(
    select restaurant_id, 
        count(distinct case when stock_enabled=1 then id else null end) as food_cnt, 
        count(distinct case when stock_enabled=1 then image_hash else null end) as image_cnt
    from dw.dw_prd_food
    where dt='${day}' 
    group by restaurant_id
) t2
on t1.id=t2.restaurant_id
where t2.food_cnt>0
group by t1.id;

insert overwrite table rec.rec_prd_restaurant_basic_info partition(dt='${day}')
select restaurant_id, agent_fee, min_deliver_amount, recent_food_popularity, food_cnt, image_cnt,
    round(recent_food_popularity_limited*food_score*image_score*min_deliver_amount_score*agent_fee_score,3) as base_algo_rank_score_without_random
from temp.temp_rec_prd_restaurant_basic_info;
