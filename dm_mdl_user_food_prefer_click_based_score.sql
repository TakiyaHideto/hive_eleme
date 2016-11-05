#***************************************************************************************************
# **  文件名称： dm_mdl_user_food_prefer_click_based_score.sql
# **  功能描述： 通过对用户对全量食品的点击，计算食品偏好
# **  创建者： jiahao.dong
# **  创建日期： 2016-09-13
#***************************************************************************************************

drop table dm.dm_mdl_user_food_prefer_click_based_score;
create table dm.dm_mdl_user_food_prefer_click_based_score(
user_id string, 
food_id string,
food_name string,
last_click_time string,
click_cnt int,
price double,
score double
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ; 

set mapred.max.split.size=350000000;
drop table temp.temp_user_food_click_info_month;
create table temp.temp_user_food_click_info_month as
select user_id, parse_json_object(activity_param,"dish_id") as food_id, max(log_time) as last_click_time, count(*) as click_cnt
from dw.dw_log_app_pv_day_inc
where datediff("${day}",dt)<30
group by user_id, parse_json_object(activity_param,"dish_id")
having user_id is not null and parse_json_object(activity_param,"dish_id") is not null and user_id<>886;

drop table temp.temp_user_click_food_info_global;
create table temp.temp_user_click_food_info_global as
select t1.user_id, t1.food_id, t1.last_click_time, t1.click_cnt, t2.price
from temp.temp_user_food_click_info_month t1
join(
    select id as food_id, price
    from dw.dw_prd_food
    where dt='${day}'
) t2
on t1.food_id=t2.food_id;

insert overwrite table dm.dm_mdl_user_food_prefer_click_based_score
select t1.user_id, t1.food_id, t2.food_name, t1.last_click_time, t1.click_cnt, t1.price, round(1/(1+exp(-t1.score)),2) as score
from (
    select user_id, food_id, last_click_time, click_cnt, price,
        bound_data(price,2,50)*click_cnt/(datediff("${day}",last_click_time)+1) as score
    from temp.temp_user_click_food_info_global
) t1
left outer join (
    select food_id, food_name
    from dm.dm_mdl_food_name_normalize_day
    where dt='${day}'
) t2
on t1.food_id=t2.food_id;


