#***************************************************************************************************
# **  文件名称： dm_mdl_guess_food_style_tag_evaluation_day.sql
# **  功能描述： 评价猜你喜欢各个tag的效果
# **  创建者： jiahao.dong
# **  创建日期： 2016-09-13
#***************************************************************************************************

drop table dm.dm_mdl_guess_food_style_tag_evaluation_day;
create table dm.dm_mdl_guess_food_style_tag_evaluation_day(
dt string, 
tab_name string,
tab_index int,
click_per_pv double,
click_per_uv double,
cvr_uv double,
cvr_pv double
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;


drop table temp.temp_guess_tag_click_04;
create table temp.temp_guess_tag_click_04 as
select * from
(select dt, eleme_device_id, session_id, activity_id, 0 tab_index, split(parse_json_object(activity_param, 'tab_name'), ',')[0] tab_name  
from dw.dw_log_app_pv_day_inc where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886
union all 
select dt, eleme_device_id, session_id, activity_id, 1 tab_index, split(parse_json_object(activity_param, 'tab_name'), ',')[1] tab_name  
from dw.dw_log_app_pv_day_inc where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886
union all 
select dt, eleme_device_id, session_id, activity_id, 2 tab_index, split(parse_json_object(activity_param, 'tab_name'), ',')[2] tab_name  
from dw.dw_log_app_pv_day_inc where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886
union all 
select dt, eleme_device_id, session_id, activity_id, 3 tab_index, split(parse_json_object(activity_param, 'tab_name'), ',')[3] tab_name  
from dw.dw_log_app_pv_day_inc where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886
union all 
select dt, eleme_device_id, session_id, activity_id, 4 tab_index, split(parse_json_object(activity_param, 'tab_name'), ',')[4] tab_name  
from dw.dw_log_app_pv_day_inc where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886
union all 
select dt, eleme_device_id, session_id, activity_id, 5 tab_index, split(parse_json_object(activity_param, 'tab_name'), ',')[5] tab_name  
from dw.dw_log_app_pv_day_inc where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886
union all 
select dt, eleme_device_id, session_id, activity_id, 6 tab_index, split(parse_json_object(activity_param, 'tab_name'), ',')[6] tab_name  
from dw.dw_log_app_pv_day_inc where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886)a
where length(a.tab_name)>0;

insert overwrite table dm.dm_mdl_guess_food_style_tag_evaluation_day
select a.dt, a.tab_name, a.tab_index,
round(count(c.eleme_device_id)/count(b.eleme_device_id),4) as click_per_pv,
round(count(distinct c.eleme_device_id)/count(distinct b.eleme_device_id),4) as click_per_uv,
round(sum(d.is_buy)/count(distinct b.eleme_device_id),4) as cvr_uv,
round(sum(d.is_buy)/count(b.eleme_device_id),4) as cvr_pv
from(
    select * from temp.temp_guess_tag_click_04
)a
left join(
    select distinct dt, eleme_device_id, session_id, activity_id, parse_json_object(activity_param, 'tab_name') tab_name  
    from dw.dw_log_app_pv_day_inc 
    where datediff('${day}',dt)<=15 and activity_id=2616 and user_id<>886
)b 
on a.session_id=b.session_id
left join(
    select distinct dt, eleme_device_id, session_id, activity_id, parse_json_object(activity_param, 'tab_name') tab_name  
    from dw.dw_log_app_pv_day_inc 
    where datediff('${day}',dt)<=15 and activity_id=2617 and user_id<>886
)c 
on b.session_id=c.session_id
left join(
    select session_id, tab_name, is_buy 
    from dw.dw_log_guess_click_day_inc 
    where datediff('${day}',dt)<=15 and is_click=1 and user_id<>886
)d 
on c.session_id=d.session_id
group by a.dt, a.tab_name, a.tab_index;
