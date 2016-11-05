#***************************************************************************************************
# **  文件名称： rec_hotfood_user_food_rec.sql
# **  功能描述： 合并买过的和推荐的热卖美食数据
# **  创建者： jiahao.dong
# **  创建日期： 2016-07-08
#***************************************************************************************************

insert overwrite table rec.rec_hotfood_user_info partition(dt='${day}', model='food_prefer') 
select *
from(	select user_id,'food_prefer', concat('{', concat_ws(',', collect_set(
		concat('\"', rec_food_name,'\":\{\"score\":', cast(score as string), ', \"is_fresh\":', is_fresh, '\}'))), '}') info,
		from_unixtime(unix_timestamp()) as time
		from rec.rec_hotfood_user_food_rec
		where model='food_prefer_assoc_rule'
		group by user_id
	union all
		select user_id,'food_prefer', concat('{', concat_ws(',', collect_set(
		concat('\"', food_name,'\":\{\"score\":', cast(rec_score as string), ', \"is_fresh\":', is_fresh, '\}'))), '}') info,
		from_unixtime(unix_timestamp()) as time
		from rec.rec_hotfood_user_food_rec 
		where model='food_prefer_part_buy_rec'
		group by user_id) t;