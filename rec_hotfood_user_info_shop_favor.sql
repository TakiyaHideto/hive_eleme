#***************************************************************************************************
# **  Profile Service @ dt.rec
#
# **  文件名称：rec_hotfood_user_info_shop_favor.sql
# **  功能描述：推荐用户收藏的餐厅
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-09-28
#
#***************************************************************************************************

drop table temp.temp_rec_user_favor_restaurant_user_info;
create table temp.temp_rec_user_favor_restaurant_user_info as
select user_id, 
    concat('{',concat_ws(',',collect_set(concat('\"',cast(restaurant_id as string),'\":{','\"created_at\":"',created_at,'\",\"is_favor\":1', '}'))),'}') as shop_favor
from dw.dw_com_favored_restaurant
where dt='${day}' and datediff('${day}', created_at)<60
group by user_id;

insert overwrite table rec.rec_hotfood_user_info partition(dt='${day}', model='shop_favor')
select user_id,'shop_favor', shop_favor, from_unixtime(unix_timestamp()) 
from temp.temp_rec_user_favor_restaurant_user_info ;
 
 