#***************************************************************************************************
# **  文件名称： dm_ups_food_info_h2c_beta.sql
# **  功能描述： 食品画像导入Cassandra beta环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-12-06
# **
# **  ChangeLog
#***************************************************************************************************


set mapred.max.split.size=128000000;
insert into table dm_test.dm_ups_food_info_h2c_beta
select
restaurant_id,
parse_json_object(profile_json,'base.name',false) as name,
parse_json_object(profile_json,'base.restaurant_id',false) as restaurant_id,
parse_json_object(profile_json,'base.price_original',false) as price_original,
parse_json_object(profile_json,'base.price_current',false) as price_current,
parse_json_object(profile_json,'base.price_changed_at',false) as price_changed_at,
parse_json_object(profile_json,'base.description',false) as description,
parse_json_object(profile_json,'base.is_new',false) as is_new,
parse_json_object(profile_json,'base.is_featured',false) as is_featured,
parse_json_object(profile_json,'base.is_gum',false) as is_gum,
parse_json_object(profile_json,'base.is_spicy',false) as is_spicy,
parse_json_object(profile_json,'base.has_activity',false) as has_activity,
parse_json_object(profile_json,'base.sold_out',false) as sold_out,
parse_json_object(profile_json,'base.packing_fee',false) as packing_fee,

parse_json_object(profile_json,'trd.order_cnt_7',false) as order_cnt_7,
parse_json_object(profile_json,'trd.order_cnt_30',false) as order_cnt_7,
parse_json_object(profile_json,'trd.cnt_7',false) as cnt_7,
parse_json_object(profile_json,'trd.cnt_30',false) as cnt_30,

parse_json_object(profile_json,'tag.cat_0',false) as cat_0,
parse_json_object(profile_json,'tag.cat_1',false) as cat_1,
parse_json_object(profile_json,'tag.cat_2',false) as cat_2,
parse_json_object(profile_json,'tag.flavor',false) as flavor,
parse_json_object(profile_json,'tag.cooking_method',false) as cooking_method,
parse_json_object(profile_json,'tag.tag_function',false) as tag_function,
parse_json_object(profile_json,'tag.tag_scene',false) as tag_scene

from dm.dm_ups_food_info where dt='3000-12-31';

