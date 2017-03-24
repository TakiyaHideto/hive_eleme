#***************************************************************************************************
# **  文件名称： dm_ups_restaurant_info_h2c_prod_bjc1.sql
# **  功能描述： 餐厅画像导入Cassandra beta环境
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-12-01
# **
# **  ChangeLog
#***************************************************************************************************


set mapred.max.split.size=128000000;
insert into table dm_test.dm_ups_restaurant_info_h2c_prod_bjc1
select
restaurant_id,
parse_json_object(profile_json,'base.name',false) as name,
parse_json_object(profile_json,'base.address',false) as address,
parse_json_object(profile_json,'base.restaurant_type',false) as restaurant_type,
parse_json_object(profile_json,'base.latitude',false) as latitude,
parse_json_object(profile_json,'base.longitude',false) as longitude,
parse_json_object(profile_json,'base.geohash',false) as geohash,
parse_json_object(profile_json,'base.city_id',false) as city_id,
parse_json_object(profile_json,'base.city_name',false) as city_name,
parse_json_object(profile_json,'base.min_deliver_amt',false) as min_deliver_amt,
-- parse_json_object(profile_json,'base.deliver_fee',false) as deliver_fee,
parse_json_object(profile_json,'base.bu_flag',false) as bu_flag,
parse_json_object(profile_json,'base.time_ensure_spent',false) as time_ensure_spent,
parse_json_object(profile_json,'base.time_ensure_discount',false) as time_ensure_discount,
parse_json_object(profile_json,'base.deliver_type',false) as deliver_type,
parse_json_object(profile_json,'base.food_number',false) as food_number,
parse_json_object(profile_json,'base.has_license',false) as has_license,
parse_json_object(profile_json,'base.has_service_license',false) as has_service_license,
parse_json_object(profile_json,'base.is_exclusive',false) as is_exclusive,
parse_json_object(profile_json,'base.is_sia',false) as is_sia,

parse_json_object(profile_json,'category.cat0_name',false) as cat0_name,
parse_json_object(profile_json,'category.cat1_name',false) as cat1_name,

parse_json_object(profile_json,'rank.recent_7_open_days',false) as recent_7_open_days,
parse_json_object(profile_json,'rank.recent_30_open_days',false) as recent_30_open_days,
parse_json_object(profile_json,'rank.recent_7_open_hours',false) as recent_7_open_hours,
parse_json_object(profile_json,'rank.recent_30_open_hours',false) as recent_30_open_hours,
parse_json_object(profile_json,'rank.recent_7_order_complain_cnt',false) as recent_7_order_complain_cnt,
parse_json_object(profile_json,'rank.recent_30_order_complain_cnt',false) as recent_30_order_complain_cnt,
parse_json_object(profile_json,'rank.recent_7_order_remind_cnt',false) as recent_7_order_remind_cnt,
parse_json_object(profile_json,'rank.recent_30_order_remind_cnt',false) as recent_30_order_remind_cnt,
parse_json_object(profile_json,'rank.recent_7_user_refuse_order_cnt',false) as recent_7_user_refuse_order_cnt,
parse_json_object(profile_json,'rank.recent_30_user_refuse_order_cnt',false) as recent_30_user_refuse_order_cnt,
parse_json_object(profile_json,'rank.recent_7_rst_refuse_order_cnt',false) as recent_7_rst_refuse_order_cnt,
parse_json_object(profile_json,'rank.recent_30_rst_refuse_order_cnt',false) as recent_30_rst_refuse_order_cnt,

parse_json_object(profile_json,'comment.star_rating_5_cnt',false) as star_rating_5_cnt,
parse_json_object(profile_json,'comment.star_rating_4_cnt',false) as star_rating_4_cnt,
parse_json_object(profile_json,'comment.star_rating_3_cnt',false) as star_rating_3_cnt,
parse_json_object(profile_json,'comment.star_rating_2_cnt',false) as star_rating_2_cnt,
parse_json_object(profile_json,'comment.star_rating_1_cnt',false) as star_rating_1_cnt,

parse_json_object(profile_json,'trade.recent_7_order_cnt',false) as recent_7_order_cnt,
parse_json_object(profile_json,'trade.recent_30_order_cnt',false) as recent_30_order_cnt,
parse_json_object(profile_json,'trade.recent_7_user_cnt',false) as recent_7_user_cnt,
parse_json_object(profile_json,'trade.recent_30_user_cnt',false) as recent_30_user_cnt,
parse_json_object(profile_json,'trade.recent_7_ord_usr_ratio',false) as recent_7_ord_usr_ratio,
parse_json_object(profile_json,'trade.recent_30_ord_usr_ratio',false) as recent_30_ord_usr_ratio,
parse_json_object(profile_json,'trade.recent_7_returned_customer_scale',false) as recent_7_returned_customer_scale,
parse_json_object(profile_json,'trade.recent_30_returned_customer_scale',false) as recent_30_returned_customer_scale,
parse_json_object(profile_json,'trade.recent_7_order_amt',false) as recent_7_order_amt,
parse_json_object(profile_json,'trade.recent_30_order_amt',false) as recent_30_order_amt,
parse_json_object(profile_json,'trade.recent_30_order_price_median',false) as recent_30_order_price_median,
parse_json_object(profile_json,'trade.recent_30_order_price_stddev',false) as recent_30_order_price_stddev,
parse_json_object(profile_json,'trade.recent_30_payment_amt',false) as recent_30_payment_amt,
parse_json_object(profile_json,'trade.recent_30_payment_median',false) as recent_30_payment_median,
parse_json_object(profile_json,'trade.recent_30_payment_stddev',false) as recent_30_payment_stddev,
parse_json_object(profile_json,'trade.recent_30_eleme_subsidy_amt',false) as recent_30_eleme_subsidy_amt,
parse_json_object(profile_json,'trade.recent_30_eleme_subsidy_median',false) as recent_30_eleme_subsidy_median,
parse_json_object(profile_json,'trade.recent_30_eleme_subsidy_stddev',false) as recent_30_eleme_subsidy_stddev,
parse_json_object(profile_json,'trade.recent_30_order_chargeback_scale',false) as recent_30_order_chargeback_scale,
parse_json_object(profile_json,'trade.recent_30_order_unpaid_cnt',false) as recent_30_order_unpaid_cnt,
parse_json_object(profile_json,'trade.recent_30_order_unpaid_scale',false) as recent_30_order_unpaid_scale,
parse_json_object(profile_json,'trade.recent_7_order_hongbao_preferential_avg',false) as recent_7_order_hongbao_preferential_avg,
parse_json_object(profile_json,'trade.recent_7_order_hongbao_preferential_stddev',false) as recent_7_order_hongbao_preferential_stddev,
parse_json_object(profile_json,'trade.order_time_prefer',false) as order_time_prefer,
parse_json_object(profile_json,'trade.order_date_prefer',false) as order_date_prefer,
parse_json_object(profile_json,'trade.order_month_distribution',false) as order_month_distribution,
parse_json_object(profile_json,'trade.food_top_10',false) as food_top_10,

parse_json_object(profile_json,'rank.rest_food_cate_distribution',false) as rest_food_cate_distribution,
parse_json_object(profile_json,'log.recent_30_alltime_exp_ctr',false) as recent_30_alltime_exp_ctr,
parse_json_object(profile_json,'log.recent_30_alltime_exp_cnt',false) as recent_30_alltime_exp_cnt,
parse_json_object(profile_json,'log.recent_30_meal_exp_ctr',false) as recent_30_meal_exp_ctr,
parse_json_object(profile_json,'log.recent_30_meal_exp_cnt',false) as recent_30_meal_exp_cnt,
parse_json_object(profile_json,'log.recent_30_aftertea_exp_ctr',false) as recent_30_aftertea_exp_ctr,
parse_json_object(profile_json,'log.recent_30_aftertea_exp_cnt',false) as recent_30_aftertea_exp_cnt,
parse_json_object(profile_json,'log.recent_30_nightsnack_exp_ctr',false) as recent_30_nightsnack_exp_ctr,
parse_json_object(profile_json,'log.recent_30_nightsnack_exp_cnt',false) as recent_30_nightsnack_exp_cnt,
parse_json_object(profile_json,'trade.user_platform_order_cnt',false) as rest_delivertime_avg,
parse_json_object(profile_json,'rank.rest_delivertime_avg',false) as rest_delivertime_avg,
parse_json_object(profile_json,'base.food_price_avg',false) as food_price_avg,

parse_json_object(profile_json,'base.security_level',false) as security_level,
parse_json_object(profile_json,'rank.recent_30_is_rest_discount',false) as recent_30_is_rest_discount,
parse_json_object(profile_json,'trade.recent_30_top_3_food_sales_scale',false) as recent_30_top_3_food_sales_scale,
parse_json_object(profile_json,'trade.recent_14_order_cnt',false) as recent_14_order_cnt,
parse_json_object(profile_json,'rank.recent_14_favor_cnt',false) as recent_14_favor_cnt,
parse_json_object(profile_json,'rank.recent_14_favor_avg',false) as recent_14_favor_avg,
parse_json_object(profile_json,'trade.recent_14_returned_customer_cnt',false) as recent_14_returned_customer_cnt,
parse_json_object(profile_json,'trade.recent_30_rest_subsidy_median',false) as recent_30_rest_subsidy_median,
parse_json_object(profile_json,'comment.rating_score_avg',false) as rating_score_avg,
parse_json_object(profile_json,'base.is_premium',false) as is_premium,
parse_json_object(profile_json,'base.is_hummer',false) as is_hummer,
parse_json_object(profile_json,'base.food_has_picture_scale',false) as food_has_picture_scale,
parse_json_object(profile_json,'base.food_has_picture_cnt',false) as food_has_picture_cnt,
parse_json_object(profile_json,'base.is_certification',false) as is_certification,
parse_json_object(profile_json,'base.is_new',false) as is_new,
parse_json_object(profile_json,'base.is_royalty',false) as is_royalty,
parse_json_object(profile_json,'base.is_gka',false) as is_gka,
parse_json_object(profile_json,'base.is_controlled_by_eleme',false) as is_controlled_by_eleme,
parse_json_object(profile_json,'base.is_rescued',false) as is_rescued,
parse_json_object(profile_json,'rank.recent_30_order_complain_scale',false) as recent_30_order_complain_scale,
parse_json_object(profile_json,'rank.recent_30_rst_refuse_order_scale',false) as recent_30_rst_refuse_order_scale,
parse_json_object(profile_json,'rank.recent_30_user_refuse_order_scale',false) as recent_30_user_refuse_order_scale,
parse_json_object(profile_json,'comment.recent_30_negtive_comment_cnt',false) as recent_30_negtive_comment_cnt,
parse_json_object(profile_json,'comment.comment_cnt',false) as comment_cnt,
parse_json_object(profile_json,'rank.recent_30_order_remind_scale',false) as recent_30_order_remind_scale

from dm.dm_ups_restaurant_info where dt='3000-12-31';

