#***************************************************************************************************
# ** 文件名称： hive_pad.sh
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-15
#***************************************************************************************************


output_file='temp'

day_index=1
dt=`date -d -${day_index}day +%Y%m%d`
day=`date -d -${day_index}day +%Y-%m-%d`

echo dt
echo $day

hive -e "
    select 
        concat(
            concat('restaurant_id','\002',if(name is not null, restaurant_id ,'_N_')),'\001',
            concat('name','\002',if(name is not null, name ,'_N_')),'\001',
            concat('address','\002',if(address is not null, address ,'_N_')),'\001',
            concat('restaurant_type','\002',if(restaurant_type is not null, restaurant_type ,'_N_')),'\001',
            concat('latitude','\002',if(latitude is not null, latitude ,'_N_')),'\001',
            concat('longitude','\002',if(longitude is not null, longitude ,'_N_')),'\001',
            concat('geohash','\002',if(geohash is not null, geohash ,'_N_')),'\001',
            concat('city_id','\002',if(city_id is not null, city_id ,'_N_')),'\001',
            concat('city_name','\002',if(city_name is not null, city_name ,'_N_')),'\001',
            concat('min_deliver_amt','\002',if(min_deliver_amt is not null, min_deliver_amt ,'_N_')),'\001',
            concat('bu_flag','\002',if(bu_flag is not null, bu_flag ,'_N_')),'\001',
            concat('time_ensure_spent','\002',if(time_ensure_spent is not null, time_ensure_spent ,'_N_')),'\001',
            concat('time_ensure_discount','\002',if(time_ensure_discount is not null, time_ensure_discount ,'_N_')),'\001',
            concat('deliver_type','\002',if(deliver_type is not null, deliver_type ,'_N_')),'\001',
            concat('food_number','\002',if(food_number is not null, food_number ,'_N_')),'\001',
            concat('has_license','\002',if(has_license is not null, has_license ,'_N_')),'\001',
            concat('has_service_license','\002',if(has_service_license is not null, has_service_license ,'_N_')),'\001',
            concat('is_exclusive','\002',if(is_exclusive is not null, is_exclusive ,'_N_')),'\001',
            concat('is_sia','\002',if(is_sia is not null, is_sia ,'_N_')),'\001',
            concat('cat0_name','\002',if(cat0_name is not null, cat0_name ,'_N_')),'\001',
            concat('cat1_name','\002',if(cat1_name is not null, cat1_name ,'_N_')),'\001',
            concat('recent_7_open_days','\002',if(recent_7_open_days is not null, recent_7_open_days ,'_N_')),'\001',
            concat('recent_30_open_days','\002',if(recent_30_open_days is not null, recent_30_open_days ,'_N_')),'\001',
            concat('recent_7_open_hours','\002',if(recent_7_open_hours is not null, recent_7_open_hours ,'_N_')),'\001',
            concat('recent_30_open_hours','\002',if(recent_30_open_hours is not null, recent_30_open_hours ,'_N_')),'\001',
            concat('recent_7_order_complain_cnt','\002',if(recent_7_order_complain_cnt is not null, recent_7_order_complain_cnt ,'_N_')),'\001',
            concat('recent_30_order_complain_cnt','\002',if(recent_30_order_complain_cnt is not null, recent_30_order_complain_cnt ,'_N_')),'\001',
            concat('recent_7_order_remind_cnt','\002',if(recent_7_order_remind_cnt is not null, recent_7_order_remind_cnt ,'_N_')),'\001',
            concat('recent_30_order_remind_cnt','\002',if(recent_30_order_remind_cnt is not null, recent_30_order_remind_cnt ,'_N_')),'\001',
            concat('recent_7_user_refuse_order_cnt','\002',if(recent_7_user_refuse_order_cnt is not null, recent_7_user_refuse_order_cnt ,'_N_')),'\001',
            concat('recent_30_user_refuse_order_cnt','\002',if(recent_30_user_refuse_order_cnt is not null, recent_30_user_refuse_order_cnt ,'_N_')),'\001',
            concat('recent_7_rst_refuse_order_cnt','\002',if(recent_7_rst_refuse_order_cnt is not null, recent_7_rst_refuse_order_cnt ,'_N_')),'\001',
            concat('recent_30_rst_refuse_order_cnt','\002',if(recent_30_rst_refuse_order_cnt is not null, recent_30_rst_refuse_order_cnt ,'_N_')),'\001',
            concat('star_rating_5_cnt','\002',if(star_rating_5_cnt is not null, star_rating_5_cnt ,'_N_')),'\001',
            concat('star_rating_4_cnt','\002',if(star_rating_4_cnt is not null, star_rating_4_cnt ,'_N_')),'\001',
            concat('star_rating_3_cnt','\002',if(star_rating_3_cnt is not null, star_rating_3_cnt ,'_N_')),'\001',
            concat('star_rating_2_cnt','\002',if(star_rating_2_cnt is not null, star_rating_2_cnt ,'_N_')),'\001',
            concat('star_rating_1_cnt','\002',if(star_rating_1_cnt is not null, star_rating_1_cnt ,'_N_')),'\001',
            concat('recent_7_order_cnt','\002',if(recent_7_order_cnt is not null, recent_7_order_cnt ,'_N_')),'\001',
            concat('recent_30_order_cnt','\002',if(recent_30_order_cnt is not null, recent_30_order_cnt ,'_N_')),'\001',
            concat('recent_7_user_cnt','\002',if(recent_7_user_cnt is not null, recent_7_user_cnt ,'_N_')),'\001',
            concat('recent_30_user_cnt','\002',if(recent_30_user_cnt is not null, recent_30_user_cnt ,'_N_')),'\001',
            concat('recent_7_ord_usr_ratio','\002',if(recent_7_ord_usr_ratio is not null, recent_7_ord_usr_ratio ,'_N_')),'\001',
            concat('recent_30_ord_usr_ratio','\002',if(recent_30_ord_usr_ratio is not null, recent_30_ord_usr_ratio ,'_N_')),'\001',
            concat('recent_7_returned_customer_scale','\002',if(recent_7_returned_customer_scale is not null, recent_7_returned_customer_scale ,'_N_')),'\001',
            concat('recent_30_returned_customer_scale','\002',if(recent_30_returned_customer_scale is not null, recent_30_returned_customer_scale ,'_N_')),'\001',
            concat('recent_7_order_amt','\002',if(recent_7_order_amt is not null, recent_7_order_amt ,'_N_')),'\001',
            concat('recent_30_order_amt','\002',if(recent_30_order_amt is not null, recent_30_order_amt ,'_N_')),'\001',
            concat('recent_30_order_price_median','\002',if(recent_30_order_price_median is not null, recent_30_order_price_median ,'_N_')),'\001',
            concat('recent_30_order_price_stddev','\002',if(recent_30_order_price_stddev is not null, recent_30_order_price_stddev ,'_N_')),'\001',
            concat('recent_30_payment_amt','\002',if(recent_30_payment_amt is not null, recent_30_payment_amt ,'_N_')),'\001',
            concat('recent_30_payment_median','\002',if(recent_30_payment_median is not null, recent_30_payment_median ,'_N_')),'\001',
            concat('recent_30_payment_stddev','\002',if(recent_30_payment_stddev is not null, recent_30_payment_stddev ,'_N_')),'\001',
            concat('recent_30_eleme_subsidy_amt','\002',if(recent_30_eleme_subsidy_amt is not null, recent_30_eleme_subsidy_amt ,'_N_')),'\001',
            concat('recent_30_eleme_subsidy_median','\002',if(recent_30_eleme_subsidy_median is not null, recent_30_eleme_subsidy_median ,'_N_')),'\001',
            concat('recent_30_eleme_subsidy_stddev','\002',if(recent_30_eleme_subsidy_stddev is not null, recent_30_eleme_subsidy_stddev ,'_N_')),'\001',
            concat('recent_30_order_chargeback_scale','\002',if(recent_30_order_chargeback_scale is not null, recent_30_order_chargeback_scale ,'_N_')),'\001',
            concat('recent_30_order_unpaid_cnt','\002',if(recent_30_order_unpaid_cnt is not null, recent_30_order_unpaid_cnt ,'_N_')),'\001',
            concat('recent_30_order_unpaid_scale','\002',if(recent_30_order_unpaid_scale is not null, recent_30_order_unpaid_scale ,'_N_')),'\001',
            concat('recent_7_order_hongbao_preferential_avg','\002',if(recent_7_order_hongbao_preferential_avg is not null, recent_7_order_hongbao_preferential_avg ,'_N_')),'\001',
            concat('recent_7_order_hongbao_preferential_stddev','\002',if(recent_7_order_hongbao_preferential_stddev is not null, recent_7_order_hongbao_preferential_stddev ,'_N_')),'\001',
            concat('order_time_prefer','\002',if(order_time_prefer is not null, order_time_prefer ,'_N_')),'\001',
            concat('order_date_prefer','\002',if(order_date_prefer is not null, order_date_prefer ,'_N_')),'\001',
            concat('order_month_distribution','\002',if(order_month_distribution is not null, order_month_distribution ,'_N_')),'\001',
            concat('food_top_10','\002',if(food_top_10 is not null, food_top_10 ,'_N_'))
        )
    from(
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
            parse_json_object(profile_json,'trade.food_top_10',false) as food_top_10
        from
            dm.dm_ups_restaurant_info
        where
            dt='3000-12-31'
        ) t
;" > ${output_file}

