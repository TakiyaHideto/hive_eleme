. /etc/profile 
#***************************************************************************************************
# ** 文件名称： pps_prod_extract_rest_data.sql
# ** 功能描述： pps数据生产，抽取rocksdb所需的餐厅数据
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-02-17
#***************************************************************************************************

day=`date -d "1 days ago"  +%Y-%m-%d`
dt=`date +%Y%m%d --date='1 days ago'`
dt_3day=`date +%Y%m%d --date='3 days ago'`

data_dir=/home/dt.rec/rec_ext/project/pps_writedata/data/restaurant/${dt}

mkdir ${data_dir}
cd ${data_dir}

input_file=pps_restaurant_info_data
split_file_name=${data_dir}/pps_restaurant_info_data_part
rest_file_meta_list=${data_dir}/rest_file_meta_list

okay_file=${data_dir}/done.okay

rm *

hive -e "
select 
    concat(
        concat('restaurant_id','\003',if(restaurant_id is not null, restaurant_id ,'_N_')),'\001',
        concat('name','\003',if(name is not null, name ,'_N_')),'\002',
        concat('address','\003',if(address is not null, address ,'_N_')),'\002',
        concat('restaurant_type','\003',if(restaurant_type is not null, restaurant_type ,'_N_')),'\002',
        concat('latitude','\003',if(latitude is not null, latitude ,'_N_')),'\002',
        concat('longitude','\003',if(longitude is not null, longitude ,'_N_')),'\002',
        concat('geohash','\003',if(geohash is not null, geohash ,'_N_')),'\002',
        concat('city_id','\003',if(city_id is not null, city_id ,'_N_')),'\002',
        concat('city_name','\003',if(city_name is not null, city_name ,'_N_')),'\002',
        concat('min_deliver_amt','\003',if(min_deliver_amt is not null, min_deliver_amt ,'_N_')),'\002',
        concat('bu_flag','\003',if(bu_flag is not null, bu_flag ,'_N_')),'\002',
        concat('time_ensure_spent','\003',if(time_ensure_spent is not null, time_ensure_spent ,'_N_')),'\002',
        concat('time_ensure_discount','\003',if(time_ensure_discount is not null, time_ensure_discount ,'_N_')),'\002',
        concat('deliver_type','\003',if(deliver_type is not null, deliver_type ,'_N_')),'\002',
        concat('food_number','\003',if(food_number is not null, food_number ,'_N_')),'\002',
        concat('has_license','\003',if(has_license is not null, has_license ,'_N_')),'\002',
        concat('has_service_license','\003',if(has_service_license is not null, has_service_license ,'_N_')),'\002',
        concat('is_exclusive','\003',if(is_exclusive is not null, is_exclusive ,'_N_')),'\002',
        concat('is_sia','\003',if(is_sia is not null, is_sia ,'_N_')),'\002',
        concat('cat0_name','\003',if(cat0_name is not null, cat0_name ,'_N_')),'\002',
        concat('cat1_name','\003',if(cat1_name is not null, cat1_name ,'_N_')),'\002',
        concat('recent_7_open_days','\003',if(recent_7_open_days is not null, recent_7_open_days ,'_N_')),'\002',
        concat('recent_30_open_days','\003',if(recent_30_open_days is not null, recent_30_open_days ,'_N_')),'\002',
        concat('recent_7_open_hours','\003',if(recent_7_open_hours is not null, recent_7_open_hours ,'_N_')),'\002',
        concat('recent_30_open_hours','\003',if(recent_30_open_hours is not null, recent_30_open_hours ,'_N_')),'\002',
        concat('recent_7_order_complain_cnt','\003',if(recent_7_order_complain_cnt is not null, recent_7_order_complain_cnt ,'_N_')),'\002',
        concat('recent_30_order_complain_cnt','\003',if(recent_30_order_complain_cnt is not null, recent_30_order_complain_cnt ,'_N_')),'\002',
        concat('recent_7_order_remind_cnt','\003',if(recent_7_order_remind_cnt is not null, recent_7_order_remind_cnt ,'_N_')),'\002',
        concat('recent_30_order_remind_cnt','\003',if(recent_30_order_remind_cnt is not null, recent_30_order_remind_cnt ,'_N_')),'\002',
        concat('recent_7_user_refuse_order_cnt','\003',if(recent_7_user_refuse_order_cnt is not null, recent_7_user_refuse_order_cnt ,'_N_')),'\002',
        concat('recent_30_user_refuse_order_cnt','\003',if(recent_30_user_refuse_order_cnt is not null, recent_30_user_refuse_order_cnt ,'_N_')),'\002',
        concat('recent_7_rst_refuse_order_cnt','\003',if(recent_7_rst_refuse_order_cnt is not null, recent_7_rst_refuse_order_cnt ,'_N_')),'\002',
        concat('recent_30_rst_refuse_order_cnt','\003',if(recent_30_rst_refuse_order_cnt is not null, recent_30_rst_refuse_order_cnt ,'_N_')),'\002',
        concat('star_rating_5_cnt','\003',if(star_rating_5_cnt is not null, star_rating_5_cnt ,'_N_')),'\002',
        concat('star_rating_4_cnt','\003',if(star_rating_4_cnt is not null, star_rating_4_cnt ,'_N_')),'\002',
        concat('star_rating_3_cnt','\003',if(star_rating_3_cnt is not null, star_rating_3_cnt ,'_N_')),'\002',
        concat('star_rating_2_cnt','\003',if(star_rating_2_cnt is not null, star_rating_2_cnt ,'_N_')),'\002',
        concat('star_rating_1_cnt','\003',if(star_rating_1_cnt is not null, star_rating_1_cnt ,'_N_')),'\002',
        concat('recent_7_order_cnt','\003',if(recent_7_order_cnt is not null, recent_7_order_cnt ,'_N_')),'\002',
        concat('recent_30_order_cnt','\003',if(recent_30_order_cnt is not null, recent_30_order_cnt ,'_N_')),'\002',
        concat('recent_7_user_cnt','\003',if(recent_7_user_cnt is not null, recent_7_user_cnt ,'_N_')),'\002',
        concat('recent_30_user_cnt','\003',if(recent_30_user_cnt is not null, recent_30_user_cnt ,'_N_')),'\002',
        concat('recent_7_ord_usr_ratio','\003',if(recent_7_ord_usr_ratio is not null, recent_7_ord_usr_ratio ,'_N_')),'\002',
        concat('recent_30_ord_usr_ratio','\003',if(recent_30_ord_usr_ratio is not null, recent_30_ord_usr_ratio ,'_N_')),'\002',
        concat('recent_7_returned_customer_scale','\003',if(recent_7_returned_customer_scale is not null, recent_7_returned_customer_scale ,'_N_')),'\002',
        concat('recent_30_returned_customer_scale','\003',if(recent_30_returned_customer_scale is not null, recent_30_returned_customer_scale ,'_N_')),'\002',
        concat('recent_7_order_amt','\003',if(recent_7_order_amt is not null, recent_7_order_amt ,'_N_')),'\002',
        concat('recent_30_order_amt','\003',if(recent_30_order_amt is not null, recent_30_order_amt ,'_N_')),'\002',
        concat('recent_30_order_price_median','\003',if(recent_30_order_price_median is not null, recent_30_order_price_median ,'_N_')),'\002',
        concat('recent_30_order_price_stddev','\003',if(recent_30_order_price_stddev is not null, recent_30_order_price_stddev ,'_N_')),'\002',
        concat('recent_30_payment_amt','\003',if(recent_30_payment_amt is not null, recent_30_payment_amt ,'_N_')),'\002',
        concat('recent_30_payment_median','\003',if(recent_30_payment_median is not null, recent_30_payment_median ,'_N_')),'\002',
        concat('recent_30_payment_stddev','\003',if(recent_30_payment_stddev is not null, recent_30_payment_stddev ,'_N_')),'\002',
        concat('recent_30_eleme_subsidy_amt','\003',if(recent_30_eleme_subsidy_amt is not null, recent_30_eleme_subsidy_amt ,'_N_')),'\002',
        concat('recent_30_eleme_subsidy_median','\003',if(recent_30_eleme_subsidy_median is not null, recent_30_eleme_subsidy_median ,'_N_')),'\002',
        concat('recent_30_eleme_subsidy_stddev','\003',if(recent_30_eleme_subsidy_stddev is not null, recent_30_eleme_subsidy_stddev ,'_N_')),'\002',
        concat('recent_30_order_chargeback_scale','\003',if(recent_30_order_chargeback_scale is not null, recent_30_order_chargeback_scale ,'_N_')),'\002',
        concat('recent_30_order_unpaid_cnt','\003',if(recent_30_order_unpaid_cnt is not null, recent_30_order_unpaid_cnt ,'_N_')),'\002',
        concat('recent_30_order_unpaid_scale','\003',if(recent_30_order_unpaid_scale is not null, recent_30_order_unpaid_scale ,'_N_')),'\002',
        concat('recent_7_order_hongbao_preferential_avg','\003',if(recent_7_order_hongbao_preferential_avg is not null, recent_7_order_hongbao_preferential_avg ,'_N_')),'\002',
        concat('recent_7_order_hongbao_preferential_stddev','\003',if(recent_7_order_hongbao_preferential_stddev is not null, recent_7_order_hongbao_preferential_stddev ,'_N_')),'\002',
        concat('order_time_prefer','\003',if(order_time_prefer is not null, order_time_prefer ,'_N_')),'\002',
        concat('order_date_prefer','\003',if(order_date_prefer is not null, order_date_prefer ,'_N_')),'\002',
        concat('order_month_distribution','\003',if(order_month_distribution is not null, order_month_distribution ,'_N_')),'\002',
        concat('food_top_10','\003',if(food_top_10 is not null, food_top_10 ,'_N_'))
        )
from(
    select
        case when restaurant_id is not null then concat('2:', restaurant_id) else null end as restaurant_id,
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
;" > ${input_file}

rm ${split_file_name}_*
rm ${rest_file_meta_list}
cat ${input_file} | split ${input_file} -l 100000 ${split_file_name}_

for file in ${split_file_name}_*;
do
    file_name=`echo "${file}" | awk 'gsub("'"${data_dir}/"'","") {print $0}'`
    tar -zcvf ${file_name}.gz ${file_name} 
    echo "${file_name}.gz" >> ${rest_file_meta_list}
    rm ${file_name}
done 





