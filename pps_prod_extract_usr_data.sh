. /etc/profile

#***************************************************************************************************
# ** 文件名称： pps_prod_extract_usr_data.sh
# ** 功能描述： pps数据生产，抽取redis所需的活跃用户数据
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-09
#***************************************************************************************************

day=`date -d "1 days ago"  +%Y-%m-%d`
dt=`date +%Y%m%d --date='1 days ago'`
dt_3day=`date +%Y%m%d --date='3 days ago'`

mkdir /home/dt.rec/rec_ext/project/pps_writedata/data/user
data_dir=/home/dt.rec/rec_ext/project/pps_writedata/data/user/${dt}

mkdir ${data_dir}
cd ${data_dir}

input_file=pps_user_info_data
split_file_name=${data_dir}/pps_user_info_data_part
usr_file_meta_list=${data_dir}/usr_file_meta_list

okay_file=${data_dir}/done.okay

rm *

hive -e "
    select
        concat(
            concat('user_id','\003',if(user_id is not null, user_id ,'null')),'\001',
            concat('email','\003',if(email is not null, email ,'null')),'\002',
            concat('gender','\003',if(gender is not null, gender ,'null')),'\002',
            concat('nick_name','\003',if(nick_name is not null, nick_name ,'null')),'\002',
            concat('pre_phone','\003',if(pre_phone is not null, pre_phone ,'null')),'\002',
            concat('create_time','\003',if(create_time is not null, create_time ,'null')),'\002',
            concat('source','\003',if(source is not null, source ,'null')),'\002',
            concat('brand','\003',if(brand is not null, brand ,'null')),'\002',
            concat('os_platform','\003',if(os_platform is not null, os_platform ,'null')),'\002',
            concat('os_version','\003',if(os_version is not null, os_version ,'null')),'\002',
            concat('model','\003',if(model is not null, model ,'null')),'\002',
            concat('id','\003',if(id is not null, id ,'null')),'\002',
            concat('resolution','\003',if(resolution is not null, resolution ,'null')),'\002',
            concat('category_prefer','\003',if(category_prefer is not null, category_prefer ,'null')),'\002',
            concat('hongbao_sensitive','\003',if(hongbao_sensitive is not null, hongbao_sensitive ,'null')),'\002',
            concat('price_sensitive','\003',if(price_sensitive is not null, price_sensitive ,'null')),'\002',
            concat('style_prefer','\003',if(style_prefer is not null, style_prefer ,'null')),'\002',
            concat('flavor_prefer','\003',if(flavor_prefer is not null, flavor_prefer ,'null')),'\002',
            concat('food_prefer','\003',if(food_prefer is not null, food_prefer ,'null')),'\002',
            concat('visit_address','\003',if(visit_address is not null, visit_address ,'null')),'\002',
            concat('last_visit_time','\003',if(last_visit_time is not null, last_visit_time ,'null')),'\002',
            concat('visit_time_prefer','\003',if(visit_time_prefer is not null, visit_time_prefer ,'null')),'\002',
            concat('visit_date_prefer','\003',if(visit_date_prefer is not null, visit_date_prefer ,'null')),'\002',
            concat('visit_province_id','\003',if(visit_province_id is not null, visit_province_id ,'null')),'\002',
            concat('first_visit_time','\003',if(first_visit_time is not null, first_visit_time ,'null')),'\002',
            concat('visit_city_id','\003',if(visit_city_id is not null, visit_city_id ,'null')),'\002',
            concat('order_subsidy','\003',if(order_subsidy is not null, order_subsidy ,'null')),'\002',
            concat('order_address','\003',if(order_address is not null, order_address ,'null')),'\002',
            concat('first_order_time','\003',if(first_order_time is not null, first_order_time ,'null')),'\002',
            concat('last_order_time','\003',if(last_order_time is not null, last_order_time ,'null')),'\002',
            concat('order_date_prefer','\003',if(order_date_prefer is not null, order_date_prefer ,'null')),'\002',
            concat('order_cnt','\003',if(order_cnt is not null, order_cnt ,'null')),'\002',
            concat('consume_level','\003',if(consume_level is not null, consume_level ,'null')),'\002',
            concat('order_avg_fee','\003',if(order_avg_fee is not null, order_avg_fee ,'null')),'\002',
            concat('order_province_id','\003',if(order_province_id is not null, order_province_id ,'null')),'\002',
            concat('order_hongbao','\003',if(order_hongbao is not null, order_hongbao ,'null')),'\002',
            concat('order_bu_flag','\003',if(order_bu_flag is not null, order_bu_flag ,'null')),'\002',
            concat('order_amt','\003',if(order_amt is not null, order_amt ,'null')),'\002',
            concat('point','\003',if(point is not null, point ,'null')),'\002',
            concat('order_time_prefer','\003',if(order_time_prefer is not null, order_time_prefer ,'null')),'\002',
            concat('order_city_id','\003',if(order_city_id is not null, order_city_id ,'null')),'\002',
            concat('is_sia','\003',if(is_sia is not null, is_sia ,'null')),'\002',
            concat('phone_number','\003',if(phone_number is not null, phone_number ,'null')),'\002',
            concat('delivery_type_prefer','\003',if(delivery_type_prefer is not null, delivery_type_prefer ,'null')),'\002',
            concat('recent_30_avg_delivery_fee','\003',if(recent_30_avg_delivery_fee is not null, recent_30_avg_delivery_fee ,'null')),'\002',
            concat('collect_rest_list','\003',if(collect_rest_list is not null, collect_rest_list ,'null')),'\002',
            concat('click_rest_list','\003',if(click_rest_list is not null, click_rest_list ,'null')),'\002',
            concat('order_rest_list','\003',if(order_rest_list is not null, order_rest_list ,'null')),'\002',
            concat('score_rest_list','\003',if(score_rest_list is not null, score_rest_list ,'null')),'\002',
            concat('order_again_rest_list','\003',if(order_again_rest_list is not null, order_again_rest_list ,'null')),'\002',
            concat('recent_30_active_day','\003',if(recent_30_active_day is not null, recent_30_active_day ,'null')),'\002',
            concat('recent_30_play_time','\003',if(recent_30_play_time is not null, recent_30_play_time ,'null')),'\002',
            concat('recent_30_order_cnt','\003',if(recent_30_order_cnt is not null, recent_30_order_cnt ,'null')),'\002',
            concat('recent_30_order_amt','\003',if(recent_30_order_amt is not null, recent_30_order_amt ,'null')),'\002',
            concat('delivery_priority','\003',if(delivery_priority is not null, delivery_priority ,'null')),'\002',
            concat('is_vip','\003',if(is_vip is not null, is_vip ,'null')),'\002',
            concat('phone_city_id','\003',if(phone_city_id is not null, phone_city_id ,'null')),'\002',
            concat('phone_city_name','\003',if(phone_city_name is not null, phone_city_name ,'null')),'\002',
            concat('phone_province_id','\003',if(phone_province_id is not null, phone_province_id ,'null')),'\002',
            concat('phone_province_name','\003',if(phone_province_name is not null, phone_province_name ,'null')),'\002',
            concat('hongbao_balance','\003',if(hongbao_balance is not null, hongbao_balance ,'null')),'\002',
            concat('recent_7_reminder_order_num','\003',if(recent_7_reminder_order_num is not null, recent_7_reminder_order_num ,'null')),'\002',
            concat('recent_7_reminder_order_rate','\003',if(recent_7_reminder_order_rate is not null, recent_7_reminder_order_rate ,'null')),'\002',
            concat('recent_7_withdraw_order_num','\003',if(recent_7_withdraw_order_num is not null, recent_7_withdraw_order_num ,'null')),'\002',
            concat('recent_7_withdraw_order_rate','\003',if(recent_7_withdraw_order_rate is not null, recent_7_withdraw_order_rate ,'null')),'\002',
            concat('rest_distance_avg','\003',if(rest_distance_avg is not null, rest_distance_avg ,'null')),'\002',
            concat('order_discount_rate','\003',if(order_discount_rate is not null, order_discount_rate ,'null')),'\002',
            concat('order_manjian_rate','\003',if(order_manjian_rate is not null, order_manjian_rate ,'null')),'\002',
            concat('recent_90_click_premium_rest_rate','\003',if(recent_90_click_premium_rest_rate is not null, recent_90_click_premium_rest_rate ,'null')),'\002',
            concat('recent_90_visit_dish_per_rest_avg','\003',if(recent_90_visit_dish_per_rest_avg is not null, recent_90_visit_dish_per_rest_avg ,'null')),'\002',
            concat('recent_30_is_new','\003',if(recent_30_is_new is not null, recent_30_is_new ,'null')),'\002',
            concat('order_interval_avg','\003',if(order_interval_avg is not null, order_interval_avg ,'null')),'\002',
            concat('order_interval_min','\003',if(order_interval_min is not null, order_interval_min ,'null')),'\002',
            concat('order_delivery_fee_rate','\003',if(order_delivery_fee_rate is not null, order_delivery_fee_rate ,'null')),'\002',
            concat('click_rest_sale_avg','\003',if(click_rest_sale_avg is not null, click_rest_sale_avg ,'null')),'\002',
            concat('click_rest_open_time_avg','\003',if(click_rest_open_time_avg is not null, click_rest_open_time_avg ,'null')),'\002',
            concat('click_rest_safety_level_avg','\003',if(click_rest_safety_level_avg is not null, click_rest_safety_level_avg ,'null')),'\002',
            concat('click_rest_score_avg','\003',if(click_rest_score_avg is not null, click_rest_score_avg ,'null')),'\002',
            concat('click_rest_desc_length_avg','\003',if(click_rest_desc_length_avg is not null, click_rest_desc_length_avg ,'null')),'\002',
            concat('click_rest_delivery_time_avg','\003',if(click_rest_delivery_time_avg is not null, click_rest_delivery_time_avg ,'null')),'\002',
            concat('click_rest_has_picture_rate','\003',if(click_rest_has_picture_rate is not null, click_rest_has_picture_rate ,'null')),'\002',
            concat('click_rest_discount_avg','\003',if(click_rest_discount_avg is not null, click_rest_discount_avg ,'null')),'\002',
            concat('rest_order_cat0_prefer','\003',if(rest_order_cat0_prefer is not null, rest_order_cat0_prefer ,'null')),'\002',
            concat('rest_order_cat1_prefer','\003',if(rest_order_cat1_prefer is not null, rest_order_cat1_prefer ,'null')),'\002',
            concat('rest_behavior','\003',if(rest_behavior is not null, rest_behavior ,'null')),'\002',
            concat('cat_profile','\003',if(cat_profile is not null, cat_profile ,'null')),'\002',
            concat('rest_prefer','\003',if(rest_prefer is not null, rest_prefer ,'null'))
        )
    from(    
        select
            case when t1.user_id is not null then concat('1:', t1.user_id) else null end as user_id,
            parse_json_object(profile_json,'base.email',false) as email,
            parse_json_object(profile_json,'base.gender',false) as gender,
            parse_json_object(profile_json,'base.nick_name',false) as nick_name,
            parse_json_object(profile_json,'base.pre_phone',false) as pre_phone,
            parse_json_object(profile_json,'base.create_time',false) as create_time,
            parse_json_object(profile_json,'base.source',false) as source,
            parse_json_object(profile_json,'device.brand',false) as brand,
            parse_json_object(profile_json,'device.os_platform',false) as os_platform,
            parse_json_object(profile_json,'device.os_version',false) as os_version,
            parse_json_object(profile_json,'device.model',false) as model,
            parse_json_object(profile_json,'device.id',false) as id,
            parse_json_object(profile_json,'device.resolution',false) as resolution,
            parse_json_object(profile_json,'rec.category_prefer',false) as category_prefer,
            parse_json_object(profile_json,'rec.hongbao_sensitive',false) as hongbao_sensitive,
            parse_json_object(profile_json,'rec.price_sensitive',false) as price_sensitive,
            parse_json_object(profile_json,'rec.style_prefer',false) as style_prefer,
            parse_json_object(profile_json,'rec.flavor_prefer',false) as flavor_prefer,
            parse_json_object(profile_json,'rec.food_prefer',false) as food_prefer,
            parse_json_object(profile_json,'log.visit_address',false) as visit_address,
            parse_json_object(profile_json,'log.last_visit_time',false) as last_visit_time,
            parse_json_object(profile_json,'log.visit_time_prefer',false) as visit_time_prefer,
            parse_json_object(profile_json,'log.visit_date_prefer',false) as visit_date_prefer,
            parse_json_object(profile_json,'log.visit_province_id',false) as visit_province_id,
            parse_json_object(profile_json,'log.first_visit_time',false) as first_visit_time,
            parse_json_object(profile_json,'log.visit_city_id',false) as visit_city_id,
            parse_json_object(profile_json,'trd.order_subsidy',false) as order_subsidy,
            parse_json_object(profile_json,'trd.order_address',false) as order_address,
            parse_json_object(profile_json,'trd.first_order_time',false) as first_order_time,
            parse_json_object(profile_json,'trd.last_order_time',false) as last_order_time,
            parse_json_object(profile_json,'trd.order_date_prefer',false) as order_date_prefer,
            parse_json_object(profile_json,'trd.order_cnt',false) as order_cnt,
            parse_json_object(profile_json,'trd.consume_level',false) as consume_level,
            parse_json_object(profile_json,'trd.order_avg_fee',false) as order_avg_fee,
            parse_json_object(profile_json,'trd.order_province_id',false) as order_province_id,
            parse_json_object(profile_json,'trd.order_hongbao',false) as order_hongbao,
            parse_json_object(profile_json,'trd.order_bu_flag',false) as order_bu_flag,
            parse_json_object(profile_json,'trd.order_amt',false) as order_amt,
            parse_json_object(profile_json,'trd.point',false) as point,
            parse_json_object(profile_json,'trd.order_time_prefer',false) as order_time_prefer,
            parse_json_object(profile_json,'trd.order_city_id',false) as order_city_id,
            parse_json_object(profile_json,'speciality.is_sia',false) as is_sia,
            parse_json_object(profile_json,'base.phone_number',false) as phone_number,
            parse_json_object(profile_json,'trd.delivery_type_prefer') as delivery_type_prefer,
            parse_json_object(profile_json,'trd.recent_30_avg_delivery_fee') as recent_30_avg_delivery_fee,
            parse_json_object(profile_json,'trd.collect_rest_list') as collect_rest_list,
            parse_json_object(profile_json,'trd.click_rest_list') as click_rest_list,
            parse_json_object(profile_json,'trd.order_rest_list') as order_rest_list,
            parse_json_object(profile_json,'trd.score_rest_list') as score_rest_list,
            parse_json_object(profile_json,'trd.order_again_rest_list') as order_again_rest_list,
            parse_json_object(profile_json,'trd.recent_30_active_day') as recent_30_active_day,
            parse_json_object(profile_json,'trd.recent_30_play_time') as recent_30_play_time,
            parse_json_object(profile_json,'trd.recent_30_order_cnt') as recent_30_order_cnt,
            parse_json_object(profile_json,'trd.recent_30_order_amt') as recent_30_order_amt,
            parse_json_object(profile_json,'speciality.delivery_priority') as delivery_priority,
            parse_json_object(profile_json,'base.is_vip') as is_vip,
            parse_json_object(profile_json,'base.phone_city_id') as phone_city_id,
            parse_json_object(profile_json,'base.phone_city_name') as phone_city_name,
            parse_json_object(profile_json,'base.phone_province_id') as phone_province_id,
            parse_json_object(profile_json,'base.phone_province_name') as phone_province_name,
            parse_json_object(profile_json,'trd.hongbao_balance') as hongbao_balance,
            parse_json_object(profile_json,'trd.recent_7_reminder_order_num') as recent_7_reminder_order_num,
            parse_json_object(profile_json,'trd.recent_7_reminder_order_rate') as recent_7_reminder_order_rate,
            parse_json_object(profile_json,'trd.recent_7_withdraw_order_num') as recent_7_withdraw_order_num,
            parse_json_object(profile_json,'trd.recent_7_withdraw_order_rate') as recent_7_withdraw_order_rate,
            parse_json_object(profile_json,'trd.rest_distance_avg') as rest_distance_avg,
            parse_json_object(profile_json,'trd.order_discount_rate') as order_discount_rate,
            parse_json_object(profile_json,'trd.order_manjian_rate') as order_manjian_rate,
            parse_json_object(profile_json,'trd.recent_90_click_premium_rest_rate') as recent_90_click_premium_rest_rate,
            parse_json_object(profile_json,'trd.recent_90_visit_dish_per_rest_avg') as recent_90_visit_dish_per_rest_avg,
            parse_json_object(profile_json,'trd.recent_30_is_new') as recent_30_is_new,
            parse_json_object(profile_json,'trd.order_interval_avg') as order_interval_avg,
            parse_json_object(profile_json,'trd.order_interval_min') as order_interval_min,
            parse_json_object(profile_json,'trd.order_delivery_fee_rate') as order_delivery_fee_rate,
            parse_json_object(profile_json,'trd.click_rest_sale_avg') as click_rest_sale_avg,
            parse_json_object(profile_json,'trd.click_rest_open_time_avg') as click_rest_open_time_avg,
            parse_json_object(profile_json,'trd.click_rest_safety_level_avg') as click_rest_safety_level_avg,
            parse_json_object(profile_json,'trd.click_rest_score_avg') as click_rest_score_avg,
            parse_json_object(profile_json,'trd.click_rest_desc_length_avg') as click_rest_desc_length_avg,
            parse_json_object(profile_json,'trd.click_rest_delivery_time_avg') as click_rest_delivery_time_avg,
            parse_json_object(profile_json,'trd.click_rest_has_picture_rate') as click_rest_has_picture_rate,
            parse_json_object(profile_json,'trd.click_rest_discount_avg') as click_rest_discount_avg,
            parse_json_object(profile_json,'rec.rest_order_cat0_prefer') as rest_order_cat0_prefer,
            parse_json_object(profile_json,'rec.rest_order_cat1_prefer') as rest_order_cat1_prefer,
            parse_json_object(profile_json,'rec.rest_behavior') as rest_behavior,
            parse_json_object(profile_json,'rec.cat_profile') as cat_profile,
            parse_json_object(profile_json,'rec.rest_prefer') as rest_prefer
        from(
            select
                user_id
            from(
                select
                    user_id,
                    count(*) as cnt
                from
                    dw.dw_log_app_pv_day_inc
                where
                    dt>=get_date('${day}',-5) and
                    user_id is not null
                group by
                    user_id
                sort by
                    cnt desc
                ) t
            limit 6000000
            ) t1
        join(
            select
                user_id,
                profile_json
            from
                dm.dm_ups_user_info
            where
                dt='3000-12-31'
            ) t2
        on(
            t1.user_id=t2.user_id
        )
    ) t
    where user_id is not null
;" > ${input_file}

rm ${split_file_name}_*
rm ${usr_file_meta_list}
cat ${input_file} | split ${input_file} -l 100000 ${split_file_name}_

for file in ${split_file_name}_*;
do
    file_name=`echo "${file}" | awk 'gsub("'"${data_dir}/"'","") {print $0}'`
    tar -zcvf ${file_name}.gz ${file_name} 
    echo "${file_name}.gz" >> ${usr_file_meta_list}
    rm ${file_name}
done