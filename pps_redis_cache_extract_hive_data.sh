. /etc/profile

#***************************************************************************************************
# ** 文件名称： pps_redis_cache_extract_hive_data.sh
# ** 功能描述： pps服务，向redis写活跃用户数据
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-09
#***************************************************************************************************

cd /home/dt.rec/rec_ext/project/pps_writedata

data_dir='/home/dt.rec/rec_ext/project/pps_writedata/'
input_file='/home/dt.rec/rec_ext/project/pps_writedata/active_user_info_list'
input_file_gz='/home/dt.rec/rec_ext/project/pps_writedata/active_user_info_list.gz'
split_file_name='/home/dt.rec/rec_ext/project/pps_writedata/active_user_info_list_part'
user_file_meta_list='/home/dt.rec/rec_ext/project/pps_writedata/user_file_meta_list'

okay_file='/home/dt.rec/rec_ext/project/pps_writedata/done.okay'

start_time=`date`

day=`date -d "1 days ago"  +%Y-%m-%d`
dt=`date +%Y%m%d --date='1 days ago'`
cp active_user_info_list ./backup/active_user_info_list_${dt}

echo $day

touch ${okay_file}
echo "-----------------------" >> $okay_file
echo `date`": start" >> $okay_file

hive -e "
    select
        t1.user_id,
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
        parse_json_object(profile_json,'rec.rest_order_cat0_prefer') as click_rest_discount_avg,
        parse_json_object(profile_json,'rec.rest_order_cat1_prefer') as click_rest_discount_avg,
        parse_json_object(profile_json,'rec.rest_behavior') as click_rest_discount_avg
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
                dt>=get_date('${day}',-8) and
                user_id is not null
            group by
                user_id
            sort by
                cnt desc
            ) t
        limit 5000000
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
;" > ${input_file}
echo `date`": finish extracting data" >> $okay_file


rm ${split_file_name}_*
rm ${user_file_meta_list}
cat ${input_file} | head -5000000 | split ${input_file} -l 100000 ${split_file_name}_
echo `date`": finish splitting file" >> $okay_file

# tar -zcvf ${input_file_gz} ${split_file_name}_*
for file in ${split_file_name}_*;
do
    file_name=`echo "${file}" | awk 'gsub("'"$data_dir"'","") {print $0}'`
    tar -zcvf ${file_name}.gz ${file_name} 
    echo "${file_name}.gz" >> ${user_file_meta_list}
done 
echo `date`": finish compressing file" >> $okay_file 

echo `date`": end" >> $okay_file
echo -e "-----------------------\n" >> $okay_file


. ~/.bash_profile

#***************************************************************************************************
# ** 文件名称： pps_transfer_cache.sh
# ** 功能描述： pps服务，转化数据格式，便于local cache写入
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-01-04
#***************************************************************************************************


cd /home/dt.rec/rec_ext/project/pps_writedata/transfer_cache

day=`date -d "1 days ago"  +%Y-%m-%d`
dt=`date +%Y%m%d --date='1 days ago'`

data_dir='/home/dt.rec/rec_ext/project/pps_writedata/transfer_cache/'
local_cache_raw_file='active_user_info_list_local'
local_cache_format_file='active_user_info_list.cache'
split_file_name='active_user_info_list.cache.part'
user_file_meta_list='user_cache_meta_file'

type_user=1
type_shop=2

cat ../active_user_info_list | head -1000000 > ${local_cache_raw_file}

java -cp InsertInfoToRedisCluster-1.0-SNAPSHOT-jar-with-dependencies.jar me.ele.dt.pps.kit.TransferFormatCacheFile ${local_cache_raw_file} ${local_cache_format_file} ${type_user}

rm ${split_file_name}_*
rm ${user_file_meta_list}
split ${local_cache_format_file} -l 100000 ${split_file_name}_

for file in ${split_file_name}_*;
do
    file_name=`echo "${file}"`
    tar -zcvf ${file_name}.gz ${file_name}
    echo "${file_name}.gz" >> ${user_file_meta_list}
done
