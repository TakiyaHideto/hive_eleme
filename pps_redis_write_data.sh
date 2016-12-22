
. /etc/profile

#***************************************************************************************************
# ** 文件名称： pps_redis_write_data.sh
# ** 功能描述： pps服务，向redis写活跃用户数据
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-09
#***************************************************************************************************


# input_file='/home/master/workspace/data/mail/active_user_feature_data'
input_file='/home/dt.rec/rec_ext/project/pps_writedata/active_user_info_list'

okay_file='/home/dt.rec/rec_ext/project/pps_writedata/done.okay'

start_time=`date`

day=`date -d "1 days ago"  +%Y-%m-%d`
echo $day

hive -e "
    select
        t1.user_id,
        parse_json_object(profile_json,'rec.category_prefer',false) as category_prefer,
        parse_json_object(profile_json,'trd.consume_level',false) as consume_level,
        parse_json_object(profile_json,'base.create_time',false) as create_time,
        parse_json_object(profile_json,'speciality.delivery_priority') as delivery_priority,
        parse_json_object(profile_json,'base.email',false) as email,
        parse_json_object(profile_json,'trd.first_order_time',false) as first_order_time,
        parse_json_object(profile_json,'rec.flavor_prefer',false) as flavor_prefer,
        parse_json_object(profile_json,'rec.food_prefer',false) as food_prefer,
        parse_json_object(profile_json,'base.gender',false) as gender,
        parse_json_object(profile_json,'rec.hongbao_sensitive',false) as hongbao_sensitive,
        parse_json_object(profile_json,'speciality.is_sia',false) as is_sia,
        '0' as is_vip,
        parse_json_object(profile_json,'trd.last_order_time',false) as last_order_time,
        parse_json_object(profile_json,'base.nick_name',false) as nick_name,
        parse_json_object(profile_json,'trd.order_address',false) as order_address,
        parse_json_object(profile_json,'trd.order_amt',false) as order_amt,
        parse_json_object(profile_json,'trd.order_avg_fee',false) as order_avg_fee,
        parse_json_object(profile_json,'trd.order_city_id',false) as order_city_id,
        parse_json_object(profile_json,'trd.order_cnt',false) as order_cnt,
        parse_json_object(profile_json,'trd.order_date_prefer',false) as order_date_prefer,
        parse_json_object(profile_json,'trd.order_province_id',false) as order_province_id,
        parse_json_object(profile_json,'trd.order_time_prefer',false) as order_time_prefer,
        parse_json_object(profile_json,'device.os_platform',false) as os_platform,
        parse_json_object(profile_json,'device.os_version',false) as os_version,
        parse_json_object(profile_json,'base.phone_number',false) as phone_number,
        parse_json_object(profile_json,'base.pre_phone',false) as pre_phone,
        parse_json_object(profile_json,'rec.price_sensitive',false) as price_sensitive,
        parse_json_object(profile_json,'base.source',false) as source,
        parse_json_object(profile_json,'rec.style_prefer',false) as style_prefer  
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
        limit 200000
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


touch ${okay_file}
stop_time=`date`

echo $start_time >> $okay_file
echo $stop_time >> $okay_file
echo "-----------------------" >> $okay_file













