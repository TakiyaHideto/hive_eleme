#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_flavor_prefer.sql
# ** 功能描述： 生成用户口味偏好，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-15
#***************************************************************************************************

drop table temp.temp_mdl_user_food_flavor_day_sub1;
create table temp.temp_mdl_user_food_flavor_day_sub1 as
    select 
        a.user_id, 
        b.flavor, 
        count(distinct order_id) as order_cnt, 
        sum(total_price) as total_price_total, 
        max(created_at) as last_order_time_max, 
        -- sum(user_month_order_num) as user_month_order_num_total, 
        '${day}' as record_day
    from (
        select
            user_id,
            order_id,
            entity_id as food_id,
            food_name,
            total_price,
            created_at
            from(
                select
                    user_id,
                    id,
                    total as total_price,
                    created_at
                from
                    dw.dw_trd_order_wide_day
                where 
                    dt>=get_date('${day}',-61) and
                    order_status=1
                ) t1
            join(
                select
                    order_id,
                    entity_id,
                    name as food_name
                from
                    dw.dw_trd_order_item
                where 
                    dt='${day}'
                ) t2
            on(
                t1.id=t2.order_id
                )
        ) a
    join(
        select
            food_id,
            t1.food_name,
            t1.normalize_food_name,
            t2.flavor
        from(
            select
                food_id,
                food_name,
                normalize_food_name
            from
                dm.dm_mdl_food_name_normalize_day
            where 
                dt='${day}'
            ) t1
        join(
            select 
                food_name, 
                flavor
            from 
                dim.dim_mdl_food_tag_classification 
            where 
                part='class1_3'
            ) t2
        on(
            t1.normalize_food_name=t2.food_name
            )
        ) b
    on(
        a.food_id=b.food_id
        )
    where 
        b.flavor!='无口味' and 
        b.flavor!='暂无'
    group by 
        a.user_id, 
        b.flavor
;


drop table temp.temp_mdl_user_food_flavor_day_sub2;
create table temp.temp_mdl_user_food_flavor_day_sub2 as 
    select
        user_id,
        sum(order_cnt) as user_month_order_num_total
    from 
        temp.temp_mdl_user_food_flavor_day_sub1
    group by 
        user_id
;


drop table temp.temp_mdl_user_food_flavor_day;
create table temp.temp_mdl_user_food_flavor_day as 
    select 
        t1.user_id,
        t1.flavor,
        order_cnt as order_num_total,
        total_price_total,
        last_order_time_max,
        t2.user_month_order_num_total,
        record_day
    from
        temp.temp_mdl_user_food_flavor_day_sub1 t1
    join
        temp.temp_mdl_user_food_flavor_day_sub2 t2
    on(
        t1.user_id=t2.user_id
        )
;




drop table temp.temp_rec_user_flavor_score;
create table temp.temp_rec_user_flavor_score as
    select 
        user_id, 
        flavor, 
        (order_num_total/user_month_order_num_total)*bound_data(total_price_total,2,100)/datediff(record_day,last_order_time_max) as score
    from 
        temp.temp_mdl_user_food_flavor_day;

drop table temp.temp_rec_user_flavor_score_normalized;
create table temp.temp_rec_user_flavor_score_normalized as
    select 
        t.user_id, 
        concat('{', concat_ws(',',collect_set(concat('\"',t.flavor,'\":','\"',t.score_normalized,'\"'))), '}') flavor_info
    from(
        select 
            user_id, 
            flavor, 
            round(1/(1+exp(-score)),2) as score_normalized
        from 
            temp.temp_rec_user_flavor_score
        ) t
    group by 
        t.user_id
;
 

drop table temp.temp_ups_user_flavor_prefer_partition;
create table temp.temp_ups_user_flavor_prefer_partition as 
    select 
        t.user_id, 
        t.attr_value
    from(
        select 
            case when t1.user_id is not null then t1.user_id else t2.user_id end as user_id,
            case when t2.flavor_info is not null then t2.flavor_info else t1.attr_value end as attr_value 
        from(
            select 
                user_id, 
                attr_value
            from 
                dm.dm_ups_user_item_info
            where 
                flag='rec_flavor' and 
                attr_key='flavor_prefer' and 
                datediff('${day}',dt)=1
                ) t1
        full outer join (
            select 
                user_id, 
                flavor_info
            from 
                temp.temp_rec_user_flavor_score_normalized
                ) t2
        on (
            t1.user_id=t2.user_id
            )
        ) t
    group by 
        t.user_id, 
        t.attr_value
;



insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_flavor')
    select 
        user_id, 
        'rec' as top_category,
        'flavor_prefer' as attr_key, 
        attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_ups_user_flavor_prefer_partition
;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_flavor')
    select 
        user_id, 
        'rec' as top_category,
        'flavor_prefer' as attr_key, 
        attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_ups_user_flavor_prefer_partition
;






