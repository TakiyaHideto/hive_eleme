#***************************************************************************************************
# ** 文件名称： dm.dm_ups_user_item_info_food_prefer.sql
# ** 功能描述： 生成用户食品偏好，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-11
#***************************************************************************************************

drop table temp.temp_mdl_food_user_food_sample_sub1;
create table temp.temp_mdl_food_user_food_sample_sub1 as
    select 
        a.user_id,
        b.normalize_food_name as food_name,  
        count(order_id) as order_cnt, 
        sum(a.total_price) as total_price,
        max(a.created_at) as last_order_time
        -- sum(t2.user_month_order_num) as user_month_order_num
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
                    dw.dw_trd_order_wide
                where 
                    dt='${day}' and 
                    datediff('${day}',created_at)<61 and
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
                    dt='${day}' and
                    datediff('${day}',created_at)<61
                ) t2
            on(
                t1.id=t2.order_id
                )
        ) a
    join(
        select
            food_id,
            food_name,
            normalize_food_name
        from
            dm.dm_mdl_food_name_normalize_day
        where 
            dt='${day}'      
        ) b
    on(
        a.food_id=b.food_id
        )
    group by 
        a.user_id,
        b.normalize_food_name
;

drop table temp.temp_mdl_food_user_food_sample_sub2;
create table temp.temp_mdl_food_user_food_sample_sub2 as 
    select
        user_id,
        sum(order_cnt) as user_month_order_num
    from
        temp.temp_mdl_food_user_food_sample_sub1
    group by 
        user_id
;

drop table temp.temp_mdl_food_user_food_sample;
create table temp.temp_mdl_food_user_food_sample as 
    select
        t1.user_id,
        food_name,
        order_cnt,
        total_price,
        last_order_time,
        user_month_order_num,
        '${day}' as record_day
    from 
        temp.temp_mdl_food_user_food_sample_sub1 t1
    join 
        temp.temp_mdl_food_user_food_sample_sub2 t2
    on(
        t1.user_id=t2.user_id
        )
;


drop table temp.temp_rec_user_food_score;
create table temp.temp_rec_user_food_score as
    select
        user_id,
        food_name,
        score,
        row_number() over (partition by user_id order by score desc) as rno
    from(
        select 
            user_id, 
            food_name, 
            round((order_cnt/user_month_order_num)*bound_data(total_price,2,50)/(datediff('${day}',last_order_time)+1)+0.01,2) as score
        from 
            temp.temp_mdl_food_user_food_sample
        ) t
;




drop table temp.temp_rec_user_food_score_normalized;
create table temp.temp_rec_user_food_score_normalized as
    select 
        t.user_id, 
        concat('{', concat_ws(',',collect_set(concat('\"',t.food_name,'\":','\"',t.score_normalized,'\"'))), '}') food_info
    from(
        select 
            user_id, 
            food_name, 
            score as score_normalized
        from 
            temp.temp_rec_user_food_score
        where
            rno<21 and 
            food_name != '' and 
            food_name is not null 
        ) t
    group by 
        t.user_id
; 


drop table temp.temp_ups_user_food_prefer_partition;
create table temp.temp_ups_user_food_prefer_partition as 
    select 
        t.user_id, 
        t.attr_value
    from(
        select 
            case when t1.user_id is not null then t1.user_id else t2.user_id end as user_id,
            case when t2.food_info is not null then t2.food_info else t1.attr_value end as attr_value 
        from(
            select 
                user_id, 
                attr_value
        from 
            dm.dm_ups_user_item_info
        where 
            flag='rec_food' and 
            attr_key='food_prefer' and 
            datediff('${day}',dt)=1
        ) t1
    full outer join (
        select 
            user_id, 
            food_info
        from temp.temp_rec_user_food_score_normalized) t2
        on t1.user_id=t2.user_id) t
    group by t.user_id, t.attr_value
;

insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_food')
    select 
        user_id, 
        'rec' as top_category,
        'food_prefer' as attr_key, 
        attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_ups_user_food_prefer_partition;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_food')
    select 
        user_id, 
        'rec' as top_category,
        'food_prefer' as attr_key, 
        attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_ups_user_food_prefer_partition;
