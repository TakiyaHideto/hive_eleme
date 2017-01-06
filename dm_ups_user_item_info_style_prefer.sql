#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_style_prefer.sql
# ** 功能描述： 生成用户口味风格，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-18
#***************************************************************************************************

-- sub task 1: 提取食品正则名称和风格的映射
-- DROP TABLE temp.temp_food_tag_classification_style_mapping;
-- CREATE TABLE temp.temp_food_tag_classification_style_mapping AS
--     SELECT 
--         t2.food_name, 
--         t1.single_tag as style
--     FROM(
--         SELECT 
--             food_name, 
--             single_tag
--         FROM (
            -- SELECT 
            --     food_name, 
            --     category
            -- FROM 
            --     dim.dim_mdl_food_tag_classification
            -- WHERE
            --     part='class1_function'
            --     order by category desc limit 6;

--             ) t
--         LATERAL VIEW EXPLODE(split(category,'#')) tmp AS single_tag
--         WHERE 
--             single_tag is not null and 
--             single_tag!=''
--             ) t1
--     JOIN (
--         SELECT 
--             food_name, 
--             single_tag
--         FROM(
--             SELECT 
--                 food_name, 
--                 concat_ws('#', category, flavor, method) as food_tag 
--             FROM 
--                 dim.dim_mdl_food_tag_classification 
--             WHERE part='class1_3'
--             ) t
--         LATERAL VIEW EXPLODE(split(food_tag,'#')) tmp AS single_tag
--         ) t2
--     ON (t2.single_tag=t1.food_name)
-- ;



-- sub task 2: 计算用户下单信息
    -- sub task 2.1: 计算用户每个偏好订单数量和价格
drop table temp.temp_mdl_user_food_style_day_sub1;
create table temp.temp_mdl_user_food_style_day_sub1 as
    select 
        a.user_id,
        b.style,  
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
           regexp_replace(style,'\"','') as style
        from(
            select
	            food_id,
	            tag_function
            from
            	dm.dm_mdl_food_name_normalize_day
            where 
            	dt='${day}'
            ) t1
            lateral view explode(split(regexp_replace(tag_function,"\\[|\\]",""),',')) tmp AS style) b
    on(
        a.food_id=b.food_id
        )
    where 
        b.style!='无口味' and 
        b.style!='暂无'
    group by 
        a.user_id, 
        b.style
;

-- sub task 2: 计算用户下单信息
    -- sub task 2.2: 计算用户总订单数
drop table temp.temp_mdl_user_food_style_day_sub2;
create table temp.temp_mdl_user_food_style_day_sub2 as
    select
        user_id,
        sum(order_cnt) as ord_user_cnt_total
    from
        temp.temp_mdl_user_food_style_day_sub1
    group by 
        user_id
;

-- sub task 2: 计算用户下单信息
    -- sub task 2.3: 合并用户下单信息
drop table temp.temp_mdl_user_food_style_day;
create table temp.temp_mdl_user_food_style_day as 
    select
        t1.user_id,
        t1.style,  
        order_cnt as order_num_total, 
        total_price_total, 
        last_order_time_max, 
        t2.ord_user_cnt_total as user_month_order_num_total, 
        '${day}' as record_day
    from
        temp.temp_mdl_user_food_style_day_sub1 t1
    join
        temp.temp_mdl_user_food_style_day_sub2 t2
    on(
        t1.user_id=t2.user_id
        )
;



-- drop table temp.temp_mdl_user_food_style_day_click_based;
-- create table temp.temp_mdl_user_food_style_day_click_based as 
--     select 
--         t1.user_id, 
--         t2.style, 
--         max(t1.last_click_time) as last_click_time, 
--         sum(t1.click_cnt) as click_cnt, 
--         avg(t1.price) as price, 
--         avg(t1.score) as score
--     from 
--         dm.dm_mdl_user_food_prefer_click_based_score t1
--     join 
--         temp.temp_food_tag_classification_style_mapping t2
--     on (
--         t1.food_name=t2.food_name
--         )
--     group by
--         t1.user_id, 
--         t2.style;



-- drop table temp.temp_rec_user_style_score;
-- create table temp.temp_rec_user_style_score as
--     select 
--         t1.user_id, 
--         max(case when t1.style is not null then t1.style else t2.style end) as style,
--         max(case when t1.score is not null then t1.score else t2.score end) as score
--     from( 
--         select user_id, 
--         style, 
--         round(1/(1+exp(-(order_num_total/user_month_order_num_total)*bound_data(total_price_total,2,250)/(datediff(record_day,last_order_time_max)+1))),2) as score
--         from 
--             temp.temp_mdl_user_food_style_day
--         ) t1
--     join(
--         select 
--             user_id, 
--             style, score 
--         from 
--             temp.temp_mdl_user_food_style_day_click_based
--         ) t2
--     on (
--         t1.user_id=t2.user_id
--         )
--     group by 
--         t1.user_id
-- ;



-- drop table temp.temp_rec_user_style_score;
-- create table temp.temp_rec_user_style_score as
--     select user_id, 
--     style, 
--     round(1/(1+exp(-(order_num_total/user_month_order_num_total)*bound_data(total_price_total,2,250)/(datediff(record_day,last_order_time_max)+1))),2) as score
--     from 
--         temp.temp_mdl_user_food_style_day
-- ;


-- sub task 3: 计算用户风格偏好分数
drop table temp.temp_rec_user_style_score;
create table temp.temp_rec_user_style_score as
    select user_id, 
    style, 
    -- round(1/(1+exp(-(order_num_total/user_month_order_num_total)*bound_data(total_price_total,2,250)/(datediff(record_day,last_order_time_max)+1))),2) as score
    round((order_num_total/user_month_order_num_total)*bound_data(total_price_total,2,250)/(datediff(record_day,last_order_time_max)+1)+0.01,2) as score
    from 
        temp.temp_mdl_user_food_style_day
;



-- sub task 4: 使用sigmoid函数正则化映射
drop table temp.temp_rec_user_style_score_normalized;
create table temp.temp_rec_user_style_score_normalized as
    select 
        t.user_id, 
        concat('{', concat_ws(',',collect_set(concat('\"',t.style,'\":','\"',t.score_normalized,'\"'))), '}') style_info
    from(
        select 
            user_id, 
            style, 
            -- round(1/(1+exp(-score)),2) as score_normalized
            score as score_normalized
        from 
            temp.temp_rec_user_style_score
        ) t
    group by 
        t.user_id
; 



drop table temp.temp_ups_user_style_prefer_partition;
create table temp.temp_ups_user_style_prefer_partition as 
    select 
        t.user_id, 
        t.attr_value
    from(
        select 
            case when t1.user_id is not null then t1.user_id else t2.user_id end as user_id,
            case when t2.style_info is not null then t2.style_info else t1.attr_value end as attr_value 
        from(
            select 
                user_id, 
                attr_value
            from 
                dm.dm_ups_user_item_info
            where 
                flag='rec_style' and 
                attr_key='style_prefer' and 
                datediff('${day}',dt)=1
            ) t1
        full outer join(
            select 
                user_id, 
                style_info
            from 
                temp.temp_rec_user_style_score_normalized
            ) t2
        on (
            t1.user_id=t2.user_id
            )
        ) t
    group by 
        t.user_id, 
        t.attr_value
;


insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_style')
    select 
        user_id, 
        'rec' as top_category,
        'style_prefer' as attr_key, 
        attr_value, 
        '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_ups_user_style_prefer_partition
		where user_id is not null
;


insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_style')
    select 
        user_id, 
        'rec' as top_category,
        'style_prefer' as attr_key, 
        attr_value, '1' as is_json, 
        from_unixtime(unix_timestamp()) as update_time
    from 
        temp.temp_ups_user_style_prefer_partition
		where user_id is not null
;