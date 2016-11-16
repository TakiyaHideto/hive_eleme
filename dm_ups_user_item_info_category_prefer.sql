#***************************************************************************************************
# ** 文件名称： dm_ups_user_item_info_category_prefer.sql
# ** 功能描述： 生成用户大类偏好，并插入到用户画像中
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-22
#***************************************************************************************************


drop table temp.temp_mdl_user_food_sample_cate_sub1;
create table temp.temp_mdl_user_food_sample_cate_sub1 as
    select 
        a.user_id,
        b.category as cate,  
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
            t2.category
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
                category
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
    group by 
        a.user_id,
        b.category
;


-- sub task 1: 计算用户下单信息
    -- sub task 1.2: 计算用户总订单数
drop table temp.temp_mdl_user_food_sample_cate_sub2;
create table temp.temp_mdl_user_food_sample_cate_sub2 as
    select
        user_id,
        sum(order_cnt) as ord_user_cnt_total
    from
        temp.temp_mdl_user_food_sample_cate_sub1
    group by 
        user_id
;

-- sub task 1: 计算用户下单信息
    -- sub task 1.3: 合并用户下单信息
drop table temp.temp_mdl_user_food_sample_cate;
create table temp.temp_mdl_user_food_sample_cate as 
    select
        t1.user_id,
        t1.style,  
        order_cnt as order_num_total, 
        total_price_total, 
        last_order_time_max, 
        t2.ord_user_cnt_total as user_month_order_num, 
        '${day}' as record_day
    from
        temp.temp_mdl_user_food_sample_cate_sub1 t1
    join
        temp.temp_mdl_user_food_sample_cate_sub2 t2
    on(
        t1.user_id=t2.user_id
        )
;




drop table temp.temp_rec_user_cate_score;
create table temp.temp_rec_user_cate_score as
    select 
        user_id,
        cate, 
        (order_num_total/user_month_order_num)*bound_data(total_price_total,2,50)/datediff('${day}',last_order_time) as score
    from 
        temp.temp_mdl_user_food_sample_cate_day;




drop table temp.temp_rec_user_cate_score_normalized;
create table temp.temp_rec_user_cate_score_normalized as
	select 
		t.user_id, 
		concat('{', concat_ws(',',collect_set(concat('\"',t.cate,'\":','\"',t.score_normalized,'\"'))), '}') food_info
	from(
		select 
			user_id, 
			cate, 
			-- round(1/(1+exp(-score)),2) as score_normalized
			score as score_normalized
		from 
			temp.temp_rec_user_cate_score
		) t
	group by 
		t.user_id
; 

drop table temp.temp_ups_user_cat_prefer_partition;
create table temp.temp_ups_user_cat_prefer_partition as 
	select 
		t.user_id, 
		t.attr_value
	from(
		select 
			case when t1.user_id is not null then 
					t1.user_id 
				else 
					t2.user_id 
				end as user_id,
			case when t2.food_info is not null then 
					t2.food_info 
				else 
					t1.attr_value 
				end as attr_value 
		from(
			select 
				user_id, 
				attr_value
			from 
				dm.dm_ups_user_item_info
			where 
				flag='rec_category' and 
				attr_key='category_prefer' and 
				datediff('${day}',dt)=1
				) t1
		full outer join (
			select 
				user_id, 
				food_info
			from 
				temp.temp_rec_user_cate_score_normalized
			) t2
		on (
			t1.user_id=t2.user_id
			)
		) t
	group by 
		t.user_id, 
		t.attr_value;




insert overwrite table dm.dm_ups_user_item_info partition(dt='${day}', flag='rec_category')
	select 
		user_id, 
		'rec' as top_category,
		'category_prefer' as attr_key, 
		attr_value, 
		'1' as is_json, 
		from_unixtime(unix_timestamp()) as update_time
	from 
		temp.temp_ups_user_cat_prefer_partition
;

insert overwrite table dm.dm_ups_user_item_info partition(dt='3000-12-31', flag='rec_category')
	select 
		user_id, 
		'rec' as top_category,
		'category_prefer' as attr_key, 
		attr_value, '1' as is_json, 
		from_unixtime(unix_timestamp()) as update_time
	from 
		temp.temp_ups_user_cat_prefer_partition
;



