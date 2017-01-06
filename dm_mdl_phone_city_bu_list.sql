#***************************************************************************************************
# ** 文件名称： dm_mdl_phone_city_bu_list.sql
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-29
#***************************************************************************************************

-- drop table temp.temp_order_user_phone_matching_list;
-- create table temp.temp_order_user_phone_matching_list as
--     select 
--         user_id,
--         phone
--     from(
--         select
--             user_id,
--             phone,
--             row_number() over (partition by user_id order by a.cnt desc) as rno
--         from(
--             select
--                 user_id,
--                 phone,
--                 count(*) as cnt
--             from
--                 dw.dw_trd_order_wide_day
--             where
--                 dt>=get_date('2016-12-28',-59) and
--                 order_status=1
--             group by 
--                 user_id, 
--                 phone
--             ) a
--         ) t
--     where 
--         rno=1 
-- ;


drop table temp.temp_user_phone_date_city_matching_list;
create table temp.temp_user_phone_date_city_matching_list as 
    select
        phone,
        city_name,
        order_bu_flag
    from(
        select
            t1.user_id,
            city_name,
            order_bu_flag
        from(
            select
                user_id,
                max(attr_value) as order_bu_flag
            from
                dm.dm_ups_user_item_info
            where 
                dt=get_date('${day}',-1) and
                attr_key='order_bu_flag'
            group by 
                user_id
            ) t1
        join(
            select
                user_id,
                max(city_name) as city_name
            from 
                dw.dw_trd_order_wide_day
            where
                dt='${day}'
            group by
                user_id
            ) t2
        on(
            t1.user_id=t2.user_id
            )
    ) a
    join
        temp.temp_order_user_phone_matching_list b
    on(
        a.user_id=b.user_id
        )
;

insert overwrite table dm.dm_mdl_phone_city_bu_list partition(dt='${day}')
	select 
		phone,
		city_name,
		order_bu_flag
	from 
		temp_user_phone_date_city_matching_list
;



