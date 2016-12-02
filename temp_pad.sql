近铁城市广场北、南座
wtw3djd
wtw3djg


陆家嘴（部分）
wtw3tjq
wtw3tdh
wtw3tjp
wtw3tjn
wtw3td9
wtw3thz
wtw3thu


高订单geohash
wtw3dju
ws0g0td
ws0s2he
webtkmp
wx4erpx
wxryp0x
w7y2mfr
wskks74
wtw3tjp
wtmthej
ww56d2z
w7mbm07
wtw0tuq
wtst3kk
wsbqjzg




select t1.id as restaurant_ID, 
t1.name as restaurant_name,
coalesce(t2.strategy, '无') as strategy,
coalesce(t2.strategy_detail,'无') as strategy_detail,
coalesce(t2.cat0_name, '---') as cat0_name,
coalesce(t2.cat1_name, '---') as cat1_name,
coalesce(t2.restaurant_tag, '---') as restaurant_tag, 
coalesce(cast(t2.cur_month_total string),'---') as cur_month_total, 
coalesce(cast(t2.lst_month_total as string), '---') as lst_month_total,
coalesce(cast(t2.sales_ratio as string),'---') as sales_ratio  from 
(
select id, name from test.test_restaurant_geoash where 1=1 and geohash='wtw3djd'
) t1 left join 
(select * from dm.dm_mdl_restaurant_eleme_subsidy_strategy where dt='3000-12-31') t2 
on (t1.id=t2.restaurant_id)







select b.cat1_name, round(sum(restaurant_subsidy)/(sum(eleme_subsidy)+sum(restaurant_subsidy)),2), round(sum(eleme_subsidy)/(sum(eleme_subsidy)+sum(restaurant_subsidy)),2)
from(select restaurant_id,sum(restaurant_subsidy) as restaurant_subsidy, sum(eleme_subsidy) as eleme_subsidy
from dw.dw_trd_order_wide where dt='2016-11-21' and order_status=1 and datediff('2016-11-21',created_at)<31 group by restaurant_id) a
join(select restaurant_id,cat1_name from rec.rec_prf_restaurant_category_info where dt='2016-11-21') b
on a.restaurant_id=b.restaurant_id
group by b.cat1_name;

select round(sum(restaurant_subsidy)/(sum(eleme_subsidy)+sum(restaurant_subsidy)),2), round(sum(eleme_subsidy)/(sum(eleme_subsidy)+sum(restaurant_subsidy)),2)
from(select restaurant_id,sum(restaurant_subsidy) as restaurant_subsidy, sum(eleme_subsidy) as eleme_subsidy
from dw.dw_trd_order_wide where dt='2016-11-21' and order_status=1 and datediff('2016-11-21',created_at)<31 group by restaurant_id) a
join(select restaurant_id,cat1_name from rec.rec_prf_restaurant_category_info where dt='2016-11-21') b
on a.restaurant_id=b.restaurant_id
;































drop table temp.temp_mdl_rst_subsidy_strategy;
create table temp.temp_mdl_rst_subsidy_strategy as 
    select
        a.restaurant_id,
        name,
        restaurant_tag,
        geohash,
        cat0_name,
        cat1_name,
        strategy,
        strategy_code,
        strategy_detail,
        lastest_subsidy,
        is_multi_strategy,
        cur_month_total,
        lst_month_total,
        sales_ratio
    from(
        select
            restaurant_id,
            concat_ws(',',collect_list(concat('(ele补',eleme_subsidy,'rst补',restaurant_subsidy,')'))) as lastest_subsidy
        from(
            select
                restaurant_id,
                activity_id,
                condition_amt,
                condition_pay_online,
                benefit_uplimit_amt,
                benefit_downlimit_amt,
                benefit_amt,
                eleme_subsidy,
                restaurant_subsidy,
                create_time,
                row_number() over (partition by restaurant_id order by create_time desc) rno
            from(
                select
                    restaurant_id,
                    t1.activity_id,
                    condition_amt,
                    condition_pay_online,
                    benefit_uplimit_amt,
                    benefit_downlimit_amt,
                    benefit_amt,
                    subsidy_amt as eleme_subsidy,
                    benefit_amt-subsidy_amt as restaurant_subsidy,
                    max(create_time) as create_time
                from(
                    select
                        restaurant_id,
                        activity_id,
                        create_time,
                        category_code,
                        condition_amt,
                        condition_pay_online,
                        benefit_uplimit_amt,
                        benefit_downlimit_amt,
                        benefit_amt,
                        subsidy_amt,
                        benefit_amt-subsidy_amt
                    from
                        dm.dm_mkt_act_order_restaurant_subsidy
                    where 
                        dt>'2016-10-01' and 
                        category_code=12 and
                        condition_amt is not null and
                        condition_pay_online is not null
                    ) t1
                join(
                    select
                        activity_id,
                        category_code
                    from 
                        dm.dm_mkt_act_order_subject
                    where 
                        dt='2016-11-20' and
                        activity_type=1010 and
                        category_code=12
                    ) t2  
                on(
                    t1.activity_id=t2.activity_id and
                    t1.category_code=t2.category_code
                    ) 
                group by 
                    restaurant_id,
                    t1.activity_id,
                    condition_amt,
                    condition_pay_online,
                    benefit_uplimit_amt,
                    benefit_downlimit_amt,
                    benefit_amt,
                    subsidy_amt,
                    benefit_amt-subsidy_amt
                ) ts1
            ) t
        where
            rno<4
        group by
            restaurant_id
        ) a
    left outer join( 
        select
            t1.restaurant_id,
            name,
            rst_tag as restaurant_tag,
            geo_cluster as geohash,
            cat0_name,
            t1.cat1_name,
            strategy,
            strategy_code,
            case when is_multi_strategy=1 then
                    concat('ele补',round(cast(split(split(strategy_code,'\\|')[0],',')[1] as int)*bound_data(ele_subsidy_scale+0.03,0,1),2),',',
                           'rst补贴',round(cast(split(split(strategy_code,'\\|')[0],',')[1] as int)*bound_data(rst_subsidy_scale-0.03,0,1),2),'|',
                           'ele补',round(cast(split(split(strategy_code,'\\|')[1],',')[1] as int)*bound_data(ele_subsidy_scale+0.03,0,1),2),',',
                           'rst补贴',round(cast(split(split(strategy_code,'\\|')[1],',')[1] as int)*bound_data(rst_subsidy_scale-0.03,0,1),2)
                           ) 
                else
                    concat('ele补',round(cast(split(strategy_code,',')[1] as int)*bound_data(ele_subsidy_scale+0.03,0,1),2),',',
                           'rst补贴',round(cast(split(strategy_code,',')[1] as int)*bound_data(rst_subsidy_scale-0.03,0,1),2)
                           ) 
                end as strategy_detail,
            is_multi_strategy,
            cur_month_total,
            lst_month_total,
            sales_ratio
        from
            temp.temp_mdl_rst_subsidy_strategy_sub2 t1
        join 
            temp.temp_mdl_rst_cate_eleme_subsidy_scale t2
        on (
            t1.cat1_name=t2.cat1_name
            )
        ) b
    on(
        a.restaurant_id=b.restaurant_id
        )
;


























from(
        select
            t.restaurant_id,
            concat_ws(',',collect_list(concat('(ele补',eleme_subsidy,'rst补',restaurant_subsidy,')'))) as lastest_subsidy
        from(
            select
                restaurant_id,
                created_at,
                eleme_subsidy,
                restaurant_subsidy,
                row_number() over (partition by restaurant_id order by created_at desc) rno
            from
                dw.dw_trd_order_wide
            where 
                dt='2016-11-20' and
                datediff('2016-11-20',created_at)<3 and
                order_status=1
            ) t
        where 
            t.rno<4
        group by 
            t.restaurant_id
        ) t3



select count(distinct restaurant_id)
from(
select
            t.restaurant_id,
            concat_ws(',',collect_list(concat('(ele补',eleme_subsidy,'rst补',restaurant_subsidy,')'))) as lastest_subsidy
        from(
            select
                restaurant_id,
                created_at,
                eleme_subsidy,
                restaurant_subsidy,
                row_number() over (partition by restaurant_id order by created_at desc) rno
            from
                dw.dw_trd_order_wide
            where 
                dt='2016-11-20' and
                datediff('2016-11-20',created_at)<3 and
                order_status=1
            ) t
        where 
            t.rno<4
        group by 
            t.restaurant_id
) t


select t1.id as restaurant_id, 
t1.name as restaurant_name,
coalesce(t2.strategy, '无') as strategy,
coalesce(t2.strategy_detail,'无') as strategy_detail,
coalesce(t2.lastest_subsidy,'无') as lastest_subsidy,
coalesce(t2.cat0_name, '---') as cat0_name,
coalesce(t2.cat1_name, '---') as cat1_name,
coalesce(t2.restaurant_tag, '---') as restaurant_tag, 
coalesce(cast(t2.cur_month_total as string),'---') as cur_month_total, 
coalesce(cast(t2.lst_month_total as string), '---') as lst_month_total,
coalesce(cast(t2.sales_ratio as string),'---') as sales_ratio  
from 
(select id, name from test.test_restaurant_geoash where 1=1 and geohash='wtw3djd') t1 
left join 
(select * from temp.temp_mdl_rst_subsidy_strategy ) t2 
on (t1.id=t2.restaurant_id)
;
















drop table temp.temp_mdl_restaurant_subsidy_distribution;
create table temp.temp_mdl_restaurant_subsidy_distribution as
    select
        a.restaurant_id,
        a.city_name,
        a.restaurant_name,
        c.bu_flag,
        b.cat0_name,
        b.cat1_name,
        a.total,
        a.eleme_subsidy,
        round(a.eleme_subsidy/a.total,2) as eleme_subsidy_scale,
        a.restaurant_subsidy,
        round(a.restaurant_subsidy/a.total,2) as restaurant_subsidy_scale
    from(
        select
            restaurant_id,
            restaurant_name,
            city_name,
            round(sum(total),2) as total,
            round(sum(eleme_subsidy),2) as eleme_subsidy,
            round(sum(restaurant_subsidy),2) as restaurant_subsidy
        from
            dw.dw_trd_order_wide_day
        where
            dt>=get_date('2016-11-28',-7) and
            order_status=1
        group by 
            restaurant_id,
            restaurant_name,
            city_name
        ) a
    join(
        select
            restaurant_id,
            collect_set(cat0_name) as cat0_name,
            collect_set(cat1_name) as cat1_name
        from
            rec.rec_prf_restaurant_category_info
        where 
            dt='2016-11-28'
        group by 
            restaurant_id
        ) b
    on(
        a.restaurant_id=b.restaurant_id
        )
    join(
        select
            id as restaurant_id,
            city_name,
            bu_flag
        from 
            dw.dw_prd_restaurant_wide
        where
            dt='2016-11-28'
        ) c
    on(
        a.restaurant_id=c.restaurant_id
        )
;




select * from temp.temp_mdl_restaurant_subsidy_distribution where total>5000 sort by eleme_subsidy_scale desc limit 1000;



select sum(eleme_subsidy)/(sum(eleme_subsidy)+sum(restaurant_subsidy)) from dw.dw_trd_order_wide_day where dt>get_date(-7) and order_status=1;







