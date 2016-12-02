#***************************************************************************************************
# ** 文件名称： dm_mdl_restaurant_eleme_subsidy_budget.sql
# ** 功能描述： 商家补贴策略经费预算
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-13
#***************************************************************************************************


-- sub task 1: 各餐厅类别的在不同时间段内订单数量信息
drop table temp.temp_mdl_restaurant_cate_month_order_info;
create table temp.temp_mdl_restaurant_cate_month_order_info as
    select 
        cat1_name, 
        date_section, 
        year,
        month,
        round(sum(order_cnt),2) as order_cnt,
        round(sum(sales_total),2) as sales_total
    from (
        select 
            cat1_name, 
            restaurant_id
        from 
            rec.rec_prf_restaurant_category_info
        where 
            dt='2016-11-13'
        ) t1
    join(
        select 
            restaurant_id, 
            year(created_at) as year,
            month(created_at) as month,
            concat(year(created_at), '-', month(created_at)) as date_section,
            count(id) as order_cnt,
            sum(total) as sales_total
        from 
            dw.dw_trd_order_wide
        where 
            dt='2016-11-13' and 
            order_date>='2015-01-01' and 
            order_status=1
        group by 
            restaurant_id, 
            year(created_at),
            month(created_at),
            concat(year(created_at), '-', month(created_at))
        ) t2
    on 
        t1.restaurant_id=t2.restaurant_id
    group by 
        t1.cat1_name, 
        t2.year,
        t2.month,
        t2.date_section
    order by 
        date_section
;



-- sub task 2: 各餐厅类别在订单增长、下降率，以及销售额的增长下降率
drop table temp.temp_mdl_restaurant_month_order_increase;
create table temp.temp_mdl_restaurant_month_order_increase as 
    select
        cat1_name,
        '2016-11-13' as currnet_date,
        round((cur_month_ord_cnt-lst_month_ord_cnt)/lst_month_ord_cnt,2) as ord_cur_inc,
        round((nxt_month_ord_cnt-cur_month_ord_cnt)/cur_month_ord_cnt,2) as ord_nxt_inc,
        round((cur_month_sales_cnt-lst_month_sales_cnt)/lst_month_sales_cnt,2) as sales_cur_inc,
        round((nxt_month_sales_cnt-cur_month_sales_cnt)/cur_month_sales_cnt,2) as sales_nxt_inc
    from(
        select
            cat1_name,
            '2016-11-13' as currnet_date,
            max(case when month('2016-11-13')=1 then 
                    if(year=year('2016-11-13')-2 and month=12,order_cnt,0)
                else 
                    if(year=year('2016-11-13')-1 and month=month('2016-11-13')-1,order_cnt,0)
                end) as lst_month_ord_cnt,

            max(if(year=year('2016-11-13')-1 and month=month('2016-11-13'), order_cnt, 0)) as cur_month_ord_cnt,

            max(case when month('2016-11-13')=12 then 
                    if(year=year('2016-11-13') and month=1,order_cnt,0)
                else
                    if(year=year('2016-11-13')-1 and month=month('2016-11-13')+1, order_cnt, 0)
                end) as nxt_month_ord_cnt,

            max(case when month('2016-11-13')=1 then 
                    if(year=year('2016-11-13')-2 and month=12,sales_total,0)
                else 
                    if(year=year('2016-11-13')-1 and month=month('2016-11-13')-1,sales_total,0)
                end) as lst_month_sales_cnt,

            max(if(year=year('2016-11-13')-1 and month=month('2016-11-13'), sales_total, 0)) as cur_month_sales_cnt,

            max(case when month('2016-11-13')=12 then 
                    if(year=year('2016-11-13') and month=1,sales_total,0)
                else
                    if(year=year('2016-11-13')-1 and month=month('2016-11-13')+1, sales_total, 0)
                end) as nxt_month_sales_cnt
        from 
            temp.temp_mdl_restaurant_cate_month_order_info
        group by 
            cat1_name
        ) t
;



-- sub task 3: 提取每家餐厅上月订单信息
drop table temp.temp_mdl_restaurant_month_order_subsidy;
create table temp.temp_mdl_restaurant_month_order_subsidy as
    select
        t1.order_id,
        t2.restaurant_id,
        t1.total,
        case when is_multi_strategy=1 then
            case when t1.total>split(split(strategy_code,'\\|')[0],',')[0] then 
                    split(split(strategy_code,'\\|')[0],',')[1]
                when t1.total>split(split(strategy_code,'\\|')[1],',')[0] then
                    split(split(strategy_code,'\\|')[1],',')[1]
                else
                    0
                end
            else
                case when t1.total>split(strategy_code,',')[0] then
                        split(strategy_code,',')[1]
                    else
                        0
                    end
            end as subsidy_eleme
    from(
        select 
            id as order_id,
            restaurant_id,
            total
        from 
            dw.dw_trd_order_wide
        where
            dt='2016-11-13' and 
            order_status=1 and 
            month(created_at)=month('2016-11-13')-1
        ) t1
    join(
        select
            *
        from 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        where 
            dt='3000-12-31'
        ) t2
    on(
        t1.restaurant_id=t2.restaurant_id
        )
;



-- sub task 4
drop table temp.temp_mdl_restaurant_month_order_subsidy_sum;
create table temp.temp_mdl_restaurant_month_order_subsidy_sum as
    select
        restaurant_id,
        order_cnt*(1+cur_inc) as order_cnt_cur,
        order_cnt*(1+cur_inc)*(1+nxt_inc) as order_cnt_nxt,
        total*(1+cur_inc) as total_cur,
        total*(1+cur_inc)*(1+nxt_inc) as total_nxt,
        subsidy_eleme*(1+cur_inc) as subsidy_eleme_cur,
        subsidy_eleme*(1+cur_inc)*(1+nxt_inc) as subsidy_eleme_nxt
    from(
        select
            restaurant_id,
            count(distinct a.order_id) order_cnt,
            sum(total) as total,
            sum(subsidy_eleme) as subsidy_eleme,
            max(b.cur_inc) as cur_inc,
            max(b.nxt_inc) as nxt_inc
        from(
            select
                t1.restaurant_id,
                t2.cat1_name,
                order_id,
                total,
                subsidy_eleme
            from 
                temp.temp_mdl_restaurant_month_order_subsidy t1
            join(
                select 
                    restaurant_id,
                    cat1_name
                from 
                    rec.rec_prf_restaurant_category_info
                where 
                    dt='2016-11-13'
                ) t2
            on(
                t1.restaurant_id=t2.restaurant_id
                )
            ) a
        join
            temp.temp_mdl_restaurant_month_order_increase b
        on (
            a.cat1_name=b.cat1_name
            )
        group by 
            restaurant_id
            ) t
;





