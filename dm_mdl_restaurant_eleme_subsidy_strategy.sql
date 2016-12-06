#***************************************************************************************************
# ** 文件名称： dm_mdl_restaurant_eleme_subsidy_strategy.sql
# ** 功能描述： 商家补贴策略
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-10-29
#***************************************************************************************************


-- sub task 1: geo区域订单信息
drop table temp.temp_shop_subsidy_geohash_cluster_ord_info;
create table temp.temp_shop_subsidy_geohash_cluster_ord_info as 
    select 
        substr(geohash_of_latlng(t1.latitude,t1.longitude),0,7) as geo_cluster,
        count(order_id) as order_cnt,
        percentile_approx(total,0.5) as order_price_median,
        stddev(total) as order_price_stddev,
        percentile_approx(total,0.8) as total_percentile_8,
        percentile_approx(total,0.5) as total_percentile_5,
        percentile_approx(total,0.3) as total_percentile_3
    from(
        select id,
            latitude,
            longitude
        from 
            dw.dw_prd_restaurant
        where 
            dt='${day}'
        ) t1
    join(
        select 
            id as order_id,
            restaurant_id,
            total
        from 
            dw.dw_trd_order_wide_day
        where 
            dt>=get_date(-31) and
            order_status=1
        ) t2
    on (
        t1.id=t2.restaurant_id
        )
    where
        t2.total<=300
    group by 
        substr(geohash_of_latlng(t1.latitude,t1.longitude),0,7)
;



-- sub task 2: 餐厅画像中抽取特征, 以餐厅为主键, 并标记是否为点评品质餐厅
drop table temp.temp_rst_feat_table;
create table temp.temp_rst_feat_table as
    select 
        t.restaurant_id,
        if(t.min_deliver_amt is null, 0, t.min_deliver_amt) as min_deliver_amt,
        if(t.deliver_fee is null, 0, t.deliver_fee) as deliver_fee,
        if(t.time_ensure_discount is null, 0, t.time_ensure_discount) as time_ensure_discount,
        if(t.deliver_type is null, 0, t.deliver_type) as deliver_type,
        if(t.recent_7_open_days is null, 0, t.recent_7_open_days) as recent_7_open_days,
        if(t.recent_30_open_days is null, 0, t.recent_30_open_days) as recent_30_open_days,
        if(t.recent_7_open_hours is null, 0, t.recent_7_open_hours) as recent_7_open_hours,
        if(t.recent_30_open_hours is null, 0, t.recent_30_open_hours) as recent_30_open_hours,
        if(t.recent_7_order_complain_cnt is null, 0, t.recent_7_order_complain_cnt) as recent_7_order_complain_cnt,
        if(t.recent_30_order_complain_cnt is null, 0, t.recent_30_order_complain_cnt) as recent_30_order_complain_cnt,
        if(t.recent_7_order_remind_cnt is null, 0, t.recent_7_order_remind_cnt) as recent_7_order_remind_cnt,
        if(t.recent_30_order_remind_cnt is null, 0, t.recent_30_order_remind_cnt) as recent_30_order_remind_cnt,
        if(t.recent_7_user_refuse_order_cnt is null, 0, t.recent_7_user_refuse_order_cnt) as recent_7_user_refuse_order_cnt,
        if(t.recent_30_user_refuse_order_cnt is null, 0, t.recent_30_user_refuse_order_cnt) as recent_30_user_refuse_order_cnt,
        if(t.recent_7_rst_refuse_order_cnt is null, 0, t.recent_7_rst_refuse_order_cnt) as recent_7_rst_refuse_order_cnt,
        if(t.recent_30_rst_refuse_order_cnt is null, 0, t.recent_30_rst_refuse_order_cnt) as recent_30_rst_refuse_order_cnt,
        if(t.recent_7_order_cnt is null, 0, t.recent_7_order_cnt) as recent_7_order_cnt,
        if(t.recent_30_order_cnt is null, 0, t.recent_30_order_cnt) as recent_30_order_cnt,
        if(t.recent_7_user_cnt is null, 0, t.recent_7_user_cnt) as recent_7_user_cnt,
        if(t.recent_30_user_cnt is null, 0, t.recent_30_user_cnt) as recent_30_user_cnt,
        if(t.recent_7_ord_usr_ratio is null, 0, t.recent_7_ord_usr_ratio) as recent_7_ord_usr_ratio,
        if(t.recent_30_ord_usr_ratio is null, 0, t.recent_30_ord_usr_ratio) as recent_30_ord_usr_ratio,
        if(t.recent_7_returned_customer_scale is null, 0, t.recent_7_returned_customer_scale) as recent_7_returned_customer_scale,
        if(t.recent_30_returned_customer_scale is null, 0, t.recent_30_returned_customer_scale) as recent_30_returned_customer_scale,
        if(t.recent_7_order_amt is null, 0, t.recent_7_order_amt) as recent_7_order_amt,
        if(t.recent_30_order_amt is null, 0, t.recent_30_order_amt) as recent_30_order_amt,
        if(t.recent_30_order_price_median is null, 0, t.recent_30_order_price_median) as recent_30_order_price_median,
        if(t.recent_30_order_price_stddev is null, 0, t.recent_30_order_price_stddev) as recent_30_order_price_stddev,
        if(t.recent_30_payment_amt is null, 0, t.recent_30_payment_amt) as recent_30_payment_amt,
        if(t.recent_30_payment_median is null, 0, t.recent_30_payment_median) as recent_30_payment_median,
        if(t.recent_30_payment_stddev is null, 0, t.recent_30_payment_stddev) as recent_30_payment_stddev,
        if(t.recent_30_eleme_subsidy_amt is null, 0, t.recent_30_eleme_subsidy_amt) as recent_30_eleme_subsidy_amt,
        if(t.recent_30_eleme_subsidy_median is null, 0, t.recent_30_eleme_subsidy_median) as recent_30_eleme_subsidy_median,
        if(t.recent_30_eleme_subsidy_stddev is null, 0, t.recent_30_eleme_subsidy_stddev) as recent_30_eleme_subsidy_stddev,
        if(t.recent_30_order_chargeback_scale is null, 0, t.recent_30_order_chargeback_scale) as recent_30_order_chargeback_scale,
        if(t.recent_30_order_unpaid_cnt is null, 0, t.recent_30_order_unpaid_cnt) as recent_30_order_unpaid_cnt,
        if(t.recent_30_order_unpaid_scale is null, 0, t.recent_30_order_unpaid_scale) as recent_30_order_unpaid_scale,
        if(t.recent_7_order_hongbao_preferential_avg is null, 0, t.recent_7_order_hongbao_preferential_avg) as recent_7_order_hongbao_preferential_avg,
        is_dianping
    from(
        select 
            t1.restaurant_id, 
            parse_json_object(parse_json_object(profile_json,"base"),"min_deliver_amt")  as min_deliver_amt,
            parse_json_object(parse_json_object(profile_json,"base"),"deliver_fee") as deliver_fee,
            parse_json_object(parse_json_object(profile_json,"base"),"time_ensure_discount") as time_ensure_discount,
            parse_json_object(parse_json_object(profile_json,"base"),"deliver_type") as deliver_type,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_7_open_days") as recent_7_open_days,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_30_open_days") as recent_30_open_days,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_7_open_hours") as recent_7_open_hours,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_30_open_hours") as recent_30_open_hours,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_7_order_complain_cnt") as recent_7_order_complain_cnt,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_30_order_complain_cnt") as recent_30_order_complain_cnt,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_7_order_remind_cnt") as recent_7_order_remind_cnt,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_30_order_remind_cnt") as recent_30_order_remind_cnt,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_7_user_refuse_order_cnt") as recent_7_user_refuse_order_cnt,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_30_user_refuse_order_cnt") as recent_30_user_refuse_order_cnt,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_7_rst_refuse_order_cnt") as recent_7_rst_refuse_order_cnt,
            parse_json_object(parse_json_object(profile_json,"rank"),"recent_30_rst_refuse_order_cnt") as recent_30_rst_refuse_order_cnt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_7_open_days") as recent_7_order_cnt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_7_open_days") as recent_30_order_cnt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_7_user_cnt") as recent_7_user_cnt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_user_cnt") as recent_30_user_cnt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_7_ord_usr_ratio") as recent_7_ord_usr_ratio,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_ord_usr_ratio") as recent_30_ord_usr_ratio,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_7_returned_customer_scale") as recent_7_returned_customer_scale,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_returned_customer_scale") as recent_30_returned_customer_scale,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_7_order_amt") as recent_7_order_amt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_order_amt") as recent_30_order_amt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_order_price_median") as recent_30_order_price_median,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_order_price_stddev") as recent_30_order_price_stddev,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_payment_amt") as recent_30_payment_amt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_payment_median") as recent_30_payment_median,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_payment_stddev") as recent_30_payment_stddev,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_eleme_subsidy_amt") as recent_30_eleme_subsidy_amt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_eleme_subsidy_median") as recent_30_eleme_subsidy_median,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_eleme_subsidy_stddev") as recent_30_eleme_subsidy_stddev,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_order_chargeback_scale") as recent_30_order_chargeback_scale,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_order_unpaid_cnt") as recent_30_order_unpaid_cnt,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_30_order_unpaid_scale") as recent_30_order_unpaid_scale,
            parse_json_object(parse_json_object(profile_json,"trade"),"recent_7_order_hongbao_preferential_avg") as recent_7_order_hongbao_preferential_avg,
            case when t2.restaurant_id is not null then 1 else 0 end as is_dianping
        from (
            select 
                *
            from 
                dm.dm_ups_restaurant_info 
            where 
                dt='3000-12-31' 
        ) t1
        left outer join (
            select 
                restaurant_id
            from 
                rec.high_quality_restaurant_info
            where 
                dt='${day}' and 
                type='dianping'
        ) t2
        on t1.restaurant_id=t2.restaurant_id
    ) t
;



-- sub task 3: 抽取点评品质餐厅
drop table temp.temp_shop_subsidy_dianping_rst;
create table temp.temp_shop_subsidy_dianping_rst as 
    select 
        t1.restaurant_id
    from (
        select 
            restaurant_id
        from
            rec.high_quality_restaurant_info
        where 
            dt='${day}' and 
            type='dianping'
        ) t1
    join (
        select
            id as restaurant_id
        from 
            dw.dw_prd_restaurant
        where
            dt='${day}' and
            is_valid=1
        ) t2
    on (
        t1.restaurant_id=t2.restaurant_id
        )
;


-- sub task 4: 抽取KA餐厅
drop table temp.temp_mdl_rst_subsidy_ka_rst;
create table temp.temp_mdl_rst_subsidy_ka_rst as 
    select
        id as restaurant_id
    from 
        dw.dw_prd_restaurant_wide
    where 
        dt='${day}' and 
        bu_flag='GKA'
    group by 
        id
;


-- sub task 5: 抽取SIG餐厅
drop table temp.temp_mdl_rst_subsidy_sig_rst;
create table temp.temp_mdl_rst_subsidy_sig_rst as 
    select
        id as restaurant_id
    from 
        dw.dw_prd_restaurant_wide
    where 
        dt='${day}' and 
        bu_flag='SIG'
    group by 
        id
;

-- sub task 6.1: 根据点评品质餐厅扩充相似品质餐厅,使用cosine相似度方法
drop table temp.temp_shop_subsidy_high_quality_rst_cosine;
create table temp.temp_shop_subsidy_high_quality_rst_cosine as 
    select 
        restaurant_id,
        (min_deliver_amt*min_deliver_amt_std+
            deliver_fee*deliver_fee_std+
            recent_7_open_days*recent_7_open_days_std+
            recent_30_open_days*recent_30_open_days_std+
            recent_7_open_hours*recent_7_open_hours_std+
            recent_30_open_hours*recent_30_open_hours_std+
            recent_7_order_complain_cnt*recent_7_order_complain_cnt_std+
            recent_30_order_complain_cnt*recent_30_order_complain_cnt_std+
            recent_7_order_remind_cnt*recent_7_order_remind_cnt_std+
            recent_7_user_refuse_order_cnt*recent_7_user_refuse_order_cnt_std+
            recent_30_user_refuse_order_cnt*recent_30_user_refuse_order_cnt_std+
            recent_7_rst_refuse_order_cnt*recent_7_rst_refuse_order_cnt_std+
            recent_30_rst_refuse_order_cnt*recent_30_rst_refuse_order_cnt_std+
            recent_7_order_cnt*recent_7_order_cnt_std+
            recent_30_order_cnt*recent_30_order_cnt_std+
            recent_7_user_cnt*recent_7_user_cnt_std+
            recent_30_user_cnt*recent_30_user_cnt_std+
            recent_7_ord_usr_ratio*recent_7_ord_usr_ratio_std+
            recent_30_ord_usr_ratio*recent_30_ord_usr_ratio_std+
            recent_7_returned_customer_scale*recent_7_returned_customer_scale_std+
            recent_30_returned_customer_scale*recent_30_returned_customer_scale_std+
            recent_7_order_amt*recent_7_order_amt_std+
            recent_30_order_amt*recent_30_order_amt_std+
            recent_30_payment_amt*recent_30_payment_amt_std+
            recent_30_eleme_subsidy_amt*recent_30_eleme_subsidy_amt_std+
            recent_30_order_chargeback_scale*recent_30_order_chargeback_scale_std+
            recent_30_order_unpaid_cnt*recent_30_order_unpaid_cnt_std+
            recent_7_order_hongbao_preferential_avg*recent_7_order_hongbao_preferential_avg_std)/
        (sqrt(min_deliver_amt*min_deliver_amt+
            deliver_fee*deliver_fee+
            recent_7_open_days*recent_7_open_days+
            recent_30_open_days*recent_30_open_days+
            recent_7_open_hours*recent_7_open_hours+
            recent_30_open_hours*recent_30_open_hours+
            recent_7_order_complain_cnt*recent_7_order_complain_cnt+
            recent_30_order_complain_cnt*recent_30_order_complain_cnt+
            recent_7_order_remind_cnt*recent_7_order_remind_cnt+
            recent_7_user_refuse_order_cnt*recent_7_user_refuse_order_cnt+
            recent_30_user_refuse_order_cnt*recent_30_user_refuse_order_cnt+
            recent_7_rst_refuse_order_cnt*recent_7_rst_refuse_order_cnt+
            recent_30_rst_refuse_order_cnt*recent_30_rst_refuse_order_cnt+
            recent_7_order_cnt*recent_7_order_cnt+
            recent_30_order_cnt*recent_30_order_cnt+
            recent_7_user_cnt*recent_7_user_cnt+
            recent_30_user_cnt*recent_30_user_cnt+
            recent_7_ord_usr_ratio*recent_7_ord_usr_ratio+
            recent_30_ord_usr_ratio*recent_30_ord_usr_ratio+
            recent_7_returned_customer_scale*recent_7_returned_customer_scale+
            recent_30_returned_customer_scale*recent_30_returned_customer_scale+
            recent_7_order_amt*recent_7_order_amt+
            recent_30_order_amt*recent_30_order_amt+
            recent_30_payment_amt*recent_30_payment_amt+
            recent_30_eleme_subsidy_amt*recent_30_eleme_subsidy_amt+
            recent_30_order_chargeback_scale*recent_30_order_chargeback_scale+
            recent_30_order_unpaid_cnt*recent_30_order_unpaid_cnt+
            recent_7_order_hongbao_preferential_avg*recent_7_order_hongbao_preferential_avg)*
        sqrt(min_deliver_amt_std*min_deliver_amt_std+
            deliver_fee_std*deliver_fee_std+
            recent_7_open_days_std*recent_7_open_days_std+
            recent_30_open_days_std*recent_30_open_days_std+
            recent_7_open_hours_std*recent_7_open_hours_std+
            recent_30_open_hours_std*recent_30_open_hours_std+
            recent_7_order_complain_cnt_std*recent_7_order_complain_cnt_std+
            recent_30_order_complain_cnt_std*recent_30_order_complain_cnt_std+
            recent_7_order_remind_cnt_std*recent_7_order_remind_cnt_std+
            recent_7_user_refuse_order_cnt_std*recent_7_user_refuse_order_cnt_std+
            recent_30_user_refuse_order_cnt_std*recent_30_user_refuse_order_cnt_std+
            recent_7_rst_refuse_order_cnt_std*recent_7_rst_refuse_order_cnt_std+
            recent_30_rst_refuse_order_cnt_std*recent_30_rst_refuse_order_cnt_std+
            recent_7_order_cnt_std*recent_7_order_cnt_std+
            recent_30_order_cnt_std*recent_30_order_cnt_std+
            recent_7_user_cnt_std*recent_7_user_cnt_std+
            recent_30_user_cnt_std*recent_30_user_cnt_std+
            recent_7_ord_usr_ratio_std*recent_7_ord_usr_ratio_std+
            recent_30_ord_usr_ratio_std*recent_30_ord_usr_ratio_std+
            recent_7_returned_customer_scale_std*recent_7_returned_customer_scale_std+
            recent_30_returned_customer_scale_std*recent_30_returned_customer_scale_std+
            recent_7_order_amt_std*recent_7_order_amt_std+
            recent_30_order_amt_std*recent_30_order_amt_std+
            recent_30_payment_amt_std*recent_30_payment_amt_std+
            recent_30_eleme_subsidy_amt_std*recent_30_eleme_subsidy_amt_std+
            recent_30_order_chargeback_scale_std*recent_30_order_chargeback_scale_std+
            recent_30_order_unpaid_cnt_std*recent_30_order_unpaid_cnt_std+
            recent_7_order_hongbao_preferential_avg_std*recent_7_order_hongbao_preferential_avg_std)) 
        as score
    from(
        select *,
            '1' as rst_tag
        from 
            temp.temp_rst_feat_table
        where 
            is_dianping=0
    ) t1
    join(
        select 
            '1' as dianping_tag,
            avg(min_deliver_amt) as min_deliver_amt_std,
            avg(deliver_fee) as deliver_fee_std,
            avg(recent_7_open_days) as recent_7_open_days_std,
            avg(recent_30_open_days) as recent_30_open_days_std,
            avg(recent_7_open_hours) as recent_7_open_hours_std,
            avg(recent_30_open_hours) as recent_30_open_hours_std,
            avg(recent_7_order_complain_cnt) as recent_7_order_complain_cnt_std,
            avg(recent_30_order_complain_cnt) as recent_30_order_complain_cnt_std,
            avg(recent_7_order_remind_cnt) as recent_7_order_remind_cnt_std,
            avg(recent_7_user_refuse_order_cnt) as recent_7_user_refuse_order_cnt_std,
            avg(recent_30_user_refuse_order_cnt) as recent_30_user_refuse_order_cnt_std,
            avg(recent_7_rst_refuse_order_cnt) as recent_7_rst_refuse_order_cnt_std,
            avg(recent_30_rst_refuse_order_cnt) as recent_30_rst_refuse_order_cnt_std,
            avg(recent_7_order_cnt) as recent_7_order_cnt_std,
            avg(recent_30_order_cnt) as recent_30_order_cnt_std,
            avg(recent_7_user_cnt) as recent_7_user_cnt_std,
            avg(recent_30_user_cnt) as recent_30_user_cnt_std,
            avg(recent_7_ord_usr_ratio) as recent_7_ord_usr_ratio_std,
            avg(recent_30_ord_usr_ratio) as recent_30_ord_usr_ratio_std,
            avg(recent_7_returned_customer_scale) as recent_7_returned_customer_scale_std,
            avg(recent_30_returned_customer_scale) as recent_30_returned_customer_scale_std,
            avg(recent_7_order_amt) as recent_7_order_amt_std,
            avg(recent_30_order_amt) as recent_30_order_amt_std,
            avg(recent_30_payment_amt) as recent_30_payment_amt_std,
            avg(recent_30_eleme_subsidy_amt) as recent_30_eleme_subsidy_amt_std,
            avg(recent_30_order_chargeback_scale) as recent_30_order_chargeback_scale_std,
            avg(recent_30_order_unpaid_cnt) as recent_30_order_unpaid_cnt_std,
            avg(recent_7_order_hongbao_preferential_avg) as recent_7_order_hongbao_preferential_avg_std
        from 
            temp.temp_rst_feat_table
        where 
            is_dianping=1
    ) t2
    on t1.rst_tag=t2.dianping_tag;


-- sub task 6.2: 筛选相似品质餐厅
drop table temp.temp_shop_subsidy_high_quality_rst;
create table temp.temp_shop_subsidy_high_quality_rst as 
    select
        t1.restaurant_id,
        t1.score,
        t2.order_cnt
    from(
        select
            restaurant_id, 
            score
        from 
            temp.temp_shop_subsidy_high_quality_rst_cosine
        where
            score>0.8
        ) t1
    join(
        select
            restaurant_id,
            count(id) as order_cnt
        from 
            dw.dw_trd_order_wide_day
        where 
            dt>=get_date('${day}',-61) and
            order_status=1
        group by 
            restaurant_id 
        having order_cnt>0  
        ) t2
    on(
        t1.restaurant_id=t2.restaurant_id
        ) 
;



-- sub task 7: 抽取新餐厅
drop table temp.temp_shop_subsidy_new_rst;
create table temp.temp_shop_subsidy_new_rst as
    select 
        t1.restaurant_id,
        max(t2.order_price_median) as order_price_median,
        max(t2.order_price_stddev) as order_price_stddev,
        max(t2.order_cnt) as order_cnt
    from(
        select 
            id as restaurant_id,
            substr(geohash_of_latlng(latitude,longitude),0,7) as geohash,
            is_license_no,
            is_royalty,
            is_kitchen,
            is_sia
        from 
            dw.dw_prd_restaurant_wide
        where 
            dt='${day}' and 
            is_valid=1 and 
            datediff('${day}',created_at)<31
        ) t1
    join(
        select
            geo_cluster,
            order_cnt,
            order_price_median,
            order_price_stddev
        from
            temp.temp_shop_subsidy_geohash_cluster_ord_info
        ) t2
    on(
        t1.geohash=t2.geo_cluster
        )
    where 
        t1.is_license_no=1  
    group by 
        t1.restaurant_id
    having
        order_cnt>1000
;




-- sub task 8: 汇总需要补贴的餐厅
drop table temp.temp_shop_subsidy_unfilter_rst;
create table temp.temp_shop_subsidy_unfilter_rst as 
    select 
        t.restaurant_id,
        max(t.rst_tag) as rst_tag 
    from(
        select 
            restaurant_id,
            '1_high_quality' as rst_tag
        from    
            temp.temp_shop_subsidy_high_quality_rst
        union all
        select
            restaurant_id,
            '2_dianping' as rst_tag
        from 
            temp.temp_shop_subsidy_dianping_rst
        union all
        select 
            restaurant_id,
            '3_new' as rst_tag
        from 
            temp.temp_shop_subsidy_new_rst
        union all
        select 
            restaurant_id,
            '4_sig' as rst_tag
        from 
            temp.temp_mdl_rst_subsidy_sig_rst
        union all
        select 
            restaurant_id,
            '5_ka' as rst_tag
        from 
            temp.temp_mdl_rst_subsidy_ka_rst
        ) t
    group by 
        t.restaurant_id
;
 

-- sub task 9: 补充订单多的餐厅 
drop table temp.temp_shop_subsidy_complement_rst;
create table temp.temp_shop_subsidy_complement_rst as 
    select
        t1.restaurant_id,
        t1.order_cnt,
        t1.total,
        '0_complement' as rst_tag
    from(
        select 
            restaurant_id,
            count(id) as order_cnt,
            sum(total) as total
        from 
            dw.dw_trd_order_wide_day
        where 
            dt>=get_date(-61) and
            order_status=1
        group by 
            restaurant_id
        ) t1
    left outer join(
        select
            restaurant_id
        from
            temp.temp_shop_subsidy_unfilter_rst
        ) t2
    on(
        t1.restaurant_id=t2.restaurant_id
        )
    where 
        t2.restaurant_id is null
;


-- sub task 10: 汇总所有餐厅
drop table temp.temp_shop_subsidy_all_rst;
create table temp.temp_shop_subsidy_all_rst as
    select
        a.restaurant_id,
        max(a.rst_tag) as rst_tag,
        max(b.cat0_name) as cat0_name,
        max(b.cat1_name) as cat1_name
    from(
        select 
            restaurant_id,
            rst_tag
        from 
            temp.temp_shop_subsidy_unfilter_rst
        union all 
        select
            restaurant_id,
            rst_tag
        from 
            temp.temp_shop_subsidy_complement_rst
        ) a
    join(
        select
            restaurant_id,
            cat0_name,
            cat1_name
        from 
            rec.rec_prf_restaurant_category_info 
        where 
            dt='${day}' and
            cat1_name not in ('鲜花','药品','超市','零食','水果','蔬菜','营养品')
        ) b
    on(
        a.restaurant_id=b.restaurant_id
        )
    group by a.restaurant_id
;



-- sub task 11: 为每家餐厅分配活动补贴策略
drop table temp.temp_mdl_rst_subsidy_strategy_sub1;
create table temp.temp_mdl_rst_subsidy_strategy_sub1 as 
    select
        t1.restaurant_id,
        t2.name,
        t1.rst_tag,
        t1.cat0_name,
        t1.cat1_name,
        case when t2.is_premium=1 then 
                case when t2.total_percentile_3_rst/t2.total_percentile_8_rst>0.7 then
                    concat(
                    '满',cast(t2.total_percentile_8_rst*1.2 as int),'减',cast(t2.total_percentile_8_rst*(0.25-0.05*rand()*rand()) as int)
                    )
                else
                    concat(
                        '满',cast(t2.total_percentile_8_rst*1.2 as int),'减',cast(t2.total_percentile_8_rst*(0.25-0.05*rand()*rand()) as int),'|',
                        '满',cast(t2.total_percentile_3_rst*1.2 as int),'减',cast(t2.total_percentile_3_rst*(0.2-0.1*rand()*rand()) as int)
                        )
                end
            when t1.rst_tag!='3_new' then 
                case when t2.total_percentile_3_rst/t2.total_percentile_8_rst>0.7 then
                    concat(
                        '满',cast(t2.total_percentile_8_rst as int),'减',cast(t2.total_percentile_8_rst*(0.25-0.05*rand()*rand()) as int)
                        )
                else
                    concat(
                        '满',cast(t2.total_percentile_8_rst as int),'减',cast(t2.total_percentile_8_rst*(0.25-0.05*rand()*rand()) as int),'|',
                        '满',cast(t2.total_percentile_3_rst as int),'减',cast(t2.total_percentile_3_rst*(0.2-0.1*rand()*rand()) as int)
                        )
                end
            else 
                case when t2.total_percentile_3_rst/t2.total_percentile_8_rst>0.7 then
                    concat(
                        '满',cast(t2.total_percentile_8_geo as int),'减',cast(t2.total_percentile_8_geo*(0.25-0.05*rand()*rand()) as int)
                        )
                else
                    concat(
                        '满',cast(t2.total_percentile_8_geo as int),'减',cast(t2.total_percentile_8_geo*(0.25-0.05*rand()*rand()) as int),'|',
                        '满',cast(t2.total_percentile_3_geo as int),'减',cast(t2.total_percentile_3_geo*(0.2-0.1*rand()*rand()) as int)
                        )
                end
            end as strategy,
        case when t2.is_premium=1 then 
                case when t2.total_percentile_3_rst/t2.total_percentile_8_rst>0.7 then
                    concat(
                        cast(t2.total_percentile_8_rst*1.2 as int),',',cast(t2.total_percentile_8_rst*0.25 as int)
                    )
                else
                    concat(
                        cast(t2.total_percentile_8_rst*1.2 as int),',',cast(t2.total_percentile_8_rst*0.25 as int),'|',
                        cast(t2.total_percentile_3_rst*1.2 as int),',',cast(t2.total_percentile_3_rst*0.2 as int)
                        )
                end
            when t1.rst_tag!='3_new' then 
                case when t2.total_percentile_3_rst/t2.total_percentile_8_rst>0.7 then
                    concat(
                        cast(t2.total_percentile_8_rst as int),',',cast(t2.total_percentile_8_rst*0.25 as int)
                        )
                else
                    concat(
                        cast(t2.total_percentile_8_rst as int),',',cast(t2.total_percentile_8_rst*0.25 as int),'|',
                        cast(t2.total_percentile_3_rst as int),',',cast(t2.total_percentile_3_rst*0.2 as int)
                        )
                end
            else 
                case when t2.total_percentile_3_rst/t2.total_percentile_8_rst>0.7 then
                    concat(
                        cast(t2.total_percentile_8_geo as int),',',cast(t2.total_percentile_8_geo*0.25 as int)
                        )
                else
                    concat(
                        cast(t2.total_percentile_8_geo as int),',',cast(t2.total_percentile_8_geo*0.25 as int),'|',
                        cast(t2.total_percentile_3_geo as int),',',cast(t2.total_percentile_3_geo*0.2 as int)
                        )
                end
            end as strategy_code,
        case when t2.total_percentile_3_rst/t2.total_percentile_8_rst>0.7 then
            '0'
            else
            '1'
            end as is_multi_strategy
    from 
        temp.temp_shop_subsidy_all_rst t1
    join(
        select
            a.restaurant_id,
            a.name,
            a.is_premium,
            a.geo_cluster,
            bound_data(b.total_percentile_8,15,150) as total_percentile_8_rst,
            bound_data(b.total_percentile_5,15,150) as total_percentile_5_rst,
            bound_data(b.total_percentile_3,15,150) as total_percentile_3_rst,
            bound_data(c.total_percentile_8,15,150) as total_percentile_8_geo,
            bound_data(c.total_percentile_5,15,150) as total_percentile_5_geo,
            bound_data(c.total_percentile_3,15,150) as total_percentile_3_geo
        from(
            select 
                id as restaurant_id,
                name,
                is_premium,
                substr(geohash_of_latlng(latitude,longitude),0,7) as geo_cluster,
                min_deliver_amount
            from
                dw.dw_prd_restaurant
            where 
                dt='${day}' and
                is_valid=1
            ) a
        left outer join(
            select
                restaurant_id,
                bound_data(percentile_approx(total,0.8),15,150) as total_percentile_8,
                bound_data(percentile_approx(total,0.5),15,150) as total_percentile_5,
                bound_data(percentile_approx(total,0.3),15,150) as total_percentile_3
            from
                dw.dw_trd_order_wide_day
            where
                dt>=get_date(-61) and
                order_status=1
            group by 
                restaurant_id
            ) b
        on(
            a.restaurant_id=b.restaurant_id
            )
        left outer join
            temp.temp_shop_subsidy_geohash_cluster_ord_info c
        on(
            a.geo_cluster=c.geo_cluster
            )
        ) t2
    on (
        t1.restaurant_id=t2.restaurant_id
        )
;


drop table temp.temp_mdl_rst_subsidy_strategy_sub2;
create table temp.temp_mdl_rst_subsidy_strategy_sub2 as 
    select 
        t1.restaurant_id,
        t1.name,
        t1.rst_tag,
        t3.geo_cluster,
        t1.cat0_name,
        t1.cat1_name,
        t1.strategy,
        t1.strategy_code,
        t1.is_multi_strategy,
        t2.cur_month_total,
        t2.lst_month_total,
        round(t2.cur_month_total/t2.lst_month_total,2) as sales_ratio
    from
        temp.temp_mdl_rst_subsidy_strategy_sub1 t1
    join(
        select
            restaurant_id,
            round(sum(
                case when month(created_at)=month('${day}') then
                        total
                    else
                        0
                    end),2) as cur_month_total,
            round(sum(
                case when month(created_at)=month('${day}')-1 then
                        total
                    else
                        0
                    end),2) as lst_month_total
        from
            dw.dw_trd_order_wide_day
        where  
            dt>=get_date(-61) and
            order_status=1
        group by 
            restaurant_id
        ) t2
    on(
        t1.restaurant_id=t2.restaurant_id
        )
    join(
        select
            id as restaurant_id,
            substr(geohash_of_latlng(latitude,longitude),0,7) as geo_cluster
        from 
            dw.dw_prd_restaurant
        where 
            dt='${day}' and 
            is_valid=1
        ) t3
    on(
        t1.restaurant_id=t3.restaurant_id
        )
;


drop table temp.temp_mdl_rst_cate_eleme_subsidy_scale;
create table temp.temp_mdl_rst_cate_eleme_subsidy_scale as 
    select 
        b.cat1_name, 
        round(sum(restaurant_subsidy)/(sum(eleme_subsidy)+sum(restaurant_subsidy)),2) as rst_subsidy_scale, 
        round(sum(eleme_subsidy)/(sum(eleme_subsidy)+sum(restaurant_subsidy)),2) as ele_subsidy_scale,
        round(sum(restaurant_subsidy)/sum(total),2) as rst_subsidy_ratio, 
        round(sum(eleme_subsidy)/sum(total),2) as ele_subsidy_ratio
    from(
        select 
            restaurant_id,
            sum(restaurant_subsidy) as restaurant_subsidy, 
            sum(eleme_subsidy) as eleme_subsidy,
            sum(total) as total
        from 
            dw.dw_trd_order_wide_day 
        where 
            dt>get_date(-31) and 
            order_status=1
        group by 
            restaurant_id
        ) a
    join(
        select 
            restaurant_id,
            cat1_name 
        from 
            rec.rec_prf_restaurant_category_info 
        where 
            dt='${day}'
        ) b
    on(
        a.restaurant_id=b.restaurant_id
        )
    group by 
        b.cat1_name
;


drop table temp.temp_mdl_rst_subsidy_strategy;
create table temp.temp_mdl_rst_subsidy_strategy as 
    select
        t1.restaurant_id,
        name,
        rst_tag,
        geo_cluster,
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
        lastest_subsidy,
        is_multi_strategy,
        cur_month_total,
        lst_month_total,
        round(lst_month_total*0.09,2) as subsidy_total_limit,
        sales_ratio
    from 
        temp.temp_mdl_rst_subsidy_strategy_sub2 t1
    join 
        temp.temp_mdl_rst_cate_eleme_subsidy_scale t2
    on (
        t1.cat1_name=t2.cat1_name
        )
    join(
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
                dw.dw_trd_order_wide_day
            where 
                dt>=get_date(-3) and
                order_status=1
            ) t
        where 
            t.rno<4
        group by 
            t.restaurant_id
        ) t3
    on(
        t1.restaurant_id=t3.restaurant_id
        )
;



insert overwrite table dm.dm_mdl_restaurant_eleme_subsidy_strategy partition(dt='${day}')
    select
        restaurant_id,
        name,
        rst_tag,
        geo_cluster,
        cat0_name,
        cat1_name,
        strategy,
        strategy_code,
        strategy_detail,
        lastest_subsidy,
        is_multi_strategy,
        cur_month_total,
        lst_month_total,
        subsidy_total_limit,
        sales_ratio
    from 
        temp.temp_mdl_rst_subsidy_strategy
    where 
        restaurant_id!=249330 and
        strategy is not null and
        strategy_code is not null
;



insert overwrite table dm.dm_mdl_restaurant_eleme_subsidy_strategy partition(dt='3000-12-31')
    select
        restaurant_id,
        name,
        rst_tag,
        geo_cluster,
        cat0_name,
        cat1_name,
        strategy,
        strategy_code,
        strategy_detail,
        lastest_subsidy,
        is_multi_strategy,
        cur_month_total,
        lst_month_total,
        subsidy_total_limit,
        sales_ratio
    from 
        temp.temp_mdl_rst_subsidy_strategy
    where 
        restaurant_id!=249330 and
        strategy is not null and
        strategy_code is not null
;