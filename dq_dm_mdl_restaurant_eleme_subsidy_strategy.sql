#***************************************************************************************************
# **  文件名称： dq_dm_mdl_restaurant_eleme_subsidy_strategy.sql
# **  功能描述：检测商家补贴异常
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2016-11-01 
# **
# **  ChangeLog
#
#***************************************************************************************************

-- 检测补贴金额上限 180
INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=1)
    SELECT
        t.restaurant_id AS id,
        'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
        '${day}' AS date_at,
        'jiahao.dong' AS dq_man,
        'exceed subsidy_amount_threshold_sub' AS dq_type,
        'subsidy_threshold_sub' AS dq_column,
        1 AS is_error,
        'exceed subsidy_amount_threshold_sub' AS error_info,
        'error' AS error_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
    FROM(
        SELECT
            restaurant_id,
            case when is_multi_strategy=1 then
                split(split(strategy_code,'\\|')[0],',')[0]
            else
                split(strategy_code,',')[0]
            end as subsidy_threshold_sub
        FROM 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        WHERE 
            dt='3000-12-31'
        ) t
    WHERE
        t.subsidy_threshold_sub>180
    LIMIT 1
;


-- 检测补贴金额下限 10
INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=2)
    SELECT
        t.restaurant_id AS id,
        'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
        '${day}' AS date_at,
        'jiahao.dong' AS dq_man,
        'lower subsidy_amount_threshold_limit' AS dq_type,
        'subsidy_threshold_sub' AS dq_column,
        1 AS is_error,
        'lower subsidy_amount_threshold_limit' AS error_info,
        'error' AS error_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
    FROM(
        SELECT
            restaurant_id,
            case when is_multi_strategy=1 then
                split(split(strategy_code,'\\|')[1],',')[0]
            else
                split(strategy_code,',')[0]
            end as subsidy_threshold_sub
        FROM 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        WHERE 
            dt='3000-12-31'
        ) t
    WHERE
        t.subsidy_threshold_sub<10
    LIMIT 1
;


-- 检测补贴折扣上限 0.75
INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=3)
    SELECT
        t.restaurant_id AS id,
        'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
        '${day}' AS date_at,
        'jiahao.dong' AS dq_man,
        'exceed subsidy_discount_threshold_sub' AS dq_type,
        'subsidy_threshold_sub' AS dq_column,
        1 AS is_error,
        'exceed subsidy_discount_threshold_sub' AS error_info,
        'error' AS error_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
    FROM(
        SELECT
            restaurant_id,
            case when is_multi_strategy=1 then
                (split(split(strategy_code,'\\|')[0],',')[0]-split(split(strategy_code,'\\|')[0],',')[1])/split(split(strategy_code,'\\|')[0],',')[0]
            else
                (split(strategy_code,',')[0]-split(strategy_code,',')[1])/split(strategy_code,',')[0]
            end as subsidy_threshold_sub
        FROM 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        WHERE 
            dt='3000-12-31'
        ) t
    WHERE
        t.subsidy_threshold_sub<0.75
    LIMIT 1
;





-- 检测补贴前后金额大小关系 
INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=4)
    SELECT
        t.restaurant_id AS id,
        'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
        '${day}' AS date_at,
        'jiahao.dong' AS dq_man,
        'total less than subsidy' AS dq_type,
        'subsidy_threshold_sub' AS dq_column,
        1 AS is_error,
        'total less than subsidy' AS error_info,
        'error' AS error_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
    FROM(
        SELECT
            restaurant_id,
            case when is_multi_strategy=1 then
                if(cast(split(split(strategy_code,'\\|')[0],',')[0] as int)>cast(split(split(strategy_code,'\\|')[0],',')[1] as int),0,1)
            else
                if(cast(split(strategy_code,',')[0] as int)>cast(split(strategy_code,',')[1] as int),0,1)
            end as subsidy_threshold_sub
        FROM 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        WHERE 
            dt='3000-12-31'
        ) t
    WHERE
        t.subsidy_threshold_sub=1
    LIMIT 1
;



-- 检测试剂补贴金额上限 40
INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=5)
    SELECT
        t.restaurant_id AS id,
        'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
        '${day}' AS date_at,
        'jiahao.dong,yongliang.zhang' AS dq_man,
        'exceed subsidy_amount_sub' AS dq_type,
        'subsidy_threshold_sub' AS dq_column,
        1 AS is_error,
        'exceed subsidy_amount_sub' AS  error_info,
        'error' AS error_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
    FROM(
        SELECT
            restaurant_id,
            case when is_multi_strategy=1 then
                split(split(strategy_code,'\\|')[0],',')[1]
            else
                split(strategy_code,',')[1]
            end as subsidy_threshold_sub
        FROM 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        WHERE 
            dt='3000-12-31'
        ) t
    WHERE
        t.subsidy_threshold_sub>40
    LIMIT 1
;



-- 检测实际付款金额下限 7.5
INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=6)
    SELECT
        t.restaurant_id AS id,
        'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
        '${day}' AS date_at,
        'jiahao.dong,yongliang.zhang' AS dq_man,
        'lower actual payment' AS dq_type,
        'subsidy_threshold_sub' AS dq_column,
        1 AS is_error,
        'lower actual payment' AS  error_info,
        'error' AS error_type,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
    FROM(
        SELECT
            restaurant_id,
            case when is_multi_strategy=1 then
                split(split(strategy_code,'\\|')[0],',')[0]-split(split(strategy_code,'\\|')[0],',')[1]
            else
                split(strategy_code,',')[0]-split(strategy_code,',')[1]
            end as subsidy_threshold_sub
        FROM 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        WHERE 
            dt='3000-12-31'
        ) t
    WHERE
        t.subsidy_threshold_sub<7.5
    LIMIT 1
;


