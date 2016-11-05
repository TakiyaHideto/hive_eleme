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

-- 检测补贴金额上限 150
INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=1)
    SELECT
        t.restaurant_id AS id,
        'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
        '${day}' AS date_at,
        'jiahao.dong,yongliang.zhang' AS dq_man,
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
                split(split(strategy_code,' ')[0],',')[0]
            else
                split(strategy_code,',')[0]
            end as subsidy_threshold_sub
        FROM 
            dm.dm_mdl_restaurant_eleme_subsidy_strategy
        WHERE 
            dt='3000-12-31'
        ) t
    WHERE
        t.subsidy_threshold_sub>150
;
-- INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt = '${day}', p_dq_table = 'dm_ups_user_item_info', p_id = 1)
-- SELECT
--     a.user_id AS id,
--     'dm_ups_user_item_info' AS dq_table,
--     '${day}' AS date_at,
--     'yongliang.zheng,weihua.zheng' AS dq_man,
--     'multi-user_id:top_category:attr_key' AS dq_type,
--     'user_id, top_category, attr_key' AS dq_column,
--     1 AS is_error,
--     'multi-user_id:top_category:attr_key exists' AS  error_info,
--     'error' AS error_type,
--     FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
-- FROM
-- (
--     SELECT
--         user_id, top_category, attr_key, COUNT(*) AS number
--     FROM
--         dm.dm_ups_user_item_info
--     WHERE
--         dt = '3000-12-31'
--     GROUP BY
--         user_id, top_category, attr_key
-- ) a
-- WHERE
--     a.number > 1
-- LIMIT 1;





-- -- 检测补贴折扣上限 0.75
-- INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=2)
--     SELECT
--         t.restaurant_id AS id,
--         'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
--         '${day}' AS date_at,
--         'jiahao.dong,yongliang.zhang' AS dq_man,
--         'exceed subsidy_discount_threshold_sub' AS dq_type,
--         'subsidy_threshold_sub' AS dq_column,
--         1 AS is_error,
--         'exceed subsidy_discount_threshold_sub' AS error_info,
--         'error' AS error_type,
--         FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
--     FROM(
--         SELECT
--             restaurant_id,
--             case when is_multi_strategy=1 then
--                 (split(split(strategy_code,' ')[0],',')[0]-split(split(strategy_code,'|')[0],',')[1])/split(split(strategy_code,'|')[0],',')[0]
--             else
--                 (split(strategy_code,',')[0]-split(strategy_code,',')[1])/split(strategy_code,',')[0]
--             end as subsidy_threshold_sub
--         FROM 
--             dm.dm_mdl_restaurant_eleme_subsidy_strategy
--         WHERE 
--             dt='3000-12-31'
--         ) t
--     WHERE
--         t.subsidy_threshold_sub<0.75
-- ;





-- -- 检测补贴前后金额大小关系 
-- INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=3)
--     SELECT
--         t.restaurant_id AS id,
--         'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
--         '${day}' AS date_at,
--         'jiahao.dong,yongliang.zhang' AS dq_man,
--         'exceed subsidy_discount_threshold_sub' AS dq_type,
--         'subsidy_threshold_sub' AS dq_column,
--         1 AS is_error,
--         'exceed subsidy_discount_threshold_sub' AS error_info,
--         'error' AS error_type,
--         FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
--     FROM(
--         SELECT
--             restaurant_id,
--             case when is_multi_strategy=1 then
--                 if(split(split(strategy_code,' ')[0],',')[0]>split(split(strategy_code,'|')[0],',')[1],0,1)
--             else
--                 if(split(strategy_code,',')[0]>split(strategy_code,',')[1],0,1)
--             end as subsidy_threshold_sub
--         FROM 
--             dm.dm_mdl_restaurant_eleme_subsidy_strategy
--         WHERE 
--             dt='3000-12-31'
--         ) t
--     WHERE
--         t.subsidy_threshold_sub=1
-- ;




-- -- 检测试剂补贴金额上限 40
-- INSERT OVERWRITE TABLE dq.dq_table PARTITION(dt='${day}',p_dq_table='dm_mdl_restaurant_eleme_subsidy_strategy', p_id=4)
--     SELECT
--         t.restaurant_id AS id,
--         'dm_mdl_restaurant_eleme_subsidy_strategy' AS dq_table,
--         '${day}' AS date_at,
--         'jiahao.dong,yongliang.zhang' AS dq_man,
--         'exceed subsidy_amount_sub' AS dq_type,
--         'subsidy_threshold_sub' AS dq_column,
--         1 AS is_error,
--         'exceed subsidy_amount_sub' AS  error_info
--         'error' AS error_type,
--         FROM_UNIXTIME(UNIX_TIMESTAMP()) AS dw_date
--     FROM(
--         SELECT
--             restaurant_id,
--             case when is_multi_strategy=1 then
--                 split(split(strategy_code,' ')[0],',')[1]
--             else
--                 split(strategy_code,',')[1]
--             end as subsidy_threshold_sub
--         FROM 
--             dm.dm_mdl_restaurant_eleme_subsidy_strategy
--         WHERE 
--             dt='3000-12-31'
--         ) t
--     WHERE
--         t.subsidy_threshold_sub>40
-- ;


