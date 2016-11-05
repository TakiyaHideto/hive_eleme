#***************************************************************************************************
# **  文件名称： dim_ups_key_info.sql
# **  功能描述： 插入用户画像中可以使用的标签信息
#
# **  创建者：weihua.zheng@ele.me
# **  创建日期： 2016-08-23 14：30：00
# **
# **  ChangeLog
#
#***************************************************************************************************

INSERT OVERWRITE TABLE dim.dim_ups_key_info
SELECT
   a.top_category,
   CONCAT('$.', a.top_category, '.', a.key) AS key,
   a.name_cn,
   a.type,
   a.description,
   a.owner
FROM
(
    ------ base info ------
    SELECT
        'base' AS top_category,
        'source' AS key,
        '注册来源' AS name_cn,
        'int' AS type,
        '1.直接注册 2.微博 3.人人网 4.微信 5.qq' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'base' AS top_category,
        'gender' AS key,
        '性别' AS name_cn,
        'int' AS type,
        '1.男 2.女' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    ------ device info ------
    SELECT
        'device' AS top_category,
        'os_platform' AS key,
        '操作系统平台' AS name_cn,
        'string' AS type,
        '如: android ios' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'device' AS top_category,
        'network_type' AS key,
        '常用网络类型' AS name_cn,
        'string' AS type,
        '网络类型: 2g, 3g, 4g, wifi' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    ------ log info ------
    SELECT
        'log' AS top_category,
        'first_visit_time' AS key,
        '首次来访时间' AS name_cn,
        'string' AS type,
        'datetime, 如: 2016-01-01 12:00:00' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'log' AS top_category,
        'last_visit_time' AS key,
        '最近来访时间' AS name_cn,
        'string' AS type,
        'datetime, 如: 2016-01-01 12:00:00' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'log' AS top_category,
        'visit_city_id' AS key,
        '浏览常驻城市' AS name_cn,
        'int' AS type,
        'eleme city id' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'log' AS top_category,
        'visit_province_id' AS key,
        '浏览常驻省份' AS name_cn,
        'int' AS type,
        '省份id, 如: 310000' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'log' AS top_category,
        'visit_time_prefer' AS key,
        '浏览时间偏好' AS name_cn,
        'string' AS type,
        'json, 5个时间段: lunch, tea, supper, night, other' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'log' AS top_category,
        'visit_date_prefer' AS key,
        '浏览日期偏好' AS name_cn,
        'string' AS type,
        '{"workday":"0.8","weekend":"0.2"}' AS description,
        'xuewei.zhang' AS owner

    UNION ALL

    ------ trd info ------
    SELECT
        'trd' AS top_category,
        'point' AS key,
        '积分' AS name_cn,
        'int' AS type,
        '用户积分' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'first_order_time' AS key,
        '首次订单时间' AS name_cn,
        'string' AS type,
        'datetime, 如: 2016-01-01 12:00:00' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'last_order_time' AS key,
        '最近订单时间' AS name_cn,
        'string' AS type,
        'datetime, 如: 2016-01-01 12:00:00' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_city_id' AS key,
        '下单常驻城市' AS name_cn,
        'int' AS type,
        'eleme city id' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_province_id' AS key,
        '下单常驻省份' AS name_cn,
        'int' AS type,
        '省份id, 如: 310000' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_time_prefer' AS key,
        '下单时间偏好' AS name_cn,
        'string' AS type,
        'json, 5个时间段: lunch, tea, supper, night, other' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_cnt' AS key,
        '累计订单数' AS name_cn,
        'int' AS type,
        '累计订单数' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_amt' AS key,
        '累计订单金额' AS name_cn,
        'double' AS type,
        '实际付款订单总额, 保留3位小数' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_subsidy' AS key,
        '累计订单补贴' AS name_cn,
        'double' AS type,
        '累计订单不提总额, 保留3位小数' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_hongbao' AS key,
        '累计红包使用金额' AS name_cn,
        'double' AS type,
        '累计红包使用金额, 保留3位小数' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_avg_fee' AS key,
        '客单价' AS name_cn,
        'double' AS type,
        'order_amt / order_cnt, 保留3位小数' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_bu_flag' AS key,
        '订单类型' AS name_cn,
        'string' AS type,
        'bl:白领 gx:高校 other:其他' AS description,
        'weihua.zheng' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'consume_level' AS key,
        '消费水平' AS name_cn,
        'double' AS type,
        '0-100百分位数' AS description,
        'chengliang.liu' AS owner

    UNION ALL

    SELECT
        'trd' AS top_category,
        'order_date_prefer' AS key,
        '下单日期偏好' AS name_cn,
        'string' AS type,
        '{"workday":"0.8","weekend":"0.2"}' AS description,
        'xuewei.zhang' AS owner

    UNION ALL

    ------ rec info ------
    SELECT
        'rec' AS top_category,
        'style_prefer' AS key,
        '食品风格偏好' AS name_cn,
        'string' AS type,
        'json' AS description,
        'jiahao.dong' AS owner

    UNION ALL

    SELECT
        'rec' AS top_category,
        'food_prefer' AS key,
        '食物偏好' AS name_cn,
        'string' AS type,
        'json' AS description,
        'jiahao.dong' AS owner

    UNION ALL

    SELECT
        'rec' AS top_category,
        'flavor_prefer' AS key,
        '食品口味偏好' AS name_cn,
        'string' AS type,
        'json' AS description,
        'jiahao.dong' AS owner

    UNION ALL

    SELECT
        'rec' AS top_category,
        'category_prefer' AS key,
        '食品类别偏好' AS name_cn,
        'string' AS type,
        'json' AS description,
        'jiahao.dong' AS owner
) a;

