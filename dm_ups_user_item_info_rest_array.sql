#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_user_item_info_rest_array.sql
# **  功能描述：
#         1. 基于流量组提供的用户餐厅交叉特征，导入到用户画像中
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2017-01-01 16:00:00
# **
# **  ChangeLog：
#
#***************************************************************************************************

#### 得到用户手机归属地信息

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt = '${day}', flag = 'bs_user_shop_array')
SELECT
    user_id,
    'rec' AS top_category,
    'bs_user_rest' AS attr_key,
    value AS attr_value,
    1 AS is_json,
    '${day}' AS update_time
FROM
    st.st_bs_user_shop_portrait
WHERE 
    dt='${day}'
;


#### copy to dt=3000-12-31

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE dm.dm_ups_user_item_info PARTITION(dt = '3000-12-31', flag)
SELECT
    user_id, 
    top_category, 
    attr_key, 
    attr_value, 
    is_json, 
    update_time, 
    flag
FROM
    dm.dm_ups_user_item_info
WHERE
    dt = '${day}' AND 
    flag IN ('bs_user_shop_array')
;
