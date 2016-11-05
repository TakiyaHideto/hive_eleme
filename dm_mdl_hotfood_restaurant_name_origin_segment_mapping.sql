#***************************************************************************************************
# **  文件名称： dm.dm_mdl_hotfood_restaurant_name_origin_segment_mapping.sql
# **  功能描述： 餐厅名称 去掉分店店名或路名
# **  创建者： jiahao.dong
# **  创建日期： 2016-08-05
#***************************************************************************************************


DROP TABLE dm.dm_mdl_hotfood_restaurant_name_origin_segment_mapping;
CREATE TABLE dm.dm_mdl_hotfood_restaurant_name_origin_segment_mapping AS
SELECT *
FROM temp.temp_restaurant_seg_name_mapping

