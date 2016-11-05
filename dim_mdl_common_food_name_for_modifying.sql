#***************************************************************************************************
# ** 文件名称： dim_mdl_common_food_name_for_modifying.sql
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-22
#***************************************************************************************************


INSERT OVERWRITE TABLE dim.dim_mdl_common_food_name PARTITION(part='base')
SELECT t1.food_name_pattern, t1.normal_food_name, t1.priority, t1.last_update_time, t2.tag_function, t1.tag_scene, t3.category, t1.flavor, t3.category_set
FROM(
SELECT food_name_pattern, normal_food_name, priority, last_update_time, tag_scene, category, flavor
FROM dim.dim_mdl_common_food_name
WHERE part='base'
) t1
LEFT OUTER JOIN(
SELECT food_name, concat('[',concat_ws(',',collect_set(concat('\"',category,'\"'))),']') as tag_function
FROM dim.dim_mdl_food_tag_classification
WHERE part='class1_function'
GROUP BY food_name
) t2
ON t1.normal_food_name=t2.food_name
LEFT OUTER JOIN(
SELECT food_name, priority, category, flavor, method, category_set
FROM dim.dim_mdl_food_tag_classification
WHERE part='class1_3'
) t3
ON t1.normal_food_name=t3.food_name; 