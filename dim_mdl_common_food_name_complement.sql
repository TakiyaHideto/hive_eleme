#***************************************************************************************************
# ** 文件名称： dim_mdl_common_food_name_complement.sql
# ** 功能描述： 补充dim_mdl_common_food_name_complement表中正则菜品名称
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-09-26
#***************************************************************************************************

-- sub task 1: 从原本地读取正则化名称的temp表中读取数据
drop table temp.temp_normalize_food_name_complementary_copy;
create table temp.temp_normalize_food_name_complementary_copy as
select food_name_pattern, 
    normal_food_name, 
    priority,
    '${day}' as last_update_time,
    'invalid' as tag_function,
    'invalid' as tag_scene,
    'invalid' as category,
    'invalid' as flavor,
    'invalid' as tag_category
from temp.temp_normalize_food_name_complementary;


-- sub task 2: 整合所有正则化名称, dim_mdl_common_food_name表中选取2016-10-16以前稳定的数据
drop table temp.temp_mdl_common_food_name_complement;
create table temp.temp_mdl_common_food_name_complement as 
select t.food_name_pattern, t.normal_food_name,
    max(t.priority) as priority,
    max(t.last_update_time) as last_update_time,
    max(t.tag_function) as tag_function,
    max(t.tag_scene) as tag_scene,
    max(t.category) as category,
    max(t.flavor) as flavor,
    max(t.tag_category) as tag_category
from(
    select food_name_pattern, 
        normal_food_name, 
        priority,
        last_update_time,
        tag_function,
        tag_scene,
        category,
        flavor,
        tag_category
    from dim.dim_mdl_common_food_name
    where part='base' and last_update_time<'2016-10-16'
    union all
    select food_name_pattern, 
        normal_food_name, 
        priority,
        case when last_update_time='invalid' then null else last_update_time end as last_update_time,
        case when tag_function='invalid' then null else tag_function end as tag_function,
        case when tag_scene='invalid' then null else tag_scene end as tag_scene,
        case when category='invalid' then null else category end as category,
        case when flavor='invalid' then null else flavor end as flavor,
        case when tag_category='invalid' then null else tag_category end as tag_category
    from temp.temp_normalize_food_name_complementary_copy
) t
group by t.food_name_pattern, t.normal_food_name;


-- sub task 3: import data to table
insert overwrite table dim.dim_mdl_common_food_name partition(part='base')
select food_name_pattern, normal_food_name, priority, last_update_time, 
    tag_function, tag_scene, category, flavor, tag_category
from temp.temp_mdl_common_food_name_complement;

--