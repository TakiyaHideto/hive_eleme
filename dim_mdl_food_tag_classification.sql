#***************************************************************************************************
# ** 文件名称： dim_mdl_food_tag_classification.sql
# ** 功能描述： 导入temp.temp_food_tag_classification
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-08-22
#***************************************************************************************************

DROP TABLE dim.dim_mdl_food_tag_classification;
CREATE TABLE dim.dim_mdl_food_tag_classification(
food_name STRING, 
priority INT,
category STRING,
flavor STRING,
method STRING,
category_set STRING
) PARTITIONED BY (part STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE ; 

INSERT OVERWRITE TABLE dim.dim_mdl_food_tag_classification PARTITION(part='class1_2')
SELECT food_name, priority, category, flavor, method, null
FROM temp.temp_food_tag_classification
WHERE part='class1';

INSERT OVERWRITE TABLE dim.dim_mdl_food_tag_classification PARTITION(part='class2_3')
SELECT food_name, priority, category, flavor, method, null
FROM temp.temp_food_tag_classification
WHERE part='class2';

INSERT OVERWRITE TABLE dim.dim_mdl_food_tag_classification PARTITION(part='class3_4')
SELECT food_name, priority, category, flavor, method, null
FROM temp.temp_food_tag_classification
WHERE part='class3';

INSERT OVERWRITE TABLE dim.dim_mdl_food_tag_classification PARTITION(part='class4_function')
SELECT food_name, priority, category, flavor, method, null
FROM temp.temp_food_tag_classification
WHERE part='class4_function';

DROP TABLE temp.temp_food_tag_classification_class1_3;
CREATE TABLE temp.temp_food_tag_classification_class1_3 AS
SELECT t.food_name, t.priority, t.category, t.flavor, t.method, row_number() over (PARTITION BY food_name ORDER BY t.priority desc) AS rno
FROM(
SELECT t4.food_name, t3.priority, t3.category, t3.flavor, t3.method
FROM(
SELECT t2.food_name, t1.priority, t1.category, t1.flavor, t1.method
FROM (
SELECT food_name, priority, category, flavor, method
FROM temp.temp_food_tag_classification
WHERE part='class3'
) t1
JOIN (
SELECT food_name, category, flavor, method
FROM temp.temp_food_tag_classification
WHERE part='class2'
) t2
ON t1.food_name=t2.category
) t3
JOIN (
SELECT food_name, category, flavor, method
FROM temp.temp_food_tag_classification
WHERE part='class1'
) t4
ON t3.food_name=t4.category
GROUP BY t4.food_name, t3.priority, t3.category, t3.flavor, t3.method) t;

INSERT OVERWRITE TABLE dim.dim_mdl_food_tag_classification PARTITION(part='class1_3')
SELECT t1.food_name,t1.priority, t1.category, t1.flavor, t1.method, t2.category_set
FROM(
SELECT food_name, priority, category, flavor, method
FROM temp.temp_food_tag_classification_class1_3
WHERE rno=1) t1
JOIN(
SELECT food_name, concat('[',concat_ws(',',collect_set(case when category!='暂无' then concat('\"',category,'\"') else null end)), ']') as category_set
FROM temp.temp_food_tag_classification_class1_3
GROUP BY food_name
)t2
ON t1.food_name=t2.food_name;


INSERT OVERWRITE TABLE dim.dim_mdl_food_tag_classification PARTITION(part='class1_function')
SELECT t4.food_name, length(t4.food_name) as priority, t4.func, null, null, null
FROM(
SELECT 
CASE WHEN func='休闲甜品' and (t3.food_name like '%饭%' or t3.food_name like '%面%') THEN null 
WHEN func='休闲甜品' and (t3.food_name like '%蛋%' and t3.food_name not like '%蛋仔%') THEN null
WHEN func='休闲甜品' and (t3.food_name like '%鱿鱼%') THEN null
WHEN func='小资情调' and (t3.food_name like '%鱿鱼%') THEN null
WHEN func='滋补养颜' and t3.food_name like '%鱿鱼%' THEN null
WHEN func='减肥塑形' and (t3.food_name like '%黄焖%' or t3.food_name like '%肥%' or t3.food_name like '%排骨%') THEN null
WHEN func='狂野食肉' and (t3.food_name like '%泡椒凤爪%' or t3.food_name like '%丸%' or t3.food_name like '%套餐%' or t3.food_name like '%饭%' or t3.food_name like '%面%') THEN null
WHEN func='最炫麻辣风' and t3.food_name like '%青椒%' THEN null else t3.food_name END AS food_name, t3.func
FROM(
SELECT t2.food_name, t1.func
FROM(
SELECT food_name, func 
FROM(
SELECT food_name, category 
FROM dim.dim_mdl_food_tag_classification 
WHERE part='class4_function' and category is not null and category!=''
) t
LATERAL VIEW EXPLODE(split(category,'#')) tmpTable AS func
GROUP BY food_name, func
) t1
JOIN(
SELECT *
FROM dim.dim_mdl_food_tag_classification
WHERE part='class1_3'
) t2
ON t1.food_name=t2.category
) t3 
) t4
WHERE t4.func is not null and t4.func!='' and t4.food_name is not null
GROUP BY t4.food_name, length(t4.food_name), t4.func;
