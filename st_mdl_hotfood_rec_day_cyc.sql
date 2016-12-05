#***************************************************************************************************
# ** 文件名称： st_mdl_hotfood_rec_day_cyc.sql
# ** 功能描述：
# ** 创建者： yongliang.zhang
# ** 修改：jiahao.dong@ele.me
# ** 创建日期： 2016年06月20日
# ** 修改日期 修改人 修改内容
# **
#***************************************************************************************************
 -- 计算所有有效餐厅的有效食物基本信息--

DROP TABLE IF EXISTS temp.temp_hotfood_rec_rst_food_${dt};
CREATE TABLE temp.temp_hotfood_rec_rst_food_${dt} AS
SELECT f.restaurant_id,
       f.id food_id,
       f.price,
       f.name food_name,
       coalesce(d.7_quantity, 0) 7_quantity,
       coalesce(d.30_quantity, 0) 30_quantity,
       substring(geohash_of_latlng(r.latitude, r.longitude), 1, 5) geohash,
       r.city_id,
       if(f.name LIKE '%月饼%' OR f.name LIKE '%巧克力%' OR f.name LIKE '%德芙%' OR f.name LIKE '%金帝%' OR f.name LIKE '%费列罗%', 1, 0) is_gift
FROM
  (SELECT *
   FROM dw.dw_prd_restaurant
   WHERE dt='${day}'
     AND is_valid=1) r
INNER JOIN
  (SELECT *
   FROM dw.dw_prd_food
   WHERE dt='${day}'
     AND is_valid=1
     AND length(image_hash)>10
     AND restaurant_id <> 249330
     AND price>=5
     AND price<=500
     AND name NOT LIKE '%可乐%'
     AND name NOT LIKE '%王老吉%'
     AND name NOT LIKE '%雪碧%'
     AND name NOT LIKE '%加多宝%'
     AND name NOT LIKE '%矿泉水%'
     AND name NOT LIKE '%纯净水%'
     AND name NOT LIKE '%康师傅%'
     AND name NOT LIKE '%饮用水%'
     AND name NOT LIKE '%汽水%'
     AND name NOT LIKE '%酒水%'
     AND name NOT LIKE '%红牛%'
     AND name NOT LIKE '%雀巢%'
     AND name NOT LIKE '%上好佳%'
     AND name NOT LIKE '%旺仔%'
     AND name NOT LIKE '%统一%'
     AND name NOT LIKE '%百岁山%'
     AND name NOT LIKE '%农夫山泉%'
     AND name not LIKE '%百威%'
     AND name NOT LIKE '%健力宝%'
     AND name NOT LIKE '%芬达%'
     AND name NOT LIKE '%美年达%'
     AND name NOT LIKE '%汇源%'
     AND name NOT LIKE '%美汁源%'
     AND name NOT LIKE '%旺仔%'
     AND name NOT LIKE '%营养快线%'
     AND name NOT LIKE '%伊利%'
     AND name NOT LIKE '%蒙牛%'
     AND name NOT LIKE '%椰树%'
     AND NAME NOT LIKE '%养乐多%'
     AND name NOT LIKE '%脉动%'
     AND name NOT LIKE '%格瓦斯%'
     AND name NOT LIKE '%银鹭%'
     AND name NOT LIKE '%天喔%'
     AND name NOT LIKE '%东方树叶%'
     AND name NOT LIKE '%七喜%'
     AND name NOT LIKE '%雪碧%'
     AND name NOT LIKE '%立顿%'
     AND name NOT LIKE '%光明%'
     AND name NOT LIKE '%农夫果园%'
     AND name NOT LIKE '%娃哈哈%'
     AND name NOT LIKE '%恒大冰泉%'
     AND NAME NOT LIKE '%锐奥%'
     AND NAME NOT LIKE '%雪花%'
     AND name NOT LIKE '%青岛%'
     AND name NOT LIKE '%燕京%'
     AND name NOT LIKE '%珠江%'
     AND name NOT LIKE '%茅台%'
     AND name NOT LIKE '%五粮液%'
     AND name NOT LIKE '%洋河大曲%'
     AND name NOT LIKE '%泸州老窖%'
     AND name NOT LIKE '%汾酒%'
     AND name NOT LIKE '%郎酒%'
     AND NAME NOT  LIKE '%哈尔滨%'
     AND name NOT LIKE '%古井贡%'
     AND name NOT LIKE '%西凤酒%'
     AND name NOT LIKE '%董酒%'
     AND name NOT LIKE '%剑南春%'
     AND name NOT LIKE '%果粒橙%'
     AND name NOT LIKE '冰红茶%'
     AND name NOT LIKE '%味全%'
     AND name NOT LIKE '%葡萄酒%'
     AND name NOT LIKE '%饮料%'
     AND name NOT LIKE '%干红%'
     AND name NOT LIKE '%瓜子%'
     AND name NOT LIKE '%优活%'
     AND name NOT LIKE '%张裕%'
     AND name not like '%张家街%'
     AND name NOT LIKE '%小样%'
     AND NAME!='绿茶'
     AND NAME NOT LIKE '绿茶%ML'
     AND NAME NOT LIKE '%豆奶'
     AND NAME NOT LIKE '%优益C%'
     AND NAME NOT LIKE '%益达%'
     AND NAME NOT LIKE '%怡宝%'
     AND NAME NOT LIKE '%小糊涂%'
     AND NAME NOT LIKE '%香飘飘%'
     AND NAME NOT LIKE '%维他%'
     AND NAME NOT LIKE '%唯怡%'
     AND NAME NOT LIKE '%旺旺%'
     AND NAME NOT LIKE '%王致和%'
     AND NAME NOT LIKE '%天喔%'
     AND NAME NOT LIKE '%王致和%'
     AND NAME NOT LIKE '%天地壹号%'
     AND NAME NOT LIKE '%天地1号%'
     AND NAME NOT LIKE '%特仑苏%'
     AND NAME NOT LIKE '%酸梅%'
    AND name NOT LIKE '%果橙%'
    AND name NOT LIKE '%果粒%'
    AND name NOT like '%苹果醋%'
    AND NAME NOT LIKE '乌龙茶%'
    AND NAME NOT LIKE '%洛神花茶%'
    AND NAME NOT LIKE '%三得利%'
    AND NAME NOT LIKE '%蜂蜜柚子茶%'
     AND NAME NOT LIKE '%鸡尾酒%'
     AND NAME NOT LIKE '%RIO%'
     AND NAME NOT LIKE '%锐澳%'
     AND NAME NOT LIKE '%贝塔%'
     AND NAME NOT LIKE '%切块%'
     AND NAME NOT LIKE '%切片%'
     AND NAME NOT LIKE '%锐欧%'
     AND NAME NOT LIKE '%荷兰乳%'
     AND NAME NOT LIKE '%美年达%'
     AND NAME NOT LIKE '%饮料%'
     AND NAME NOT LIKE '%橙汁%'
     AND NAME NOT LIKE '%VC果王%'
     AND NAME NOT LIKE '%万利来%'
     AND NAME NOT LIKE '%东北大板%'
    AND NAME NOT LIKE '%乐事%'
    AND NAME NOT like '%龙津%'
    AND NAME NOT LIKE '%龙井绿茶%'
    AND NAME NOT LIKE '%菠萝啤%'
   AND NAME NOT LIKE '%妙恋%'
   AND NAME NOT LIKE '%威龙%'
AND NAME NOT LIKE '%优乐美%'
AND NAME NOT LIKE '%圣牧%'
AND NAME NOT LIKE '%女儿红%'
AND NAME NOT LIKE '%孔府%'
AND  name NOT  like '%宝矿力水特%'
AND  name NOT  like '%李子园%'
AND  name NOT  like '%原叶%'
AND  name NOT  like '%麒麟茶饮料%'
AND  name NOT  like '%草本乐%'
AND  name NOT  like '%达利园%'
AND  name NOT  like '%今麦郎%'
AND  name NOT  like '%东鹏%'
AND  name NOT  like '%乐百氏%'
     ) f ON (r.id = f.restaurant_id)
LEFT OUTER JOIN
  (SELECT *
   FROM dm.dm_prd_portrait_restaurant_food
   WHERE dt='${day}') d ON (f.id = d.id);

 -- 计算每个餐厅的汇总数据, 并剔除部分不需要的品类

DROP TABLE IF EXISTS temp.temp_hotfood_rec_rst_sale_${dt};

CREATE TABLE temp.temp_hotfood_rec_rst_sale_${dt} AS
SELECT t1.restaurant_id,
       t1.7_quantity,
       t1.30_quantity,
       t1.food_num,
       t1.city_id,
       t1.geohash,
       coalesce(t2.order_num, 0) order_num,
       t1.gift_num
FROM
  (SELECT restaurant_id,
          count(*) food_num,
          max(city_id) city_id,
          max(geohash) geohash,
          sum(is_gift) gift_num,
          max(7_quantity) 7_quantity,
                          max(30_quantity) 30_quantity
   FROM temp.temp_hotfood_rec_rst_food_${dt}
   GROUP BY restaurant_id) t1
LEFT OUTER JOIN
  (SELECT restaurant_id,
          city_id,
          count(*) order_num
   FROM dw.dw_trd_order_wide_day
   WHERE dt >= get_date('${day}', -6)
     AND dt <= '${day}'
     AND order_status = 1
     AND city_id > 0
   GROUP BY restaurant_id,
            city_id) t2 ON (t1.restaurant_id=t2.restaurant_id)
LEFT OUTER JOIN
  (SELECT *
   FROM rec.rec_cls_restaurant_category_relation
   WHERE dt='3000-12-31'
     AND primary_category IN ('蔬菜',
                              '生鲜',
                              '鲜花',
                              '超市',
                              '水站',
                              '奶站',
                              '粮油',
                              '茶',
                              '药品')
    AND NAME LIKE '%次日达%') t3 ON (t1.restaurant_id=t3.restaurant_id)
WHERE t3.restaurant_id IS NULL ;

 -- 餐厅数据

DROP TABLE IF EXISTS temp.temp_hotfood_rec_rst_${dt};

CREATE TABLE temp.temp_hotfood_rec_rst_${dt} (
  restaurant_id bigint,
  7_quantity bigint,
  is_important int,
  is_special_category int,
  is_hot int, is_remote int
) partitioned BY (
  step string
);

 -- 有任意一个食物30天卖出两单的

INSERT overwrite TABLE temp.temp_hotfood_rec_rst_${dt} partition(step='p_food_hot')
SELECT restaurant_id,
       7_quantity,
       0,
       0,
       0,
       0
FROM temp.temp_hotfood_rec_rst_sale_${dt}
WHERE 30_quantity>=2;

 -- GKA 餐厅

INSERT overwrite TABLE temp.temp_hotfood_rec_rst_${dt} partition(step='p_important')
SELECT id,
       0,
       1,
       0,
       0,
       0
FROM dw.dw_prd_restaurant_wide
WHERE dt='${day}'
  AND is_valid=1
  AND bu_flag IN ('GKA',
                  'SIG');

 -- 高档餐厅

INSERT overwrite TABLE temp.temp_hotfood_rec_rst_${dt} partition(step='p_is_special')
SELECT restaurant_id,
       0,
       0,
       1,
       0,
       0
FROM rec.rec_cls_restaurant_category_relation
WHERE dt='3000-12-31'
  AND primary_category IN ('披萨意面',
                           '汉堡',
                           '日韩料理',
                           '西餐');

 -- 7天订单量>=5的所有餐厅都要，但有效食物少于5个的都要过滤掉-

INSERT overwrite TABLE temp.temp_hotfood_rec_rst_${dt} partition(step='p_order_hot')
SELECT restaurant_id,
       0,
       0,
       0,
       1,
       0
FROM temp.temp_hotfood_rec_rst_sale_${dt}
WHERE food_num>=5
  AND (order_num>=5
       OR city_id>=30);

 -- 对于方圆5公里之内(5位geohash近似）排名前40的餐厅都要，但有效食物少于5个的都要过滤掉

INSERT overwrite TABLE temp.temp_hotfood_rec_rst_${dt} partition(step='p_remote')
SELECT restaurant_id,
       0,
       0,
       0,
       0,
       1
FROM
  (SELECT restaurant_id,
          row_number() over(partition BY geohash
                            ORDER BY order_num DESC) rno
   FROM temp.temp_hotfood_rec_rst_sale_${dt}
   WHERE food_num>=5) r
WHERE r.rno<=40;

 -- 汇总满足前述条件的餐厅

INSERT overwrite TABLE temp.temp_hotfood_rec_rst_${dt} partition(step='result')
SELECT restaurant_id,
       0,
       0,
       0,
       0,
       0
FROM
  (SELECT restaurant_id,
          max(7_quantity) 7_quantity,
          max(is_important+is_special_category+ is_hot + is_remote) is_special
   FROM temp.temp_hotfood_rec_rst_${dt}
   WHERE step LIKE 'p_%'
   GROUP BY restaurant_id) t
WHERE (t.7_quantity>=3
       OR t.is_special>0);

 -- 合并time_period标签和功能标签

DROP TABLE IF EXISTS temp.temp_mdl_food_name_normalize;

CREATE TABLE temp.temp_mdl_food_name_normalize AS
SELECT t.food_id,
       t.food_name,
       t.normalize_food_name,
       t.price,
       t.last_update_time,
       if(t.tag_function='[]', NULL, t.tag_function) tag_function,
       t.tag_scene,
       t.category,
       t.flavor
FROM
  (SELECT food_id,
          food_name,
          normalize_food_name,
          price,
          last_update_time,
          tag_scene,
          category,
          flavor,
          regexp_replace(
            concat('[',
                CASE WHEN tag_function IS NOT NULL THEN concat(regexp_replace(tag_function,'\\[|\\]',''),',') ELSE concat('null_flag',',') END,
                CASE WHEN time_period IS NOT NULL THEN concat(regexp_replace(time_period,'\\[|\\]',''),',') ELSE concat('null_flag',',') END,
                CASE WHEN tag_holiday IS NOT NULL THEN concat(regexp_replace(tag_holiday,'\\[|\\]',''),',') ELSE concat('null_flag',',') END,
                CASE WHEN tag_season IS NOT NULL THEN concat(regexp_replace(tag_season,'\\[|\\]', '')) ELSE concat('null_flag') END,
              ']'),
            ',null_flag|null_flag,|null_flag',
            '') AS tag_function
   FROM dm.dm_mdl_food_name_normalize_day
WHERE dt='3000-12-31') t;

 -- 计算备选食物的所有标签，并针对每家餐厅normal_food_name相同的食物只取一个

DROP TABLE IF EXISTS temp.temp_hotfood_rec_normal_food_name_${dt};

CREATE TABLE temp.temp_hotfood_rec_normal_food_name_${dt} AS
SELECT t.restaurant_id,
       t.food_id,
       t.price,
       t.7_quantity,
       t.30_quantity,
       t.real_food_name,
       t.normal_food_name,
       t.tag_function,
       t.tag_scene,
       t.category,
       t.weight
FROM
  (SELECT b.restaurant_id,
          b.food_id,
          b.price,
          b.7_quantity,
          b.30_quantity,
          b.food_name real_food_name,
          c.normalize_food_name normal_food_name,
          c.tag_function,
          c.tag_scene,
          c.category,
          b.weight,
          d.category no_analysis_category,
          row_number() over (partition BY b.restaurant_id, coalesce(c.normalize_food_name,'unknown')
                             ORDER BY b.weight DESC) rno
   FROM
     (SELECT restaurant_id,
             food_id,
             price,
             7_quantity,
             30_quantity,
             food_name,
             is_gift,
             30_quantity+5*7_quantity+7*is_gift weight
             -- 30_quantity+5*7_quantity+100*is_gift+ (case when food_name like '%月饼%' then 100 else 0 end) weight
      FROM temp.temp_hotfood_rec_rst_food_${dt}) b
   LEFT OUTER JOIN temp.temp_mdl_food_name_normalize c ON (b.food_id=c.food_id)
   LEFT OUTER JOIN
     (SELECT *
      FROM rec.rec_cls_restaurant_category
      WHERE is_secondary=1
        AND need_food_analysis=0) d ON (c.normalize_food_name=d.category)) t
WHERE (t.rno=1
       OR t.normal_food_name IS NULL
       OR t.no_analysis_category IS NOT NULL);

 -- 获取食物推荐理由

DROP TABLE IF EXISTS temp.temp_hotfood_rec_reason_${dt};

CREATE TABLE temp.temp_hotfood_rec_reason_${dt} AS
SELECT food_id,
       concat('{', concat_ws(',', collect_set(concat('"', reason, '":{"score":', score, '}'))), '}') rec_reason
FROM
  (SELECT food_id,
          reason,
          max(score) score
   FROM rec.rec_hotfood_food_rec_reason
   WHERE dt='${day}'
   GROUP BY food_id,
            reason) A
GROUP BY food_id;

 -- 获取食物信息

DROP TABLE IF EXISTS temp.temp_hotfood_rec_base_${dt};

CREATE TABLE temp.temp_hotfood_rec_base_${dt} AS
SELECT b.restaurant_id,
     b.food_id,
     b.price,
     b.7_quantity,
     b.30_quantity,
     b.real_food_name,
     b.normal_food_name,
     b.tag_function,
     b.tag_scene,
     b.category,
     b.rno rno,
     d.cat0_name,
     d.cat1_name,
     if(b.rno<=12 AND (b.rno<=4 OR b.7_quantity>=20),1,0) is_use
     --case when (b.rno<=15 or (b.food_name like '%月饼%' and b.rno<=30))  and (b.rno<=4 or b.7_quantity>=20 or b.is_gift=1) then 1 else 0 end is_use
FROM
  (SELECT *
   FROM temp.temp_hotfood_rec_rst_${dt}
   WHERE step='result') a
INNER JOIN
  (SELECT *,
          row_number() over(partition BY restaurant_id
                            ORDER BY weight DESC) rno
   FROM temp.temp_hotfood_rec_normal_food_name_${dt}) b ON (a.restaurant_id=b.restaurant_id)
LEFT OUTER JOIN
  (SELECT restaurant_id,
          concat('[', concat_ws(',',collect_set(concat('\"',cat0_name,'\"'))), ']') AS cat0_name,
          concat('[', concat_ws(',',collect_set(concat('\"',cat1_name,'\"'))), ']') AS cat1_name
   FROM rec.rec_prf_restaurant_category_info
   WHERE dt='${day}'
   GROUP BY restaurant_id) d ON (b.restaurant_id=d.restaurant_id);

 -- 汇总信息
INSERT overwrite TABLE st.st_mdl_hotfood_rec_day partition (dt='${day}')
SELECT t1.restaurant_id,
       t1.food_id,
       t1.category,
       t1.price,
       t1.7_quantity,
       t1.30_quantity,
       t3.version_id,
       concat('{',
          concat('\"name\":\"', get_normal_word(t1.real_food_name,0), '\"'),
          concat(',\"tag_func\":', coalesce(t1.tag_function, '[]')),
          concat(',\"tag_scene\":', coalesce(t1.tag_scene, '[]')),
          concat(',\"normal_name\":\"', coalesce(t1.normal_food_name, ''), '\"'),
          concat(',\"7_salesrate\":', round(coalesce(t2.7_salesrate,0),4)),
          concat(',\"30_salesrate\":', round(coalesce(t2.30_salesrate,0),4)),
          concat(',\"order_7rate\":', round(coalesce(t2.order_7rate,0),4)),
          concat(',\"order_30rate\":', round(coalesce(t2.order_30rate,0),4)),
          concat(',\"best_rate\":', round(coalesce(t2.best_rate,0),4)),
          concat(',\"cat0_name\":', coalesce(t1.cat0_name, '[]')),
          concat(',\"cat1_name\":', coalesce(t1.cat1_name, '[]')),
          concat(',\"has_activity\":', coalesce(t4.has_activity, 0)),
          concat(',\"is_new\":', coalesce(t4.is_new, 0)),
          concat(',\"is_featured\":', coalesce(t4.is_featured, 0)),
          concat(',\"is_gum\":', coalesce(t4.is_gum, 0)),
          concat(',\"is_spicy\":', coalesce(t4.is_spicy, 0)),
          concat(',\"rec_reason\":', coalesce(t5.rec_reason, '{}')),
       '}') content
FROM
  (SELECT *
   FROM temp.temp_hotfood_rec_base_${dt}
   WHERE is_use=1) t1
LEFT OUTER JOIN
  (SELECT *
   FROM rec.rec_hotfood_food_feature_rate
   WHERE dt='${day}') t2 ON (t1.food_id=t2.food_id)
LEFT OUTER JOIN
  (SELECT *
   FROM dw.dw_prd_food
   WHERE dt ='${day}'
     AND is_valid=1) t4 ON (t1.food_id=t4.id)
LEFT OUTER JOIN
  temp.temp_hotfood_rec_reason_${dt} t5 ON (t1.food_id = t5.food_id)
JOIN
  (SELECT unix_timestamp() version_id) t3;

