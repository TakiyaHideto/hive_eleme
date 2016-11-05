
#***************************************************************************************************
# ** 文件名称： st_mdl_hotfood_rec_day
# ** 功能描述：
# ** 创建者： yongliang.zhang
# ** 修改：jiahao.dong@ele.me
# ** 创建日期： 2016年06月20日
# ** 修改日期 修改人 修改内容
# **
#***************************************************************************************************


---- 计算所有有效餐厅的有效食物基本信息------
drop table temp.temp_hotfood_rec_rst_food_${dt};
create table temp.temp_hotfood_rec_rst_food_${dt} as
select f.restaurant_id, f.id food_id, f.price, f.name food_name, coalesce(d.7_quantity, 0) 7_quantity, coalesce(d.30_quantity, 0) 30_quantity,
substring(geohash_of_latlng(r.latitude, r.longitude), 1, 5) geohash, r.city_id,
case when f.name like '%月饼%' or f.name like '%巧克力%' or f.name like '%德芙%' or f.name like '%金帝%'
or f.name like '%费列罗%' then 1 else 0 end is_gift
from (
select * from dw.dw_prd_restaurant where dt='${day}' and is_valid=1) r
join (
select * from dw.dw_prd_food where dt='${day}' and is_valid=1 and length(image_hash)>10
and restaurant_id <> 249330 and price>=5 and price<=500
and name not like '%可乐%' and name not like '%王老吉%' and name not like '%雪碧%'
and name not like '%加多宝%' and name not like '%矿泉水%' and name not like '%纯净水%'
and name not like '%康师傅%' and name not like '%饮用水%' and name not like '%汽水%'
and name not like '%酒水%' and name not like '%红牛%' and name not like '%雀巢%'
and name not like '%上好佳%' and name not like '%旺仔%' and name not like '%统一%'
and name not like '%百岁山%' and name not like '%农夫山泉%'
 ) f
on (r.id=f.restaurant_id)
left outer join (
select * from dm.dm_prd_portrait_restaurant_food where dt='${day}'
 ) d
on (f.id=d.id);

---- 计算每个餐厅的汇总数据, 并剔除部分不需要的品类----------
drop table temp.temp_hotfood_rec_rst_sale_${dt};
create table temp.temp_hotfood_rec_rst_sale_${dt} as
select t1.restaurant_id, t1.7_quantity, t1.30_quantity, t1.food_num, t1.city_id, t1.geohash, coalesce(t2.order_num, 0) order_num, t1.gift_num from
(
   select restaurant_id, max(7_quantity) 7_quantity, max(30_quantity) 30_quantity, count(*) food_num, max(city_id) city_id,
      max(geohash) geohash,  sum(is_gift) gift_num
   from temp.temp_hotfood_rec_rst_food_${dt}
   group by restaurant_id
) t1 left outer join
 (
    select restaurant_id, city_id, count(*) order_num
    from dw.dw_trd_order_wide_day
    where dt>=get_date('${day}', -6) and dt<='${day}' and order_status=1 and city_id>0
    group by restaurant_id, city_id
 ) t2 on (t1.restaurant_id=t2.restaurant_id) left outer join
 (
    select * from rec.rec_cls_restaurant_category_relation
    where dt='3000-12-31' and primary_category in
        ('蔬菜'
        ,'生鲜'
        ,'鲜花'
        ,'超市'
        ,'水站','奶站', '粮油','茶', '药品'
       #,'营养品'
       #,'水果'
        )
 ) t3 on (t1.restaurant_id=t3.restaurant_id)
 where t3.restaurant_id is null ;

 drop table temp.temp_hotfood_rec_rst_${dt};
 create table temp.temp_hotfood_rec_rst_${dt}
 (
 restaurant_id bigint,
 7_quantity bigint,
 is_important int,
 is_special_category int,
 is_hot int,
 is_remote int
 ) partitioned by (step string);


 -----有任意一个食物30天卖出两单的------
 insert overwrite table temp.temp_hotfood_rec_rst_${dt} partition(step='p_food_hot')
 select restaurant_id, 7_quantity, 0, 0, 0, 0 from temp.temp_hotfood_rec_rst_sale_${dt} where 30_quantity>=2;

 ---- GKA 餐厅-----
 insert overwrite table temp.temp_hotfood_rec_rst_${dt} partition(step='p_important')
 select id, 0, 1, 0, 0, 0
 from dw.dw_prd_restaurant_wide
 where dt='${day}' and is_valid=1 and bu_flag in ('GKA', 'SIG');

 ---- 高档餐厅-----
 insert overwrite table temp.temp_hotfood_rec_rst_${dt} partition(step='p_is_special')
 select restaurant_id, 0, 0, 1, 0, 0 from rec.rec_cls_restaurant_category_relation where dt='3000-12-31'
 and primary_category in ('披萨意面', '汉堡', '日韩料理', '西餐');

-- #-----动销且销量占城市前50%的，或者7天订单量大于10的，有效食物少于5个的都要过滤掉---
-- #insert overwrite table temp.temp_hotfood_rec_rst_${dt} partition(step='p_order_hot')
-- #select t1.restaurant_id, 0, 0, 0, 1, 0 from
-- #(
-- #select restaurant_id, city_id, order_num, food_num, gift_num from temp.temp_hotfood_rec_rst_sale_${dt}
-- #where (order_num>=1 and food_num>=5)
-- # or (gift_num>=4)
-- #) t1
-- #join
-- #(
-- #select city_id, percentile_approx(order_num, 0.5) top50 from temp.temp_hotfood_rec_rst_sale_${dt}
-- #group by city_id
-- #) t2 on (t1.city_id=t2.city_id)
-- #where t1.order_num>=10 or t1.order_num>t2.top50 or t1.gift_num>=4
-- #;

 -----7天订单量>=5的所有餐厅都要，但有效食物少于5个的都要过滤掉---
 insert overwrite table temp.temp_hotfood_rec_rst_${dt} partition(step='p_order_hot')
 select restaurant_id, 0, 0, 0, 1, 0 from temp.temp_hotfood_rec_rst_sale_${dt}
 where food_num>=5  and (order_num>=5 or city_id>=30);


 -----对于方圆5公里之内(5位geohash近似）排名前40的餐厅都要，但有效食物少于5个的都要过滤掉---
 insert overwrite table temp.temp_hotfood_rec_rst_${dt} partition(step='p_remote')
 select restaurant_id, 0, 0, 0, 0, 1 from
 (
    select restaurant_id, row_number() over(partition by geohash order by order_num desc) rno
    from temp.temp_hotfood_rec_rst_sale_${dt} where food_num>=5
 ) r
 where r.rno<=40;


 -----汇总满足前述条件的餐厅------
 insert overwrite table temp.temp_hotfood_rec_rst_${dt} partition(step='result')
 select restaurant_id, 0, 0, 0, 0, 0 from
 (
 select restaurant_id, max(7_quantity) 7_quantity, max(is_important+is_special_category+ is_hot + is_remote) is_special
 from temp.temp_hotfood_rec_rst_${dt} where step like 'p_%'
 group by restaurant_id
 ) t
 where (t.7_quantity>=3 or t.is_special>0);


 -----合并time_period标签和功能标签-----
 drop table temp.temp_mdl_food_name_normalize;
 create table temp.temp_mdl_food_name_normalize as
 select t.food_id, t.food_name, t.normalize_food_name, t.price, t.last_update_time, case when t.tag_function='[]' then null else t.tag_function end as tag_function, t.tag_scene, t.category, t.flavor
 from(
 select food_id, food_name, normalize_food_name, price, last_update_time,
 regexp_replace(concat('[',
 case when tag_function is not null then concat(regexp_replace(tag_function,"\\[|\\]",""),',') else concat('null_flag',',') end,
 case when time_period is not null then concat(regexp_replace(time_period,"\\[|\\]",""),',') else concat('null_flag',',') end,
 case when tag_holiday is not null then concat(regexp_replace(tag_holiday,"\\[|\\]",""),',') else concat('null_flag',',') end,
 case when tag_season is not null then concat(regexp_replace(tag_season,"\\[|\\]", "")) else concat('null_flag') end, ']'),",null_flag|null_flag,|null_flag","") as tag_function,
 tag_scene, category, flavor
 from dm.dm_mdl_food_name_normalize_day
 where dt='3000-12-31'
 ) t;


 --- 计算备选食物的所有标签，并针对每家餐厅normal_food_name相同的食物只取一个------
 drop table temp.temp_hotfood_rec_normal_food_name_${dt};
 create table temp.temp_hotfood_rec_normal_food_name_${dt} as
 select t.restaurant_id, t.food_id, t.price, t.7_quantity, t.30_quantity, t.real_food_name,
      t.normal_food_name, t.tag_function, t.tag_scene, t.category, t.weight
 from
 (
    select b.restaurant_id, b.food_id, b.price, b.7_quantity, b.30_quantity, b.food_name real_food_name,
           c.normalize_food_name normal_food_name, c.tag_function, c.tag_scene, c.category, b.weight, d.category no_analysis_category,
           row_number() over (partition by b.restaurant_id, coalesce(c.normalize_food_name,'unknown') order by b.weight desc) rno
    from
    (
      select restaurant_id, food_id, price, 7_quantity,30_quantity,food_name, is_gift
           # ,30_quantity+5*7_quantity+100*is_gift+ (case when food_name like '%月饼%' then 100 else 0 end) weight
             ,30_quantity+5*7_quantity+7*is_gift weight
      from temp.temp_hotfood_rec_rst_food_${dt}
    ) b left outer join
    (
        select * from temp.temp_mdl_food_name_normalize
    ) c on (b.food_id=c.food_id) left outer join
    (
        select * from rec.rec_cls_restaurant_category where is_secondary=1 and need_food_analysis=0
    ) d on (c.normalize_food_name=d.category)
 ) t
 where (t.rno=1 or t.normal_food_name is null or t.no_analysis_category is not null);



 -----获取食物信息-----
 drop table temp.temp_hotfood_rec_base_${dt};
 create table temp.temp_hotfood_rec_base_${dt} as
 select b.restaurant_id, b.food_id, b.price, b.7_quantity, b.30_quantity, b.real_food_name, b.normal_food_name,
        b.tag_function, b.tag_scene, b.category, b.rno rno, d.cat0_name, d.cat1_name
 #--下面这段代码仅在特殊节日时启用给礼物提权---
 #, case when (b.rno<=15 or (b.food_name like '%月饼%' and b.rno<=30))  and (b.rno<=4 or b.7_quantity>=20 or b.is_gift=1) then 1 else 0 end is_use
 #-- 下面这段代码平时非节假日时启用---
 , case when b.rno<=12 and (b.rno<=4 or b.7_quantity>=20) then 1 else 0 end is_use
 from
 (
    select * from temp.temp_hotfood_rec_rst_${dt} where step='result'
 ) a join
 (
    select *, row_number() over(partition by restaurant_id order by weight desc) rno
    from temp.temp_hotfood_rec_normal_food_name_${dt}
 ) b on (a.restaurant_id=b.restaurant_id)
 left outer join (
     select restaurant_id,
         concat('[', concat_ws(',',collect_set(concat('\"',cat0_name,'\"'))), ']') as cat0_name,
         concat('[', concat_ws(',',collect_set(concat('\"',cat1_name,'\"'))), ']') as cat1_name
     from rec.rec_prf_restaurant_category_info
     where dt='${day}'
     group by restaurant_id
 ) d on (b.restaurant_id=d.restaurant_id);


insert overwrite table st.st_mdl_hotfood_rec_day partition (dt='${day}')
select t1.restaurant_id, t1.food_id, t1.category,t1.price, t1.7_quantity, t1.30_quantity, t3.version_id,
concat('{\"name\":\"', get_normal_word(t1.real_food_name,0), '\"',
case when t1.tag_function is not null then concat(',\"tag_func\":', t1.tag_function) else '' end,
case when t1.tag_scene is not null then concat(',\"tag_scene\":', t1.tag_scene) else '' end,
case when t1.normal_food_name is not null then concat(',\"normal_name\":\"', t1.normal_food_name, '\"') else '' end,
concat(',\"7_salesrate\":', round(coalesce(t2.7_salesrate,0),4)),
concat(',\"30_salesrate\":', round(coalesce(t2.30_salesrate,0),4)),
concat(',\"order_7rate\":', round(coalesce(t2.order_7rate,0),4)),
concat(',\"order_30rate\":', round(coalesce(t2.order_30rate,0),4)),
concat(',\"best_rate\":', round(coalesce(t2.best_rate,0),4)),
concat(',\"cat0_name\":', t1.cat0_name),
concat(',\"cat1_name\":', t1.cat1_name),
concat(',\"has_activity\":', coalesce(t4.has_activity, 0)),
concat(',\"is_new\":', coalesce(t4.is_new, 0)),
concat(',\"is_featured\":', coalesce(t4.is_featured, 0)),
concat(',\"is_gum\":', coalesce(t4.is_gum, 0)),
concat(',\"is_spicy\":', coalesce(t4.is_spicy, 0)),
'}') content
from
(
select * from temp.temp_hotfood_rec_base_${dt} where is_use=1
) t1
left outer join
(select * from rec.rec_hotfood_food_feature_rate where dt='${day}') t2
on t1.food_id=t2.food_id
left outer join
(SELECT * from dw.dw_prd_food
     where dt ='${day}' AND is_valid=1) t4
on t1.food_id=t4.id
join
(select unix_timestamp() version_id) t3;
