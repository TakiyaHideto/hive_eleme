#***************************************************************************************************
# ** 文件名称： send_sms(hotfood_data_coverage).sh
# ** 功能描述： 用于检测数据覆盖及内容是否正常
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-09-18
#***************************************************************************************************

#!/bin/bash
. /etc/profile
set -e 
day=`date -d @$1 +%Y-%m-%d`
#定义短信接收人
receiver='18611420716'
resultFile=/home/master/workspace/data/mail/sms_hotfood_data_coverage_${day} 

#检测dm_mdl_food_name_normalize_day表，有正则化名称的食物的覆盖率
sql="select sum(case when normalize_food_name is not null and normalize_food_name!='' then 1 else 0 end)/count(*) from dm.dm_mdl_food_name_normalize_day where dt='${day}';"
echo "sql is : $sql"
echo "有正则化名称的食物的覆盖率:">>$resultFile
hive -e "$sql">>$resultFile
echo -e "\r\n">>$resultFile

#检测dm_ups_user_item_info,food偏好用户覆盖率
sql="select count(distinct user_id),count(*) \
from dm.dm_ups_user_item_info \
where dt='${day}' and flag='rec_food';"
echo "sql is : $sql"
echo "用户画像食品偏好用户覆盖:">>$resultFile
hive -e "$sql">>$resultFile
echo -e "\r\n">>$resultFile

#检测dm_ups_user_item_info,category偏好用户覆盖率
sql="select count(distinct user_id),count(*) \
from dm.dm_ups_user_item_info \
where dt='${day}' and flag='rec_category';"
echo "sql is : $sql"
echo "用户画像食品大类用户覆盖:">>$resultFile
hive -e "$sql">>$resultFile
echo -e "\r\n">>$resultFile

#检测dm_ups_user_item_info,flavor偏好用户覆盖率
sql="select count(distinct user_id),count(*) \
from dm.dm_ups_user_item_info \
where dt='${day}' and flag='rec_flavor';"
echo "sql is : $sql"
echo "用户画像食品口味偏好用户覆盖:">>$resultFile
hive -e "$sql">>$resultFile
echo -e "\r\n">>$resultFile

#检测dm_ups_user_item_info,style偏好用户覆盖率
sql="select count(distinct user_id),count(*) \
from dm.dm_ups_user_item_info \
where dt='${day}' and flag='rec_style';"
echo "sql is : $sql"
echo "用户画像食品风格偏好用户覆盖:">>$resultFile
hive -e "$sql">>$resultFile
echo -e "\r\n">>$resultFile

res1=`cat $resultFile`
echo "$res1"
/home/master/platform/env/bin/Psms $receiver $res1
rm $resultFile

# #检测dm_ups_food_item_info表，分区tag，食品对应tag的覆盖率
# select sum(case when attr_value is not null and attr_value!='' then 1 else 0 end)/count(*) from dm.dm_ups_food_item_info where dt='2016-09-17' and flag='tag' and attr_key='normalize_name';
# select sum(case when attr_value is not null and attr_value!='' then 1 else 0 end)/count(*) from dm.dm_ups_food_item_info where dt='2016-09-17' and flag='tag' and attr_key='category_fine';
# select sum(case when attr_value is not null and attr_value!='' then 1 else 0 end)/count(*) from dm.dm_ups_food_item_info where dt='2016-09-17' and flag='tag' and attr_key='category_coarse';
# select sum(case when attr_value is not null and attr_value!='' then 1 else 0 end)/count(*) from dm.dm_ups_food_item_info where dt='2016-09-17' and flag='tag' and attr_key='flavor';
# select sum(case when attr_value is not null and attr_value!='' then 1 else 0 end)/count(*) from dm.dm_ups_food_item_info where dt='2016-09-17' and flag='tag' and attr_key='cooking_method';
# select sum(case when attr_value is not null and attr_value!='' then 1 else 0 end)/count(*) from dm.dm_ups_food_item_info where dt='2016-09-17' and flag='tag' and attr_key='tag_function';
# select sum(case when attr_value is not null and attr_value!='' then 1 else 0 end)/count(*) from dm.dm_ups_food_item_info where dt='2016-09-17' and flag='tag' and attr_key='tag_scene';



# #!/bin/bash
# . /etc/profile
# set -e 
# day=`date -d @$1 +%Y-%m-%d`
# #定义短信接收人
# receiver='18717926451;18701966791;13162521958;15021571938;18917994730;15606812861;15601801259;13816980246;13621747058'
# #receiver='18717926451'
# resultFile=/home/master/workspace/data/mail/sms_hotfood_kpi_${day}
# #rm $resultFile

# sql="select concat(log_date, '日猜你喜欢/热卖美食首页入口UV',cast(t.our_uv as string), '万, 占', cast(t.our_uv_rate as string), \
# '%, 下单用户', cast(t.our_buyer_num as string), '万, 订单', cast(t.our_order_num as string), \
# '万, 占APP总单量', cast(t.our_order_rate as string), '%, 转化率', cast(t.our_rate as string), '%。 加上发现页总体UV', cast(t.total_uv as string), '万, 占', cast(t.total_uv_rate as string), \
# '%, 下单用户', cast(t.total_buyer_num as string), '万, 订单', cast(t.total_order_num as string), \
# '万, 占APP总单量', cast(t.total_order_rate as string), '%, 总体转化率', cast(t.total_rate as string), '%') from \
# (\
# select log_date, max(case when title='热卖系列' then round(uv/10000,2) else null end) our_uv, \
# max(case when title='热卖系列' then round(100*uv/app_uv,2) else null end) our_uv_rate, \
# max(case when title='热卖系列' then round(buyer_num/10000,2) else null end) our_buyer_num,\
# max(case when title='热卖系列' then round(order_num/10000,2) else null end) our_order_num,\
# max(case when title='热卖系列' then round(100*order_num/total_order_num,2) else null end) our_order_rate,\
# max(case when title='热卖系列' then round(100*buyer_num/uv,2) else null end) our_rate,\
# max(case when title='热卖系列(含发现页)' then round(uv/10000,2) else null end) total_uv,\
# max(case when title='热卖系列(含发现页)' then round(100*uv/app_uv,2) else null end) total_uv_rate, \
# max(case when title='热卖系列(含发现页)' then round(buyer_num/10000,2) else null end) total_buyer_num,\
# max(case when title='热卖系列(含发现页)' then round(order_num/10000,2) else null end) total_order_num,\
# max(case when title='热卖系列(含发现页)' then round(100*order_num/total_order_num,2) else null end) total_order_rate,\
# max(case when title='热卖系列(含发现页)' then round(100*buyer_num/uv,2) else null end) total_rate \
# from st.st_log_hotfood_kpi_day_inc where dt='${day}' group by log_date \
# ) t"
# echo "sql is : $sql"
# hive -e "$sql">$resultFile
# res1=`cat $resultFile`
# echo "$res1"
# /home/master/platform/env/bin/Psms $receiver $res1