#***************************************************************************************************
# ** 文件名称： hive_pad.sh
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-15
#***************************************************************************************************



output_file1='/tmp/shop_match/eleme_rst'
output_file2='/tmp/shop_match/baidu_rst'
output_file3='/tmp/shop_match/meituan_rst'
output_file4='/tmp/shop_match/dianping_rst'

day_index=1
dt=`date -d -${day_index}day +%Y%m%d`
day=`date -d -${day_index}day +%Y-%m-%d`

echo dt
echo $day

hive -e "
select a.restaurant_id, regexp_replace(name,'\t',' '), city_name, regexp_replace(address_text,'\t',' '), b.phone, latitude, longitude
from(
select id as restaurant_id, name, city_name, address_text, phone, latitude, longitude
from dw.dw_prd_restaurant_wide
where dt='${day}' and is_valid=1
) a
left outer join(
select restaurant_id, phone
from(
select restaurant_id, phone, row_number() over (partition by restaurant_id order by dt desc) rno
from ods.ods_eleme_order_sharding
where dt>get_date('${day}',-31) and phone is not null
) t
where rno=1
) b
on(a.restaurant_id=b.restaurant_id)
;" > ${output_file1}


hive -e "
select id, regexp_replace(name,'\t',' '), city_name, regexp_replace(address,'\t',' '), shop_phone, eleme_lat, eleme_lng
from dw.dw_ext_baidu_restaurant
where dt='${day}'
;" > ${output_file2}



hive -e "
select id, regexp_replace(name,'\t',' '), city_name, regexp_replace(address,'\t',' '), call_center, gaode_lat, gaode_lng
from dw.dw_ext_meituan_restaurant
where dt='${day}'
;" > ${output_file3}


hive -e "
select id, regexp_replace(name,'\t',' '), cityname, regexp_replace(address,'\t',' '), telphone, lat, lng
from dw.dw_ext_meituan_tuangou_restaurant
where dt='${day}'
;" > ${output_file4}




