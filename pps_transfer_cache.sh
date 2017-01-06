. ~/.bash_profile

#***************************************************************************************************
# ** 文件名称： pps_transfer_cache.sh
# ** 功能描述： pps服务，转化数据格式，便于local cache写入
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-01-04
#***************************************************************************************************


cd /home/dt.rec/rec_ext/project/pps_writedata/transfer_cache

day=`date -d "1 days ago"  +%Y-%m-%d`
dt=`date +%Y%m%d --date='1 days ago'`

data_dir='/home/dt.rec/rec_ext/project/pps_writedata/transfer_cache/'
local_cache_raw_file='active_user_info_list_local'
local_cache_format_file='active_user_info_list.cache'
split_file_name='active_user_info_list.cache.part'
user_file_meta_list='user_cache_meta_file'

type_user=1
type_shop=2

cat ../active_user_info_list | head -1000000 > ${local_cache_raw_file}

java -cp InsertInfoToRedisCluster-1.0-SNAPSHOT-jar-with-dependencies.jar me.ele.dt.pps.kit.TransferFormatCacheFile ${local_cache_raw_file} ${local_cache_format_file} ${type_user}

rm ${split_file_name}_*
rm ${user_file_meta_list}
split ${local_cache_format_file} -l 100000 ${split_file_name}_

for file in ${split_file_name}_*;
do
    file_name=`echo "${file}"`
    tar -zcvf ${file_name}.gz ${file_name}
    echo "${file_name}.gz" >> ${user_file_meta_list}
done
