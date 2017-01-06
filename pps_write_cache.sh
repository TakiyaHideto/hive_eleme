. /etc/profile
#!/bin/bash

#***************************************************************************************************
# ** 文件名称： pps_write_cache.sh
# ** 功能描述： pps服务，向redis写活跃用户数据
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-20
#***************************************************************************************************

source_dir='/home/dt.rec/rec_ext/project/pps_writedata/'
source_user_file_meta_list='/home/dt.rec/rec_ext/project/pps_writedata/user_file_meta_list'
source_data_prefix_gz='/home/dt.rec/rec_ext/project/pps_writedata/active_user_info_list'

local_dir='/home/dev/pps_write_redis_cache'
local_jar_file='/home/dev/pps_write_redis_cache/InsertInfoToRedisCluster-1.0-SNAPSHOT-jar-with-dependencies.jar'
local_user_file_meta_list='/home/dev/pps_write_redis_cache/user_file_meta_list'

log_file='/home/dev/pps_write_redis_cache/log'

echo "-----------------------" >> ${log_file}
echo `date` >> ${log_file}

cd /home/dev/pps_write_redis_cache

cp /data/redis_backup/active_user_info_list /data/redis_backup/active_user_info_list.bk
rm /data/redis_backup/active_user_info_list

/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:/home/dt.rec/rec_ext/project/pps_writedata/user_file_meta_list ./
expect "*password:"
send "dt.rec\n"
interact
expect eof
!

cat user_file_meta_list | while read line
do
filename=`echo "$source_dir""$line"`
echo "${filename}"
/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:$filename ./
expect "*password:"
send "dt.rec\n"
interact
expect eof
!
tar -zxvf $line
part_file_name=`echo "${line}" | awk 'gsub(".gz","") {print $0}'`
cat $part_file_name >> /data/redis_backup/active_user_info_list
rm active_user_info_list_part_*
done

rm active_user_info_list_part_*

cat /data/redis_backup/active_user_info_list | head -1000 > /home/dev/pps_write_redis_cache/temp

java -cp /home/dev/pps_write_redis_cache/InsertInfoToRedisCluster-1.0-SNAPSHOT-jar-with-dependencies.jar me.ele.dt.pps.writer.RedisCacheWritingAllUsers /data/redis_backup/active_user_info_list >> ${log_file}

java -cp InsertInfoToRedisCluster-1.0-SNAPSHOT-jar-with-dependencies.jar me.ele.dt.pps.kit.TransferFormatCacheFile /data/redis_backup/active_user_info_list /data/redis_backup/active_user_info_list.cache >> ${log_file}

echo `date` >> ${log_file}
echo -e "-----------------------\n\n" >> ${log_file}

