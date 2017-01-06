. /etc/profile
#!/bin/bash

#***************************************************************************************************
# ** 文件名称： pps_cache_fetch.sh
# ** 功能描述： pps服务，向redis写活跃用户数据
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-01-04
#***************************************************************************************************




source_dir='/home/dt.rec/rec_ext/project/pps_writedata/transfer_cache/'

/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:/home/dt.rec/rec_ext/project/pps_writedata/transfer_cache/user_cache_meta_file ./
expect "*password:"
send "dt.rec\n"
interact
expect eof
!

cat user_cache_meta_file | while read line
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
cat $part_file_name >> /data/redis_backup/active_user_info_list.cache
rm active_user_info_list.cache.part*
done

rm active_user_info_list.cache.part*