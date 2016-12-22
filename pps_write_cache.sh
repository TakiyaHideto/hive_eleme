. /etc/profile
#!/bin/bash

#***************************************************************************************************
# ** 文件名称： pps_write_cache.sh
# ** 功能描述： pps服务，向redis写活跃用户数据
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-20
#***************************************************************************************************


log_file='/home/dev/pps_write_redis_cache/log'
echo "-----------------------" >> ${log_file}
echo `date` >> ${log_file}

cp /home/dev/pps_write_redis_cache/active_user_info_list /home/dev/pps_write_redis_cache/active_user_info_list.bk

rm /home/dev/pps_write_redis_cache/active_user_info_list

/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:/home/dt.rec/rec_ext/project/pps_writedata/active_user_info_list /home/dev/pps_write_redis_cache/
expect "*password:"
send "dt.rec\n"
expect "*Are you sure you want to continue connecting*"
send "yes\n"
interact
expect eof
!

cat /home/dev/pps_write_redis_cache/active_user_info_list | head -1000 > /home/dev/pps_write_redis_cache/temp

java -cp /home/dev/pps_write_redis_cache/InsertInfoToRedisCluster-1.0-SNAPSHOT-jar-with-dependencies.jar me.ele.dt.pps.writer.InsertRedisInfo /home/dev/pps_write_redis_cache/active_user_info_list >> ${log_file}

echo `date` >> ${log_file}
echo -e "-----------------------\n\n" >> ${log_file}

