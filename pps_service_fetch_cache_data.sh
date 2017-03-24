. /etc/profile
#!/bin/bash

#***************************************************************************************************
# ** 文件名称： pps_service_fetch_cache_data.sh
# ** 功能描述： 
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-02-22
#***************************************************************************************************

day=`date -d "1 days ago"  +%Y-%m-%d`
dt=`date +%Y%m%d --date='1 days ago'`
dt_3day=`date +%Y%m%d --date='3 days ago'`

cd /data/external_data/

rm pps_*

source_dir=/home/dt.rec/rec_ext/project/pps_writedata/data
user_dir=${source_dir}/user/${dt}
restaurant_dir=${source_dir}/restaurant/${dt}


user_meta_file=${user_dir}/usr_file_meta_list
restaurant_meta_file=${restaurant_dir}/rest_file_meta_list


/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:${user_meta_file} ./
expect "*password:"
send "dt.rec\n"
interact
expect eof
!


/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:${restaurant_meta_file} ./
expect "*password:"
send "dt.rec\n"
interact
expect eof
!


cat usr_file_meta_list | while read line
do
filename=`echo "${user_dir}/""${line}"`
/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:${filename} ./
expect "*password:"
send "dt.rec\n"
interact
expect eof
!
# tar -zxvf $line
# part_file=`echo "${line}" | awk 'gsub(".gz","") {print $0}'`
# rm ${part_file}.gz
done

cat rest_file_meta_list | while read line
do
filename=`echo "${restaurant_dir}/""${line}"`
/usr/local/bin/expect <<!
set timeout 30
spawn scp -P 2014 dt.rec@10.0.132.64:${filename} ./
expect "*password:"
send "dt.rec\n"
interact
expect eof
!
# tar -zxvf $line
# part_file=`echo "${line}" | awk 'gsub(".gz","") {print $0}'`
# rm ${part_file}.gz
done

rm usr_file_meta_list
rm rest_file_meta_list
