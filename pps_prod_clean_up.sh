. ~/.bash_profile

#***************************************************************************************************
# ** 文件名称： pps_prod_clean_up.sh
# ** 功能描述： pps服务, 数据生产机器上清理工作
# ** 创建者： jiahao.dong
# ** 创建日期： 2017-02-20
#***************************************************************************************************

day=`date -d "1 days ago"  +%Y-%m-%d`
dt=`date +%Y%m%d --date='1 days ago'`
dt_2ago=`date +%Y%m%d --date='2 days ago'`
day=`date -d @$1 +%Y-%m-%d`
dt=`date -d @$1 +%Y%m%d`
hour=`date -d @$1 +%H`

# prod user 当前数据
parent_data_dir=/home/dt.rec/rec_ext/project/pps_writedata/data/user
data_dir=/home/dt.rec/rec_ext/project/pps_writedata/data/user/${dt}
user_info_file=pps_user_info_data


rm ${data_dir}/${user_info_file}
rm -rf ${parent_data_dir}/${dt_2ago}

# prod restaurant 当前数据
parent_data_dir=/home/dt.rec/rec_ext/project/pps_writedata/data/restaurant
data_dir=/home/dt.rec/rec_ext/project/pps_writedata/data/restaurant/${dt}
rest_info_file=pps_restaurant_info_data

rm ${data_dir}/${rest_info_file}
rm -rf ${parent_data_dir}/${dt_2ago}


# # prod user 旧版本生成任务的user数据
# cd /home/dt.rec/rec_ext/project/pps_writedata/transfer_cache
# rm active_user_info_list*