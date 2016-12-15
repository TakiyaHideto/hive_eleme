


#***************************************************************************************************
# ** 文件名称： insert_active_usr_redis_by_calling_pps.sh
# ** 功能描述： 抽取前一天活跃用户列表，调用pps，间接导入redis活跃用户
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-06
#***************************************************************************************************

input_file='/home/master/workspace/data/mail/active_user_list'
# input_file='/home/jiahao.dong/active_user_list'

day=`date -d "1 days ago"  +%Y-%m-%d`
echo $day

hive -e "
    select
        distinct user_id
    from
        dw.dw_log_app_page_event_hour_inc
    where
        dt='${day}' and
        user_id is not null and
        user_id != ''
;" > ${input_file}



# readonly service_url="http://localhost:8000/rpc"
# number 2
readonly service_url="http://10.0.46.126:8000/rpc"
# number 3
#readonly service_url="http://10.0.49.78:8000/rpc"
# number 4
#readonly service_url="http://10.0.51.140:8000/rpc"


function start_loop
{
cat $1 | while read line
do

curl -s -XPOST "${service_url}" -d "{ \"ver\":\"1.0\", \"soa\":{ \"rpc\":\"unknown|1.1\", \"req\":\"unknown^^4517025278746576119|1477986805711\" },\"iface\":\"me.ele.dt.soa.api.PersonalProfileService\", \"method\":\"getTag\", \"args\":{ \"query\":\"{\\\"appId\\\":\\\"dt.import_data_redis\\\",\\\"token\\\":\\\"696ae84730\\\",\\\"typeId\\\":1,\\\"id\\\":${line},\\\"tagNameList\\\":[\\\"category_prefer\\\",\\\"consume_level\\\",\\\"create_time\\\",\\\"delivery_priority\\\"]}\" }, \"metas\":null }"  >/dev/null 2>/dev/null &

sleep 0.001
done
}

file_name=id_list
split ${input_file} -l 4000000 ${file_name}_
# cp ${input_file} ${file_name}_

echo "file splited" >&2

for file in ${file_name}_* ;
do

start_loop ${file} &

done
wait


rm ${input_file}

