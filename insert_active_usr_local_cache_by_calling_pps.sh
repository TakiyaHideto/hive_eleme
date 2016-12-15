. /etc/profile

#***************************************************************************************************
# ** 文件名称： insert_active_usr_local_cache_by_calling_pps.sh
# ** 功能描述： 抽取前一天活跃用户列表，调用pps，间接导入redis活跃用户
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-12-06
#***************************************************************************************************

######################定义相关变量######################
#input_file='/home/master/workspace/data/mail/active_user_list'
input_file='/home/jiahao.dong/temp/active_user_list'
okay_file='/home/jiahao.dong/temp/done.ok'
file_name='/home/jiahao.dong/temp/id_list'

day=`date -d "1 days ago"  +%Y-%m-%d`
echo $day

start_time=`date`

# number 1
# readonly service_url="http://localhost:8000/rpc"
# number 2
service_url="http://10.0.46.126:8000/rpc"
# number 3
#readonly service_url="http://10.0.49.78:8000/rpc"
# number 4
#readonly service_url="http://10.0.51.140:8000/rpc"





######################定义函数######################
function print_help()
{
    echo ""
    echo "==============================================================="
    echo "--> type_id:"
    echo "type_id=1: 更新typeid, appid tag信息, Cassandra数据表的schema"
    echo "type_id=2: reset parameter"
    echo "type_id=3: 关闭本地缓存服务"
    echo "type_id=4: 开启本地缓存服务"
    echo "type_id=5: 关闭缓存服务"
    echo "type_id=6: 开启缓存服务"
    echo ""
}

function admin()
{
    if [ $# -ne 2 ]
    then
        echo "--> admin [host] [type_id]"
        return 1
    fi

    local host=$1
    local type_id=$2
    curl -s -XPOST "http://${host}:8000/rpc" -d "{
    \"ver\":\"1.0\",
    \"soa\":{\"rpc\":\"dt.pps_service|1\",\"req\":\"uuid\"},
    \"iface\":\"me.ele.dt.soa.api.PersonalProfileService\",
    \"method\":\"admin\",
    \"args\":{\"type_id\":\"${type_id}\",\"password\":\"\\\"pps_service@dt.rec\\\"\"},
    \"metas\":null}"
    echo ""
}

function cache_switch()
{
    readonly -a host_list=("10.0.48.105" "10.0.46.126" "10.0.49.78" "10.0.51.140")
    ##readonly -a host_list=("10.0.48.126")
    for host in ${host_list[@]}
    do
        echo "--> ${host}" && sleep 2
        admin ${host} $1
    done
}


function start_loop
{
cat $1 | while read line
do
curl -s -XPOST "$2" -d "{ \"ver\":\"1.0\", \"soa\":{ \"rpc\":\"unknown|1.1\", \"req\":\"unknown^^4517025278746576119|1477986805711\" },\"ifa
ce\":\"me.ele.dt.soa.api.PersonalProfileService\", \"method\":\"getTag\", \"args\":{ \"query\":\"{\\\"appId\\\":\\\"dt.import_data_redis\\\",\\\"token\\
\":\\\"696ae84730\\\",\\\"typeId\\\":1,\\\"id\\\":${line},\\\"tagNameList\\\":[\\\"category_prefer\\\",\\\"consume_level\\\",\\\"create_time\\\",\\\"del
ivery_priority\\\"]}\" }, \"metas\":null }"  >/dev/null 2>/dev/null &
done
}


hive -e "
    select 
    user_id
    from(
        select
            user_id, 
            count(*) as cnt
        from
            dw.dw_log_app_pv_day_inc
        where
            dt>=get_date('${day}',-5) and
            user_id is not null
        group by 
            user_id
        sort by 
            cnt desc
    ) t
    limit 150000
;" > ${input_file}


cache_switch 5

cat ${input_file} | head -150000 | split ${input_file} -l 75000 ${file_name}_
# cp ${input_file} ${file_name}_

echo "file splited" >&2
write_redis_time=`date`

for file in ${file_name}_* ;
do
start_loop ${file} ${service_url} &
done
wait

cache_switch 6


cache_switch 3
write_local_time=`date`
# number 1
service_url="http://10.0.48.105:8000/rpc"
for file in ${file_name}_* ;
do
start_loop ${file} ${service_url} &
done
wait

# number 3
service_url="http://10.0.49.78:8000/rpc"
for file in ${file_name}_* ;
do
start_loop ${file} ${service_url} &
done
wait

# number 4
service_url="http://10.0.51.140:8000/rpc"
for file in ${file_name}_* ;
do
start_loop ${file} ${service_url} &
done
wait
cache_switch 4


# rm ${input_file}
rm ${file_name}*


touch ${okay_file}
stop_time=`date`

echo $start_time >> $okay_file
echo $write_redis_time >> $okay_file
echo $write_local_time >> $okay_file
echo $stop_time >> $okay_file
echo "-----------------------" >> $okay_file


