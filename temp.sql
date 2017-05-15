#***************************************************************************************************
# ** 文件名称： dm_ups_user_info_h2es_alpha_create.sql
# ** 功能描述： 创建用户画像es链接表 alpha
#
# ** 创建者：jiahao.dong@ele.me
# ** 创建日期： 2017-03-21
# **
# ** ChangeLog
#***************************************************************************************************


DROP TABLE dm_ups_user_info_h2es_alpha;
CREATE TABLE dm_ups_user_info_h2es_alpha(
user_id string,
order_cnt int,
order_date_prefer ARRAY<STRUCT<key:string, value:double>>
)
STORED BY
'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES (
'es.resource'='bdi_eco_data_index/eco_user_type',
'es.nodes'='10.200.5.146,10.200.4.236,10.200.3.143',
'es.port'='9200',
'es.nodes.wan.only'='true'
)
;


#***************************************************************************************************
# ** 文件名称： dm_ups_user_info_h2es_alpha.sql
# ** 功能描述： 用户画像导入es alpha
#
# ** 创建者：jiahao.dong@ele.me
# ** 创建日期： 2017-03-22
# **
# ** ChangeLog
#***************************************************************************************************

create temporary function transfer_json_kv_format as 'com.eleme.htools.udf.TransferJsonKvFormat';

set mapred.max.split.size=128000000;
insert into table dm_ups_user_info_h2es_alpha
    select
        user_id,
        cast(parse_json_object(profile_json,' trd.order_cnt',false) as int) as order_cnt,
        to_es_nested_struct(
            if (
                parse_json_object(profile_json, 'trd.order_date_prefer', false) is not null,
                transfer_json_kv_format(parse_json_object(profile_json, 'trd.order_date_prefer', false)), 
                null
                ), 1.0
            ) as order_date_prefer
    from 
        dm.dm_ups_user_info_inc 
    where 
        dt='3000-12-31'
;


select to_es_nested_struct(transfer_json_kv_format( '{"膳食平衡": "0.23","实惠套餐": "0.61" }' ),2) as order_date_prefer;


DROP TABLE dm_ups_user_info_h2es_alpha;
CREATE TABLE dm_ups_user_info_h2es_alpha as 
select "1" as user_id, 1 as order_cnt ,to_es_nested_struct('[{"key":"实惠套餐","value":"0.61"},{"key":"膳食平衡","value":"0.23"}]',2.0) as order_date_prefer;



