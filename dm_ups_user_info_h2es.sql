#***************************************************************************************************
# ** 文件名称： dm_ups_user_info_h2es.sql
# ** 功能描述： 创建用户画像es链接表 alpha
#
# ** 创建者：jiahao.dong@ele.me
# ** 创建日期： 2017-03-21
# **
# ** ChangeLog
#***************************************************************************************************


DROP TABLE dm_ups_user_info_h2es;
CREATE TABLE dm_ups_user_info_h2es(
    user_id bigint,
    retail_user_type int
  )
STORED BY
  'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES (
  'es.resource'='bdi_eco_data_index/eco_user_type',
  'es.nodes'='10.200.5.146,10.200.4.236,10.200.3.143',
  'es.port'='9200',
  'es.mapping.id'='user_id',
  'es.nodes.wan.only'='true',
  'es.index.translog.flush_threshold_size'='4gb',
  'es.index.translog.interval'='30s',
  'es.index.translog,sync_interval'='30s',
  'index.translog.durability'='async'
)
;

set mapreduce.input.fileinputformat.split.minsize=1000000000000;
set mapreduce.input.fileinputformat.split.maxsize=1000000000000;
insert into table dm_ups_user_info_h2es
    select
        user_id,
        parse_json_object(profile_json,'trd.retail_user_type') as retail_user_type
    from 
        dm.dm_ups_user_info
    where 
        dt='3000-12-31' 
;



drop table temp.temp_dm_ups_user_info;
create table temp.temp_dm_ups_user_info as
    select
        *
    from 
        dm.dm_ups_user_info
    where 
        dt='3000-12-31' and 
        user_id<40000000
;