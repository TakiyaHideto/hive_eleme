#***************************************************************************************************
# **  文件名称： dm_ups_taker_info_h2c_beta.sql
# **  功能描述： 骑手画像导入Cassandra beta
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2017-03-20
# **
# **  ChangeLog
#***************************************************************************************************

insert into table dm_test.dm_ups_taker_info_h2c_beta
select
taker_id,
parse_json_object(profile_json,'base.city_id',false) as city_id,
parse_json_object(profile_json,'base.created_at',false) as created_at,
parse_json_object(profile_json,'base.delivery_mode',false) as delivery_mode,
parse_json_object(profile_json,'base.geo_name',false) as geo_name,
parse_json_object(profile_json,'base.max_rst_count_per_deliver',false) as max_rst_count_per_deliver,
parse_json_object(profile_json,'base.team_id',false) as team_id,
parse_json_object(profile_json,'base.team_name',false) as team_name,
parse_json_object(profile_json,'base.org_id',false) as org_id,
parse_json_object(profile_json,'base.team_created_at',false) as team_created_at,
parse_json_object(profile_json,'base.team_load',false) as team_load,
parse_json_object(profile_json,'base.team_status',false) as team_status,

parse_json_object(profile_json,'delivery.period_deliver_distance_avg',false) as period_deliver_distance_avg,
parse_json_object(profile_json,'delivery.period_fetch_distance_avg',false) as period_fetch_distance_avg,
parse_json_object(profile_json,'delivery.period_deliver_time_avg',false) as period_deliver_time_avg,
parse_json_object(profile_json,'delivery.period_deliver_speed_avg',false) as period_deliver_speed_avg,
parse_json_object(profile_json,'delivery.recent_14_deliver_speed_trend',false) as recent_14_deliver_speed_trend,
parse_json_object(profile_json,'delivery.fetch_speed_weighted_avg',false) as fetch_speed_weighted_avg,
parse_json_object(profile_json,'delivery.team_deliver_speed_rank',false) as team_deliver_speed_rank,

parse_json_object(profile_json,'status.rest_level_distribution',false) as rest_level_distribution,
parse_json_object(profile_json,'status.recent_30_order_cnt',false) as recent_30_order_cnt,
parse_json_object(profile_json,'status.recent_30_date_cnt',false) as recent_30_date_cnt,
parse_json_object(profile_json,'status.day_peak_order_cnt_avg',false) as day_peak_order_cnt_avg,
parse_json_object(profile_json,'status.day_peak_order_cnt_std',false) as day_peak_order_cnt_std,
parse_json_object(profile_json,'status.night_peak_order_cnt_avg',false) as night_peak_order_cnt_avg,
parse_json_object(profile_json,'status.night_peak_order_cnt_std',false) as night_peak_order_cnt_std,
parse_json_object(profile_json,'status.ontime_order_rate',false) as ontime_order_rate,
parse_json_object(profile_json,'status.day_peak_ontime_rate',false) as day_peak_ontime_rate,
parse_json_object(profile_json,'status.night_peak_ontime_rate',false) as night_peak_ontime_rate,
parse_json_object(profile_json,'status.recent_14_day_peak_order_cnt_trend',false) as recent_14_day_peak_order_cnt_trend,
parse_json_object(profile_json,'status.recent_14_night_peak_order_cnt_trend',false) as recent_14_night_peak_order_cnt_trend,
parse_json_object(profile_json,'status.recent_14_ontime_trend',false) as recent_14_ontime_trend

from dm.dm_ups_taker_info 
where dt='3000-12-31' and taker_id<=100000
;

