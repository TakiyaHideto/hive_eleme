#***************************************************************************************************
# **  文件名称： rec_dom_match_features_engineering_part0.sql
# **  功能描述： dom餐厅匹配特征工程 part0
#
# **  创建者：jiahao.dong@ele.me
# **  创建日期： 2017-04-05
# **
# **  ChangeLog
#***************************************************************************************************


-- sub task 1:
    -- extract original info of candidates 
    -- normalize address as json format for candidates shop
drop table temp.temp_rec_dom_match_features_engineering_part0_normalize_address;
create table temp.temp_rec_dom_match_features_engineering_part0_normalize_address as
    select
        ele_id,
        can_id,
        address1,
        address2,
        address1_json,
        address2_json,
        parse_json_object(address1_json, '街道') as address1_street,
        parse_json_object(address2_json, '街道') as address2_street,
        parse_json_object(address1_json, '楼牌') as address1_buildingno,
        parse_json_object(address2_json, '楼牌') as address2_buildingno,
        parse_json_object(address1_json, '门牌') as address1_doorno,
        parse_json_object(address2_json, '门牌') as address2_doorno,
        parse_json_object(address1_json, 'POI') as address1_poi,
        parse_json_object(address2_json, 'POI') as address2_poi
    from(
        select 
            id1 as ele_id,
            id2 as can_id,
            address1,
            address2,
            normalize_address(address1) as address1_json,
            normalize_address(address2) as address2_json
        from
            test.rec_dom_geohash_candidates
        where 
            dt = '2017-03-31' and
            version = 'chengliang_meituan'
        ) t
;


-- sub task 2:
    -- calculate edit, jw, contain features
drop table temp.temp_rec_dom_match_features_engineering_part0_cal_features;
create table temp.temp_rec_dom_match_features_engineering_part0_cal_features as 
    select
        ele_id,
        can_id,
        address1,
        address2,
        edit_distance(address1_poi, address2_poi) as edit_distance_poi,
        jw_distance(address1_poi, address2_poi) as jw_distance_poi,
        edit_distance(address1_street, address2_street) as edit_distance_street,
        jw_distance(address1_street, address2_street) as jw_distance_street,
        edit_distance(address1_buildingno, address2_buildingno) as edit_distance_buildingno,
        jw_distance(address1_buildingno, address2_buildingno) as jw_distance_buildingno,
        edit_distance(address1_doorno, address2_doorno) as edit_distance_doorno,
        jw_distance(address1_doorno, address2_doorno) as jw_distance_doorno
    from
        temp.temp_rec_dom_match_features_engineering_part0_normalize_address
;









