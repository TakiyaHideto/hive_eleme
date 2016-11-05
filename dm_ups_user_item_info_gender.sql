#***************************************************************************************************
# **  User Profile Service @ dt.rec
#
# **  文件名称： dm_ups_user_item_info_gender.sql
# **  功能描述：
#        1. 从用户的收货地址中提取填写的性别信息
#
# **  创建者：weihua.zheng@ele.me
# **  修改：jiahao.dong@ele.me
# **  创建日期： 2016-08-04 10:10:00
# **
# **  ChangeLog：
#
#***************************************************************************************************

##### sub task : 1
##### 从用户的收货地址中（daily）提取性别的信息一致（地址中可能有多个性别不一致的情况）的用户性别信息
##### 然后和历史累积的数据进行合并，同一个用户以最新的信息为准。

SET 
    mapred.max.split.size=1000000000;
DROP TABLE 
    temp.temp_single_word_sex_frq;
CREATE TABLE 
    temp.temp_single_word_sex_frq AS
SELECT 
    t.name_char, t.sex, count(*) as frq
FROM(
    SELECT 
        case when length(get_normal_word(t.name,0))>0 then single_word else t.name end as name_char, 
        sex
    FROM(
        SELECT 
            name, sex 
        FROM 
            dw.dw_usr_address 
        WHERE 
            dt>='2016-01-01' 
        GROUP BY 
            name, sex
        ) t
    LATERAL VIEW EXPLODE(split(name,'|')) tmp AS single_word
    ) t
WHERE 
    t.name_char!='' and t.name_char is not null and (t.sex='1' or t.sex='2')
GROUP BY 
    t.name_char, t.sex;


DROP TABLE 
    temp.temp_single_word_sex_prob;
CREATE TABLE 
    temp.temp_single_word_sex_prob AS
SELECT 
    name_char, sum(case when sex='1' then frq else 0 end)/sum(frq) as male_prob, sum(case when sex='2' then frq else 0 end)/sum(frq) as female_prob
FROM 
    temp.temp_single_word_sex_frq
GROUP BY 
    name_char;


DROP TABLE 
    temp.temp_test_sex_probability;
CREATE TABLE 
    temp.temp_test_sex_probability AS
SELECT 
    t1.user_id, t1.name, sum(ln(t2.male_prob)) as male_prob, sum(ln(t2.female_prob)) as female_prob
FROM(
    SELECT 
        user_id, t.name, t.name_char, t.sex
    FROM(
        SELECT 
            user_id, name, case when length(get_normal_word(t.name,0))>0 then single_word else t.name end as name_char, sex
        FROM(
            SELECT 
                user_id, name, sex
            FROM 
                dw.dw_usr_address 
            WHERE 
                dt>='2016-01-01' and user_id<>886
            GROUP BY 
                user_id, name, sex
            ) t
        LATERAL VIEW EXPLODE(split(name,'|')) tmp AS single_word
        WHERE 
            length(single_word)>0 or length(name)>0
        ) t
    WHERE 
        t.name_char!='' and t.name_char is not null and (t.sex='0' or t.sex is null)
    ) t1
JOIN 
    temp.temp_single_word_sex_prob t2
ON 
    t1.name_char=t2.name_char
WHERE 
    length(t2.name_char)>0
GROUP BY 
    t1.user_id, t1.name;


SET 
    mapred.max.split.size=1000000000;
DROP TABLE 
    temp.temp_user_sexuality_table;
CREATE TABLE 
    temp.temp_user_sexuality_table AS
SELECT t.user_id, max(t.sex) as sex
FROM(
    SELECT 
        user_id, case when sum(male_prob)>sum(female_prob) then '1' else '2' end as sex
    FROM 
        temp.temp_test_sex_probability
    GROUP BY user_id
    UNION ALL
    SELECT 
        user_id, case when sum(case when sex=1 then 1 else 0 end)>sum(case when sex=2 then 1 else 0 end) then '1' else '2' end as sex
    FROM 
        dw.dw_usr_address
    WHERE 
        dt>='2016-01-01' and name!='' and name is not null and (sex='1' or sex='2')
    GROUP BY
        user_id
) t
GROUP BY t.user_id;


INSERT OVERWRITE TABLE 
    dm.dm_ups_user_item_info PARTITION(dt='${day}', flag='base_gender')
SELECT 
    user_id, 'base' AS top_category, 'gender' AS attr_key, sex AS attr_value, 0 AS is_json, '${day}' AS update_time
FROM
    temp.temp_user_sexuality_table;

INSERT OVERWRITE TABLE 
    dm.dm_ups_user_item_info PARTITION(dt='3000-12-31', flag='base_gender')
SELECT 
    user_id, 'base' AS top_category, 'gender' AS attr_key, sex AS attr_value, 0 AS is_json, '${day}' AS update_time
FROM
    temp.temp_user_sexuality_table;



