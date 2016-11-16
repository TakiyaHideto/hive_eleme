#***************************************************************************************************
# ** 文件名称： dim_mdl_rst_normal_name_mapping.sh
# ** 功能描述： 处理餐厅标准名称
# ** 创建者： jiahao.dong
# ** 创建日期： 2016-11-11
#***************************************************************************************************

input_file='/home/master/workspace/data/mail/ori_rst_name'
output_file='/home/master/workspace/data/mail/nor_rst_name'

touch $output_file

day=`date -d @$1 +%Y-%m-%d`
echo $day

hive -e "
    select 
        name
    from 
        dw.dw_prd_restaurant
    where 
        dt='${day}' and
        is_valid=1
    group by 
        name
;" > ${input_file}



python /home/master/hadoop_project/test/dim/exec/ProcessRstNormalName.py $input_file $output_file


echo `wc -l ${input_file}`
echo `wc -l ${output_file}`


hive -e "
	drop table temp.temp_restaurant_seg_name_mapping;
	create table temp.temp_restaurant_seg_name_mapping(
		rst_ori_name string,
		rst_nor_name string,
		priority int
	) partitioned by (part string)
	row format delimited
	fields terminated by '\t' 
	stored as textfile;

	LOAD DATA LOCAL inpath '/home/master/workspace/data/mail/nor_rst_name' OVERWRITE INTO TABLE  temp.temp_restaurant_seg_name_mapping PARTITION (part='base');

	DROP TABLE dm.dm_mdl_hotfood_restaurant_name_origin_segment_mapping;
	CREATE TABLE dm.dm_mdl_hotfood_restaurant_name_origin_segment_mapping AS
		SELECT 
			*
		FROM 
			temp.temp_restaurant_seg_name_mapping
	;
"


rm $input_file
rm $output_file