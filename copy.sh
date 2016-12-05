rm *.sql
grep -r jiahao.dong /Users/hideto/Project/hadoop_project/test/ > temp.txt
grep -r jiahao.dong /Users/hideto/Project/hadoop_project/online/ >> temp.txt

cat temp.txt | while read line
do
	filename=`echo $line | awk -F ':' '{print $1}'`
	str=".svn-base"
	result=$(echo $filename | grep ".svn-base")
	if [[ "$result" != "" ]]
	then 
		echo $filename
	else
		cp $filename /Users/hideto/Project/hive_eleme
	fi
done

rm temp.txt
