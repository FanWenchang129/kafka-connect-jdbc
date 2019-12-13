#参数
insert_record=100000
batch=100000
update_begin=30001
update_end=50000
delete_begin=10001
delete_end=20000
excute_record=$((insert_record+update_end-update_begin+1+delete_end-delete_begin+1))
left_record=$((insert_record-delete_end+delete_begin-1))
table="test"
remote_user="hwq1"
remote_ip="192.168.1.3"
cp_log_path="/home/liu1804/9.6test/remote_gp_log/"

#代码
datafaker=`datafaker rdb cockroachdb://root@localhost:26257/defaultdb?sslmode=disable $table $insert_record --batch $batch --meta meta.txt`
faker_time=${datafaker#*used: }
echo -e "datafaker生成数据所用时间：$faker_time" > mix_result.txt

insert_result=`cockroach sql --insecure --execute="select time from $table where id=$batch ;"`
insert_first_time=${insert_result:5}
echo -e "drdb第一批数据到库时间：$insert_first_time" >> mix_result.txt
#########加载环境变量
gppath=`locate greenplum_path.sh`
. $gppath

#########执行update命令
cockroach sql --insecure --execute="update test set name='update' where id>=$update_begin and id<=$update_end;"
###########执行delete命令
cockroach sql --insecure --execute="delete from test  where id>=$delete_begin and id<=$delete_end;"

###########执行完成判断
echo "---------------同步完成判断-------------"
while true
do
	sleep 10
	count=`psql -h 192.168.1.3 -U hwq1  -c 'SELECT COUNT(*) FROM test;' -d testdb | sed -n '3p'`
	echo "当前count=$count"
	if (($count==$left_record))
	then
		sleep 10
		count=`psql -h 192.168.1.3 -U hwq1  -c 'SELECT COUNT(*) FROM test;' -d testdb | sed -n '3p'`
		echo "二次判断当前count=$count"
		if (($count==$left_record))
		then
			break
		fi	
	fi
done
echo "退出完成判断循环"

gp_log_path="/home/hwq1/gpadmin/gpdata/gpmaster/gpsne-1/pg_log"
gp_latest_log=`ssh ${remote_user}@${remote_ip} ls ${gp_log_path} | grep "gpdb-" | tail -1`
echo "已找到远程日志gp_latest_log="$gp_latest_log
scp ${remote_user}@${remote_ip}:$gp_log_path/$gp_latest_log $cp_log_path
last_commit=`gplogfilter "${cp_log_path}${gp_latest_log}" | grep "COMMIT" | tail -1`
last_commit_time=${last_commit:0:27}
echo -e "最后一条greenplum commit时间：$last_commit_time" >> mix_result.txt

begin=`date -d "${insert_first_time:0:19}" +%s`
end=`date -d "${last_commit_time}" +%s`
d_value=$((end-begin))
echo "执行总SQL数量=${excute_record}" >> mix_result.txt
echo "耗时=${d_value}s" >> mix_result.txt
speed=$((excute_record/d_value))
echo "同步速率=${speed}条/s" >> mix_result.txt
echo -e "插入数量=${insert_record} \n update数量=$((update_end-update_begin+1)) \n delete数量=$((delete_end-delete_begin+1))" >> mix_resuilt.txt
echo -e "update范围:$update_begin ~ $update_end \n delete范围:$delete_begin ~ $delete_end" >> mix_resuilt.txt

echo -e "\n update SQL查询：SELECT COUNT(*) FROM test where id>=$update_begin and id<=$update_end and name='update';" >> mix_result.txt
echo -e "\n delete SQL查询：SELECT COUNT(*) FROM test where id>=$delete_begin and id<=$delete_end" >> mix_result.txt
