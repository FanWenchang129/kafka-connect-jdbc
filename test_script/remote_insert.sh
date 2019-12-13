#参数
record=200000
batch=1000
table="test"
remote_user="hwq1"
remote_ip="192.168.1.3"
cp_log_path="/home/liu1804/9.6test/remote_gp_log/"

#代码
for((i=0;i<4;i++));
do 
	datafaker rdb cockroachdb://root@localhost:26257/defaultdb?sslmode=disable $table $((record/4)) --batch $batch --meta meta$i.txt &
done
#datafaker=`datafaker rdb cockroachdb://root@localhost:26257/defaultdb?sslmode=disable $table $record --interval $interval --batch $batch --meta meta.txt`

#faker_time=${datafaker#*used: }
#echo -e "datafaker生成数据所用时间：$faker_time" > insert_result.txt
wait
insert_first_time=`cockroach sql --insecure --execute="select time from $table where id=$batch ;" | sed -n '2p'`
echo -e "drdb第一条数据到库时间：$insert_first_time" >> insert_result.txt

gppath=`locate greenplum_path.sh`
. $gppath

count=0
while true
do
	sleep 10
	count=`psql -h 192.168.1.3 -U hwq1  -c 'SELECT COUNT(*) FROM test;' -d testdb | sed -n '3p'`
	echo "当前count=$count"
	if (($count==$record))
	then
		break
	fi
done
echo "退出循环"

gp_log_path="/home/hwq1/gpadmin/gpdata/gpmaster/gpsne-1/pg_log"
gp_latest_log=`ssh ${remote_user}@${remote_ip} ls ${gp_log_path} | grep "gpdb-" | tail -1`
echo "gp_latest_log="$gp_latest_log
scp ${remote_user}@${remote_ip}:$gp_log_path/$gp_latest_log $cp_log_path
last_commit=`gplogfilter "/home/liu1804/9.6test/remote_gp_log/$gp_latest_log" | grep "COMMIT" | tail -1`
last_commit_time=${last_commit:0:27}
echo -e "最后一条greenplum commit时间：$last_commit_time" >> insert_result.txt

begin=`date -d "${insert_first_time:0:19}" +%s`
end=`date -d "${last_commit_time}" +%s`
d_value=$((end-begin))
echo "耗时=${d_value}s" >> insert_result.txt
speed=$((record/d_value))
echo "同步速率=${speed}条/s" >> insert_result.txt
echo  "插入数量=${record}" >> insert_result.txt
