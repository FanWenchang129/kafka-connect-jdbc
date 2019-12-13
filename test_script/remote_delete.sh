#参数
record=100000
table="test"
remote_user="hwq1"
remote_ip="192.168.1.3"
cp_log_path="/home/liu1804/9.6test/remote_gp_log/"

#代码

cockroach sql --insecure --execute="delete from $table"
delete_start_time=$(date +"%Y-%m-%d %H:%M:%S")
echo -e "删除初始时间：$delete_start_time" > delete_result.txt

gppath=`locate greenplum_path.sh`
. $gppath

count=$record
while true
do
	sleep 30
	count=`psql -h 192.168.1.3 -U hwq1  -c 'SELECT COUNT(*) FROM test;' -d testdb | sed -n '3p'`
	echo "当前count=$count"
	if (($count==0))
	then
		break
	fi
done
echo "删除完成,退出循环"

gp_log_path="/home/hwq1/gpadmin/gpdata/gpmaster/gpsne-1/pg_log"
gp_latest_log=`ssh ${remote_user}@${remote_ip} ls ${gp_log_path} | grep "gpdb-" | tail -1`
echo "gp_latest_log="$gp_latest_log
scp ${remote_user}@${remote_ip}:$gp_log_path/$gp_latest_log $cp_log_path
last_commit=`gplogfilter "${cp_log_path}${gp_latest_log}" | grep "COMMIT" | tail -1`
last_commit_time=${last_commit:0:27}
echo -e "最后一条greenplum commit时间：$last_commit_time" >> delete_result.txt

begin=`date -d "$delete_start_time" +%s`
end=`date -d "${last_commit_time}" +%s`
d_value=$((end-begin))
echo "耗时=${d_value}s" >> delete_result.txt
speed=$((record/d_value))
echo "删除同步速率=${speed}条/s" >> delete_result.txt

