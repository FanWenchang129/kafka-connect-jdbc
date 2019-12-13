#参数
update_begin=1
update_end=200000
record=$((update_end-update_begin+1))
table="test"
remote_user="hwq1"
remote_ip="192.168.1.3"
gp_log_path="/home/hwq1/gpadmin/gpdata/gpmaster/gpsne-1/pg_log/"

cp_log_path="/home/liu1804/9.6test/remote_gp_log/"
#代码

#########加载环境变量
gppath=`locate greenplum_path.sh`
. $gppath
########记录update前时间
begin_time=$(date +%s)
#########执行update命令
cockroach sql --insecure --execute="update test set name='update2' where id>=$update_begin and id<=$update_end;"

###########执行完成判断
echo "---------------同步完成判断-------------"
while true
do
	sleep 20
	count=`psql -h 192.168.1.3 -U hwq1  -c "SELECT COUNT(*) FROM test where id>=$update_begin and id<=$update_end and name='update2';" -d testdb | sed -n '3p'`
	echo "当前已经update的count=$count"
	if (($count==$record))
	then
		break
	fi
done
echo "退出完成判断循环"

gp_latest_log=`ssh ${remote_user}@${remote_ip} ls ${gp_log_path} | grep "gpdb-" | tail -1`
echo "已找到远程日志gp_latest_log="$gp_latest_log
scp ${remote_user}@${remote_ip}:$gp_log_path/$gp_latest_log $cp_log_path
last_commit=`gplogfilter "${cp_log_path}${gp_latest_log}" | grep "COMMIT" | tail -1`
last_commit_time=${last_commit:0:27}
echo "开始时间：$begin_time" > update_result.txt
echo "最后一条greenplum commit时间：$last_commit_time" >> update_result.txt

begin=$begin_time
end=`date -d "${last_commit_time}" +%s`
d_value=$((end-begin))
echo "执行总SQL数量=${record}" >> update_result.txt
echo "耗时=${d_value}s" >> update_result.txt
speed=$((record/d_value))
echo "同步速率=${speed}条/s" >> update_result.txt
echo -e "update范围:$update_begin ~ $update_end"  >> update_result.txt

echo -e "\n update SQL查询：SELECT COUNT(*) FROM test where id>=$update_begin and id<=$update_end and name='update';" >> update_result.txt
