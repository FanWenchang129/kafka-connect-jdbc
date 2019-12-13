job_id=`cockroach sql --insecure --execute="select job_id from [SHOW JOBS] where status in('running') and description = 'CREATE CHANGEFEED FOR TABLE test INTO ''kafka://192.168.1.2:9092'' WITH confluent_schema_registry = ''http://192.168.1.2:8081'', format = ''experimental_avro'', resolved, updated';" | sed -n '2p'`
cockroach sql --insecure --execute="cancel job $job_id;"
curl -s -X DELETE 192.168.1.2:8083/connectors/faker
. /usr/local/greenplum-db/greenplum_path.sh
psql -h 192.168.1.3 -U hwq1  -c 'truncate test;' -d testdb
cockroach sql --insecure --execute="drop table test;"
cockroach sql --insecure --execute="create table test(id int  primary key,name string,age int,phone int,email string,address string,time timestamp);"
kafka-topics --delete --bootstrap-server 192.168.1.2:9092 --topic test
kafka-topics --create --bootstrap-server 192.168.1.2:9092 --partitions 6 --replication-factor 1 --topic test
cockroach sql --insecure --execute="CREATE CHANGEFEED FOR TABLE test INTO 'kafka://192.168.1.2:9092' WITH resolved,updated,format = experimental_avro, confluent_schema_registry = 'http://192.168.1.2:8081';"
curl -X POST \
 -H "Content-Type: application/json" \
 -H "Accept: application/json" \
 "http://192.168.1.3:8083/connectors" \
 --data '{
"name":"faker",
"config":{
"topics":"test",
"tasks.max":"6",
"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector",
"key.converter.schemas.enable":"true",
"value.converter.schemas.enable":"true",
"connection.url":"jdbc:postgresql://192.168.1.3/testdb?user=hwq1&password=123",
"dialect.name":"PostgreSqlDatabaseDialect",
"insert.mode":"upsert",
"delete.enabled":"true",
"pk.mode":"record_key",
"pk.fields":"id",
"auto.create":"true",
"auto.evolve":"true",
"cdc.upsert.func.name":"test_upsert"}
}'
