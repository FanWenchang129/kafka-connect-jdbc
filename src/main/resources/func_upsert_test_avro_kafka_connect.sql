CREATE OR REPLACE FUNCTION func_upsert_test_avro_kafka_connect(text, bigint, bigint, bigint, text)
  RETURNS int AS
$$

declare
v_count int;

begin

  update test_avro_kafka_connect set ts=to_timestamp( round( cast ( $3 as numeric )/ cast( 1000000 as numeric),6)),age=$4,name=$5 where id=$2;
  GET DIAGNOSTICS v_count = ROW_COUNT;
  if not found then    
    insert into test_avro_kafka_connect (id,ts,age,name) values ($2,to_timestamp( round( cast ( $3 as numeric )/ cast( 1000000 as numeric),6)),$4,$5); 
    GET DIAGNOSTICS v_count = ROW_COUNT;	
  end if;    
  return v_count;
  exception when others then    
      update test_avro_kafka_connect set ts=to_timestamp( round( cast ( $3 as numeric )/ cast( 1000000 as numeric),6)),age=$4,name=$5 where id=$2; 
      GET DIAGNOSTICS v_count = ROW_COUNT;  
      return v_count;
end;
$$ language plpgsql strict;   