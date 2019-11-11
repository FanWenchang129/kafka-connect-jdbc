/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

public class SinkRecordBuilderTest {

  @Test
  public void testUpsertNormal() {
    final Schema schema= SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    SinkRecord record = CdcRecordBuilder.upsert(schema)
      .pk("id", 1)
      .col("id", 1)
      .col("name", "nick")
      .col("age", 12)
      .build();

    final Schema valueSchemaAfter = SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    final Schema valueSchema= SchemaBuilder.struct()
      .field("updated", Schema.STRING_SCHEMA)
      .field("after", valueSchemaAfter)
      .build();

    final Struct valueAfter = new Struct(valueSchemaAfter)
      .put("id", 1)
      .put("name", "nick")
      .put("age", 12);
      
    final Struct value =  new Struct(valueSchema)
      .put("updated", "unspecified")
      .put("after", valueAfter);

    final Schema keySchema = SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .build();

    final Struct key = new Struct(keySchema)
      .put("id", 1);

    final SinkRecord recordE = new SinkRecord("dummy", 0, keySchema, key, valueSchema, value, 0);
    assert(recordE.equals(record)) ;  
  }
  
  @Test
  public void testUpsertWithoutKey () {
    final Schema schema= SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    SinkRecord record = CdcRecordBuilder.upsert(schema)
      .col("name", "nick")
      .col("age", 12)
      .build();

    final Schema valueSchemaAfter = SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    final Schema valueSchema= SchemaBuilder.struct()
      .field("updated", Schema.STRING_SCHEMA)
      .field("after", valueSchemaAfter)
      .build();

    final Struct valueAfter = new Struct(valueSchemaAfter)
      .put("name", "nick")
      .put("age", 12);
      
    final Struct value =  new Struct(valueSchema)
      .put("updated", "unspecified")
      .put("after", valueAfter);

    final SinkRecord recordE = new SinkRecord("dummy", 0, null, null, valueSchema, value, 0);
    assert(recordE.equals(record)) ;  
  }


  @Test
  public void testUpsertWithTopic () {
    final Schema schema= SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    SinkRecord record = CdcRecordBuilder.upsert("myTopic", schema)
      .col("name", "nick")
      .col("age", 12)
      .build();

    final Schema valueSchemaAfter = SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    final Schema valueSchema= SchemaBuilder.struct()
      .field("updated", Schema.STRING_SCHEMA)
      .field("after", valueSchemaAfter)
      .build();

    final Struct valueAfter = new Struct(valueSchemaAfter)
      .put("name", "nick")
      .put("age", 12);
      
    final Struct value =  new Struct(valueSchema)
      .put("updated", "unspecified")
      .put("after", valueAfter);

    final SinkRecord recordE = new SinkRecord("myTopic", 0, null, null, valueSchema, value, 0);
    assert(recordE.equals(record)) ;  
  }

  @Test
  public void testUpsertWithOtherInfo () {
    final Schema schema= SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .build();

    SinkRecord record = CdcRecordBuilder.upsert("myTopic",1,2,schema)
      .col("name", "nick")
      .col("age", 12)
      .build();

    final Schema valueSchemaAfter = SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    final Schema valueSchema= SchemaBuilder.struct()
      .field("updated", Schema.STRING_SCHEMA)
      .field("after", valueSchemaAfter)
      .build();

    final Struct valueAfter = new Struct(valueSchemaAfter)
      .put("name", "nick")
      .put("age", 12);
      
    final Struct value =  new Struct(valueSchema)
      .put("updated", "unspecified")
      .put("after", valueAfter);

    final SinkRecord recordE = new SinkRecord("myTopic", 1, null, null, valueSchema, value, 2);
    assert(recordE.equals(record)) ;  
  }

  @Test
  public void testResolvedTime () {
    final Schema schema= SchemaBuilder.struct()
    .field("resolved", Schema.STRING_SCHEMA)
    .build();

    SinkRecord record = CdcRecordBuilder.resolved("myTopic",1,2,schema)
      .time("1234")
      .build();

    final Schema valueSchema= SchemaBuilder.struct()
      .field("resolved", Schema.STRING_SCHEMA)
      .build();
      
    final Struct value =  new Struct(valueSchema)
      .put("resolved", "1234");

    final SinkRecord recordE = new SinkRecord("myTopic", 1, null, null, valueSchema, value, 2);
    assert(recordE.equals(record)) ; 
  }

  

}