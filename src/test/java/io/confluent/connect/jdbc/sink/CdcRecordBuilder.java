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

import static org.mockito.Answers.values;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public class CdcRecordBuilder {
  protected String topic = "dummy";
  protected int partition = 0;
  protected long kafkaOffset = 0;
  protected Schema keySchema = null;
  protected Object keyValue = null;
  protected Schema valueSchema = null;
  protected Struct valueAfter = null;
  protected final String defaultTime = "unspecified";

  public static UpsertRecordBuilder upsert(Schema valueSchemaAfter) {
    return new UpsertRecordBuilder(valueSchemaAfter);
  }
  
  public static UpsertRecordBuilder upsert(String topic,  Schema valueSchemaAfter) {
      return new UpsertRecordBuilder(topic, valueSchemaAfter);
  }
  
  public static UpsertRecordBuilder upsert(String topic,  int partition, long kafkaOffset, Schema valueSchemaAfter) {
      return new UpsertRecordBuilder(topic,partition, kafkaOffset, valueSchemaAfter);
  }

  public static ResolvedRecordBuilder resolved(Schema valueSchemaAfter) {
    return new ResolvedRecordBuilder(valueSchemaAfter);
  }
  
  public static ResolvedRecordBuilder resolved(String topic,  Schema valueSchemaAfter) {
      return new ResolvedRecordBuilder(topic, valueSchemaAfter);
  }
  
  public static ResolvedRecordBuilder resolved(String topic,  int partition, long kafkaOffset, Schema valueSchemaAfter) {
      return new ResolvedRecordBuilder(topic,partition, kafkaOffset, valueSchemaAfter);
  }
}

class DeleteRecordBuilder {

}

class ResolvedRecordBuilder extends CdcRecordBuilder {
  private String timestamp = "unspecified";
  
  ResolvedRecordBuilder(Schema valueSchemaAfter) {
    valueSchema = SchemaBuilder.struct()
      .field("resolved", Schema.STRING_SCHEMA)
      .build();
  }

  ResolvedRecordBuilder(String topic,  Schema valueSchemaAfter) {
    this(valueSchemaAfter);
    this.topic = topic;
  }

  ResolvedRecordBuilder(String topic,  int partition, long kafkaOffset, Schema valueSchemaAfter) {
    this(topic, valueSchemaAfter);
    this.partition = partition;
    this.kafkaOffset = kafkaOffset;
  }

  public ResolvedRecordBuilder time(String timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public SinkRecord build() {
    final Struct value = new Struct(valueSchema)
    .put("resolved", this.timestamp);

    return new SinkRecord(topic,partition,
                          keySchema,keyValue,
                          valueSchema,value,kafkaOffset);
  }

}

class UpsertRecordBuilder  extends CdcRecordBuilder {

  private String updatedTime = defaultTime;

  UpsertRecordBuilder(Schema valueSchemaAfter) {
    valueSchema = SchemaBuilder.struct()
      .field("updated", Schema.STRING_SCHEMA)
      .field("after", valueSchemaAfter)
      .build();
      
    valueAfter = new Struct(valueSchemaAfter);
  }

  UpsertRecordBuilder(String topic,  Schema valueSchemaAfter) {
    this(valueSchemaAfter);
    this.topic = topic;
  }

  UpsertRecordBuilder(String topic,  int partition, long kafkaOffset, Schema valueSchemaAfter) {
    this(topic, valueSchemaAfter);
    this.partition = partition;
    this.kafkaOffset = kafkaOffset;
  }

  public UpsertRecordBuilder pk(String pkName, int pkValue) {
    if (pkName != null) {
      keySchema = SchemaBuilder.struct()
        .field(pkName, Schema.INT32_SCHEMA)
        .build();
    }
    if (pkValue >= 0) {
      keyValue = new Struct(keySchema).put(pkName, pkValue);
    }
    return this;  
  }

  public UpsertRecordBuilder col(String colName, Object colValue) {
      valueAfter.put(colName, colValue);
      return this;
  }

  public UpsertRecordBuilder  updated(String time) {
    this.updatedTime = time;
    return this;
  }

  public SinkRecord build () {
      final Struct value = new Struct(valueSchema)
      .put("updated", updatedTime)
      .put("after", valueAfter);

      return new SinkRecord(topic,partition,
                            keySchema,keyValue,
                            valueSchema,value,kafkaOffset);
  }
}