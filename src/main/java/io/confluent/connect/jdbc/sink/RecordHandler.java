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

import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;

import java.util.HashMap;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.confluent.connect.jdbc.sink.metadata.RecordType;

public class RecordHandler {

  //FIXME: get from config
  protected static boolean checkValueSchemaName = true;

  public static SinkRecord handle(SinkRecord record) {
    SinkRecord newRecord = null;
    RecordType type = getRecordType(record);
    if (type == RecordType.RESOLVED) {
      updateResolvedTime(record);
    } else if (type == RecordType.CDC) {
      newRecord = expandValueSchema(record);
    } else { 
      newRecord = record;
    }
    return newRecord;
  }

  public static void closeCheckValueSchemaName() {
    checkValueSchemaName = false;
  }

  protected static void updateResolvedTime(SinkRecord record) {
    Schema origValueSchema = record.valueSchema();
    Field resolvedField = origValueSchema.field("resolved");
    Struct valueStruct = (Struct) record.value();
    String stringResolved = (String) valueStruct.get(resolvedField);

    int num = stringResolved.indexOf(".");

    String interceptResolved = null;
    if (num > 0) {
      interceptResolved = stringResolved.substring(0, num);
    } else if (num == -1) {
      interceptResolved = stringResolved;
    } else {
      interceptResolved = "0";
    }
    
    Long resolved = Long.parseLong(interceptResolved);

    TimeCache timeCache = TimeCache.getTimeCacheInstance();
    timeCache.updateTime(resolved);
  }

  protected static boolean maybeCdcRecord(SinkRecord record) {
    boolean maybeCdc = false;
    if (!checkValueSchemaName) {
      maybeCdc = true;
    } else {
      Schema orgiValueSchema = record.valueSchema();
      if (!isNull(orgiValueSchema)) {
        String valueSchemaName = orgiValueSchema.name();
        if (!isNull(valueSchemaName)) {
          if (valueSchemaName.equals(record.topic() + "_envelope")) {
            maybeCdc = true;
          }
        }
      }
    }
    return maybeCdc;
  }

  protected static RecordType getRecordType(SinkRecord record) {
    RecordType type = RecordType.OTHER;
    if (maybeCdcRecord(record)) {
      if (record.valueSchema() != null) {
        List<Field> fields = record.valueSchema().fields();
        Map<String, Field> fmap = new HashMap<String, Field>(); 
        for (Field field : fields) {
          fmap.put(field.name(), field);
        }    
        
        if (fmap.containsKey("updated") && fmap.containsKey("after")) {
          // if (updatedField.schema().type() == Schema.Type.STRING 
          //     && afterField.schema().type() == Schema.Type.STRUCT) { 
          //   type = RecordType.CDC;
          // } 
          type = RecordType.CDC;
        }
    
        if (fmap.containsKey("resolved")) {
          // if (resolvedField.schema().type() == Schema.Type.STRING) {
          //   type = RecordType.RESOLVED;
          // }
          type = RecordType.RESOLVED;
        }      
      }
    }
    return type;
  }

  protected static SinkRecord expandValueSchema(SinkRecord record) {
    return expand(record);
  } 

  // Transform the value-schema and expand the nested STRUCT structure
  // on the top layer, for example:
  // Before:
  // Schema {t_avro_envelope:STRUCT}
  // |
  // +--Field {name=updated, schema=STRING}
  // +--Field {name=after, schema={t_avro:STRUCT}}
  // |
  // +--Field {name=id, schema=INT64}
  // +--Field {name=ts, schema=INT64}
  // +--Field {name=age, schema=INT64}
  // +--Field {name=name, schema=STRING}
  // After:
  // Schema {t_avro_envelope:STRUCT}
  // |
  // +--Field {name=updated, schema=STRING}
  // +--Field {name=id, schema=INT64}
  // +--Field {name=ts, schema=INT64}
  // +--Field {name=age, schema=INT64}
  // +--Field {name=name, schema=STRING}
  protected static SinkRecord expand(SinkRecord record) {
    // This record is not sent by DRDB
    Schema orgiValueSchema = record.valueSchema();
    if (isNull(orgiValueSchema)) {
      return record;
    }

    Field afterField = orgiValueSchema.field("after");

    if (isNull(afterField)) {
      return record;
    }

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    schemaBuilder = schemaDelayer(afterField, schemaBuilder);
    
    final Schema valueSchema = schemaBuilder.build();

    Struct valueStruct = (Struct) record.value();
    if (isNull(valueStruct)) {
      return record;
    }
  
    Struct afterValueStruct = (Struct)valueStruct.get(afterField);
    if (isNull(afterValueStruct)) {
      //delete record
      return record.newRecord(record.topic(), record.kafkaPartition(), 
                              record.keySchema() , record.key(), 
                              null,null, 
                              record.timestamp(), record.headers());
    } else {
      Struct value = new Struct(valueSchema);

      value = valueDelayer(value, afterField, afterValueStruct);

      //upsert record
      return record.newRecord(record.topic(), record.kafkaPartition(), 
                              record.keySchema() , record.key(), 
                              valueSchema, value, 
                              record.timestamp(), record.headers());
    }
  }

  protected static SchemaBuilder schemaDelayer(Field fields, SchemaBuilder schemaBuilder) {
    for (Field field : fields.schema().fields()) {
      if (field.schema().type().equals(Schema.Type.STRUCT)) {
        schemaBuilder = schemaDelayer(field, schemaBuilder);
      } else {
        schemaBuilder.field(field.name(), field.schema());
      }
    }
    return schemaBuilder;
  }

  protected static Struct valueDelayer(Struct value, Field afterField, Struct afterValueStruct) {
    for (Field field : afterField.schema().fields()) {
      if (field.schema().type().equals(Schema.Type.STRUCT)) {
        Struct vs = (Struct)afterValueStruct.get(field);
        value = valueDelayer(value, field, vs);
      } else {
        value.put(field.name(), afterValueStruct.get(field));
      }
    }
    return value;
  }
}