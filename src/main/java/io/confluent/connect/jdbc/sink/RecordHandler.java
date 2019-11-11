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
  protected boolean utFlag = false;

  public void handle(SinkRecord record) {
    RecordType type = getRecordType(record);
    if (type == RecordType.RESOLVED) {
      updateResolvedTime(record);
      record = null;
    } else if (type == RecordType.CDC) {
      record = expandValueSchema(record);
    } else { 
      // do nothing
    }
  }

  protected void updateResolvedTime(SinkRecord record) {
    Schema origValueSchema = record.valueSchema();
    Field resolvedField = origValueSchema.field("resolved");
    Struct valueStruct = (Struct) record.value();
    String stringResolved = (String) valueStruct.get(resolvedField);

    int num = stringResolved.indexOf(".");
    String interceptResolved = stringResolved.substring(0, num);
    Long resolved = Long.parseLong(interceptResolved);

    TimeCache timeCache = TimeCache.getTimeCacheInstance();
    timeCache.updateTime(resolved);
  }

  protected boolean maybeCdcRecord(SinkRecord record) {
    boolean maybeCdc = false;
    Schema orgiValueSchema = record.valueSchema();
    if (!isNull(orgiValueSchema)) {
      String valueSchemaName = orgiValueSchema.name();
      if (!isNull(valueSchemaName) ) {
        if (valueSchemaName.equals(record.topic()+ "_envelope")){
          maybeCdc = true;
        }
      }
    }
    if (utFlag) {
      maybeCdc = true;
    }
    return maybeCdc;
  }

  protected RecordType getRecordType(SinkRecord record) {
    RecordType type = RecordType.OTHER;
    if (maybeCdcRecord(record)) {
      List<Field> fields = record.valueSchema().fields();
      Map<String, Field> fmap = new HashMap<String, Field>(); 
      for( Field field : fields) {
        fmap.put(field.name(), field);
      }
      Field updatedField  = fmap.containsKey("updated" )? fmap.get("updated") :null;
      Field afterField    = fmap.containsKey("after"   )? fmap.get("after")   :null;
      Field resolvedField = fmap.containsKey("resolved")? fmap.get("resolved"):null;

      if (updatedField !=null && afterField != null && 
          updatedField.schema().type() == Schema.Type.STRING &&
          afterField.schema().type() == Schema.Type.STRUCT) {
        type = RecordType.CDC;
      }
      if (resolvedField !=null && 
        resolvedField.schema().type() == Schema.Type.STRING) {
        type = RecordType.RESOLVED;
      }
    }
    return type;
  }

  protected RecordType getRecordType2(SinkRecord record) {
    Schema orgiValueSchema = record.valueSchema();
    if (isNull(orgiValueSchema)) {
      return RecordType.OTHER;
    }
    String valueSchemaName = orgiValueSchema.name();
    if (isNull(valueSchemaName) && !utFlag) {
      return RecordType.OTHER;
    }

    String topicName = record.topic();
    if (valueSchemaName == (topicName + "_envelope") || utFlag) {
      List<Field> fields = orgiValueSchema.fields();
      Map<String, Field> fmap = new HashMap<String, Field>(); 
      for( Field field : fields) {
        fmap.put(field.name(), field);
      }

      Field updatedField = fmap.containsKey("updated")? fmap.get("updated"):null;
      Field afterField = fmap.containsKey("after")? fmap.get("after"):null;
      Field resolvedField = fmap.containsKey("resolved")? fmap.get("resolved"):null;

      if (!isNull(resolvedField)) {
        List<Field> fieldsList = orgiValueSchema.fields();
        if (fieldsList.size() == 1) {
          return RecordType.RESOLVED;
        } else {
          return RecordType.OTHER;
        }
      }

      if (isNull(updatedField) || isNull(afterField)) {
        return RecordType.OTHER;
      }
      return RecordType.CDC;
    }

    return RecordType.OTHER;
  }

  protected SinkRecord expandValueSchema(SinkRecord record) {
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
  protected SinkRecord expand(SinkRecord record) {
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

  protected SchemaBuilder schemaDelayer(Field fields, SchemaBuilder schemaBuilder) {
    for (Field field : fields.schema().fields()) {
      if (field.schema().type().equals(Schema.Type.STRUCT)) {
        schemaBuilder = schemaDelayer(field, schemaBuilder);
      } else {
        schemaBuilder.field(field.name(), field.schema());
      }
    }
    return schemaBuilder;
  }

  protected Struct valueDelayer(Struct value, Field afterField, Struct afterValueStruct) {
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