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

import org.apache.derby.impl.store.raw.data.UpdateFieldOperation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.SchemaBuilderException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import io.confluent.connect.jdbc.sink.metadata.RecordType;

public class RecordHandlerTest extends RecordHandler{

  @Test
  public void testRecordTypeUpsert() {

    final Schema schema= SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build();

    SinkRecord record = CdcRecordBuilder.upsert(schema)
      .pk("id", 1)
      .col("name", "nick")
      .col("age", 12)
      .build();

    utFlag = true;
    RecordType recordType = super.getRecordType(record);
        assert (recordType.equals(RecordType.CDC));
    utFlag = false;
  }

  @Test
  public void testRecordTypeResolved() {

    final Schema schema= SchemaBuilder.struct()
      .field("resolved", Schema.STRING_SCHEMA)
      .build();

    SinkRecord record = CdcRecordBuilder.resolved(schema)
      .time("12345")
      .build();

    utFlag = true;
    RecordType recordType = super.getRecordType(record);
        assert (recordType.equals(RecordType.RESOLVED));
    utFlag = false;
  }

  @Test
  public void testRecordTypeOther() {

    final Schema schemaA = SchemaBuilder.struct()
      .field("updated", Schema.STRING_SCHEMA)
      .field("after", Schema.STRING_SCHEMA)
      .build();

    final Struct valueA = new Struct(schemaA)
      .put("updated", "")
      .put("after", "");

    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);

    utFlag = true;
    RecordType recordType = super.getRecordType(recordA);
        assert (recordType.equals(RecordType.OTHER));
    utFlag = false;
  }

  @Test
  public void testRecordTypeAfterFieldIsNotStruct() {
    final Schema schemaB = SchemaBuilder.struct()
        .field("id", Schema.INT32_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", Schema.STRING_SCHEMA)
        .build();
    final Struct valueB = new Struct(schemaB)
        .put("id",1)
        .put("age",26)
        .put("updated","333")
        .put("after","afterValue");
    final SinkRecord recordB = new  SinkRecord("dummy", 0, null, null, schemaB, valueB, 0);
    
    utFlag = true;
    RecordType recordType = getRecordType(recordB);
        assert (recordType.equals(RecordType.OTHER));
    utFlag = false;
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSchemaInconsistentWithValue() {
    final Schema schemaA = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", Schema.BOOLEAN_SCHEMA)
        .build();

    thrown.expect(DataException.class);
    thrown.expectMessage("Invalid Java object for schema type STRING: class java.lang.Boolean for field: \"updated\"");
    final Struct valueA = new Struct(schemaA)
        .put("updated", true)
        .put("after", "123");
  }

  @Test
  public void testExpend() {
    final Schema schemaInner2 = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInner3 = SchemaBuilder.struct()
    .field("n1",Schema.INT32_SCHEMA)
    .field("n2",Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInner1 = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("other1", schemaInner2)
    .field("other2", schemaInner3)
    .build();

    final Schema schema = SchemaBuilder.struct()
    .field("updated", Schema.STRING_SCHEMA)
    .field("after", schemaInner1)
    .build();

    final Struct value2 = new Struct(schemaInner2)
    .put("id", 1)
    .put("ts", 2);

    final Struct value3 = new Struct(schemaInner3)
    .put("n1", 3)
    .put("n2", 4);

    final Struct value1 = new Struct(schemaInner1)
    .put("name", "he")
    .put("age", 23)
    .put("other1", value2)
    .put("other2", value3);

    final Struct value = new Struct(schema)
    .put("updated", "123")
    .put("after", value1);

    final SinkRecord record = new SinkRecord("dummy", 0, null, null, schema, value, 0);
    SinkRecord recordExpended = super.expandValueSchema(record);

    final Schema schemaResult = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .field("n1", Schema.INT32_SCHEMA)
    .field("n2", Schema.INT32_SCHEMA)
    .build();

    final Struct valueResult = new Struct(schemaResult)
    .put("name", "he")
    .put("age", 23)
    .put("id", 1)
    .put("ts", 2)
    .put("n1", 3)
    .put("n2", 4);

    final SinkRecord recordResult = new SinkRecord("dummy", 0, null, null, schemaResult, valueResult, 0);
    assert(recordResult.equals(recordExpended));

    SinkRecord recordRes = super.expandValueSchema(recordResult);
    assert(recordRes.equals(recordResult));
  }

  @Test
  public void testExpendTwoLayerNested() {
    final Schema schemaInnerA1 = SchemaBuilder.struct()
    .field("m1", Schema.INT32_SCHEMA)
    .field("m2", Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInnerA2 = SchemaBuilder.struct()
    .field("m3", Schema.INT32_SCHEMA)
    .field("m4", Schema.INT32_SCHEMA)
    .build();

    final Schema schemaInnerB1 = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .field("more1", schemaInnerA1)
    .build();

    final Schema schemaInnerB2 = SchemaBuilder.struct()
    .field("n1",Schema.INT32_SCHEMA)
    .field("n2",Schema.INT32_SCHEMA)
    .field("more2", schemaInnerA2)
    .build();

    final Schema schemaInnerC = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("other1", schemaInnerB1)
    .field("other2", schemaInnerB2)
    .build();

    final Schema schema = SchemaBuilder.struct()
    .field("updated", Schema.STRING_SCHEMA)
    .field("after", schemaInnerC)
    .build();

    final Struct valueA1 = new Struct(schemaInnerA1)
    .put("m1", 1)
    .put("m2", 2);

    final Struct valueA2 = new Struct(schemaInnerA2)
    .put("m3", 3)
    .put("m4", 4);

    final Struct valueB1 = new Struct(schemaInnerB1)
    .put("id", 1)
    .put("ts", 2)
    .put("more1", valueA1);

    final Struct valueB2 = new Struct(schemaInnerB2)
    .put("n1", 3)
    .put("n2", 4)
    .put("more2", valueA2);

    final Struct valueC = new Struct(schemaInnerC)
    .put("name", "he")
    .put("age", 23)
    .put("other1", valueB1)
    .put("other2", valueB2);

    final Struct value = new Struct(schema)
    .put("updated", "123")
    .put("after", valueC);

    final SinkRecord record = new SinkRecord("dummy", 0, null, null, schema, value, 0);
    SinkRecord recordExpended = super.expandValueSchema(record);

    final Schema schemaResult = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("id", Schema.INT32_SCHEMA)
    .field("ts", Schema.INT32_SCHEMA)
    .field("m1", Schema.INT32_SCHEMA)
    .field("m2", Schema.INT32_SCHEMA)
    .field("n1", Schema.INT32_SCHEMA)
    .field("n2", Schema.INT32_SCHEMA)
    .field("m3", Schema.INT32_SCHEMA)
    .field("m4", Schema.INT32_SCHEMA)
    .build();

    final Struct valueResult = new Struct(schemaResult)
    .put("name", "he")
    .put("age", 23)
    .put("id", 1)
    .put("ts", 2)
    .put("m1", 1)
    .put("m2", 2)
    .put("n1", 3)
    .put("n2", 4)
    .put("m3", 3)
    .put("m4", 4);

    final SinkRecord recordResult = new SinkRecord("dummy", 0, null, null, schemaResult, valueResult, 0);
    assert(recordResult.equals(recordExpended));

    final SinkRecord recordRes = super.expandValueSchema(recordResult);
    assert(recordRes.equals(recordResult));
  }

  @Test
  public void testExpendSameField() {
    final Schema schemaInnerB2 = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .build();
     final Schema schemaInnerB3 = SchemaBuilder.struct()
    .field("n1",Schema.INT32_SCHEMA)
    .field("n2",Schema.INT32_SCHEMA)
    .build();
     final Schema schemaInnerB1 = SchemaBuilder.struct()
    .field("name", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("other1", schemaInnerB2)
    .field("other2", schemaInnerB3)
    .build();
     final Schema schemaB = SchemaBuilder.struct()
    .field("updated", Schema.STRING_SCHEMA)
    .field("after", schemaInnerB1)
    .build();
     final Struct valueB2 = new Struct(schemaInnerB2)
    .put("id", 1)
    .put("age", 2);
     final Struct valueB3 = new Struct(schemaInnerB3)
    .put("n1", 3)
    .put("n2", 4);
     final Struct valueB1 = new Struct(schemaInnerB1)
    .put("name", "he")
    .put("age", 23)
    .put("other1", valueB2)
    .put("other2", valueB3);
     final Struct valueB = new Struct(schemaB)
    .put("updated", "123")
    .put("after", valueB1);
     final SinkRecord recordB = new SinkRecord("dummy", 0, null, null, schemaB, valueB, 0);
    thrown.expect(SchemaBuilderException.class);
    thrown.expectMessage("Cannot create field because of field name duplication age");
    super.expandValueSchema(recordB);
  }
  
  @Test
  public void testExpendAfterFieldIsNotStruct() {
    final Schema schemaA = SchemaBuilder.struct()
        .field("updated", Schema.STRING_SCHEMA)
        .field("after", Schema.STRING_SCHEMA)
        .build();

    final Struct valueA = new Struct(schemaA)
        .put("updated", "")
        .put("after", "");

    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);

    thrown.expect(DataException.class);
    thrown.expectMessage("Cannot list fields on non-struct type");
    super.expandValueSchema(recordA);
  }

  @Test
  public void testExpendFieldIsNull() {
    final Schema schemaInner = SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .field("ts", Schema.INT32_SCHEMA)
      .field("age",Schema.INT32_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .build();

    final Schema schemaA = SchemaBuilder.struct()
      .field("updated", Schema.STRING_SCHEMA)
      .field("after", schemaInner)
      .build();

    thrown.expect(DataException.class);
    thrown.expectMessage("Invalid value: null used for required field: \"age\", schema type: INT32");

    final Struct valueInner = new Struct(schemaInner)
      .put("id", 1)
      .put("ts", 100)
      .put("age", null)
      .put("name", "nick");

    thrown.expect(DataException.class);
    thrown.expectMessage("Invalid value: null used for required field: \"age\", schema type: INT32");
    
    final Struct valueA = new Struct(schemaA)
      .put("updated", "123")
      .put("after", valueInner);
    
    final SinkRecord recordA = new SinkRecord("dummy", 0, null, null, schemaA, valueA, 0);
    super.expandValueSchema(recordA);
  } 

}