/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jdbc;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BaseJdbcAutoSchemaSinkTest {

    @Test
    public void testConvertAvroString() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField("mystring", createFieldAndGetSchema((builder) ->
                builder.name("field").type().stringType().noDefault()));
        Assert.assertEquals(converted, "mystring");

        converted = BaseJdbcAutoSchemaSink.convertAvroField(new Utf8("mystring"), createFieldAndGetSchema((builder) ->
                builder.name("field").type().stringType().noDefault()));
        Assert.assertEquals(converted, "mystring");

    }

    @Test
    public void testConvertAvroInt() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Integer.MIN_VALUE,
                createFieldAndGetSchema((builder) ->
                        builder.name("field").type().intType().noDefault()));
        Assert.assertEquals(converted, Integer.MIN_VALUE);
    }

    @Test
    public void testConvertAvroLong() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Long.MIN_VALUE, createFieldAndGetSchema((builder) ->
                builder.name("field").type().longType().noDefault()));
        Assert.assertEquals(converted, Long.MIN_VALUE);
    }

    @Test
    public void testConvertAvroBoolean() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(true, createFieldAndGetSchema((builder) ->
                builder.name("field").type().booleanType().noDefault()));
        Assert.assertEquals(converted, true);
    }

    @Test
    public void testConvertAvroEnum() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField("e1", createFieldAndGetSchema((builder) ->
                builder.name("field").type().enumeration("myenum").symbols("e1", "e2").noDefault()));
        Assert.assertEquals(converted, "e1");
    }

    @Test
    public void testConvertAvroFloat() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Float.MIN_VALUE, createFieldAndGetSchema((builder) ->
                builder.name("field").type().floatType().noDefault()));
        Assert.assertEquals(converted, Float.MIN_VALUE);
    }

    @Test
    public void testConvertAvroDouble() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Double.MIN_VALUE,
                createFieldAndGetSchema((builder) ->
                        builder.name("field").type().doubleType().noDefault()));
        Assert.assertEquals(converted, Double.MIN_VALUE);
    }


    @Test
    public void testConvertAvroUnion() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Integer.MAX_VALUE,
                createFieldAndGetSchema((builder) ->
                        builder.name("field").type().unionOf().intType().endUnion().noDefault()));
        Assert.assertEquals(converted, Integer.MAX_VALUE);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesBytes() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type().bytesType().noDefault()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesFixed() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type().fixed("fix").size(16).noDefault()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesRecord() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type()
                        .record("myrecord").fields()
                        .name("f1").type().intType().noDefault()
                        .endRecord().noDefault()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesMap() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type().map().values().stringType().noDefault()));
    }


    @Test
    public void testConvertAvroArray() {
        // Test string array conversion
        Schema stringArraySchema = createFieldAndGetSchema((builder) ->
                builder.name("field").type().array().items().stringType().noDefault());

        GenericData.Array<String> stringArray = new GenericData.Array<>(3, stringArraySchema);
        stringArray.add("item1");
        stringArray.add("item2");
        stringArray.add("item3");

        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(stringArray, stringArraySchema);
        Assert.assertTrue(converted instanceof Object[]);
        Object[] convertedArray = (Object[]) converted;
        Assert.assertEquals(convertedArray.length, 3);
        Assert.assertEquals(convertedArray[0], "item1");
        Assert.assertEquals(convertedArray[1], "item2");
        Assert.assertEquals(convertedArray[2], "item3");

        // Test integer array conversion
        Schema intArraySchema = createFieldAndGetSchema((builder) ->
                builder.name("field").type().array().items().intType().noDefault());

        GenericData.Array<Integer> intArray = new GenericData.Array<>(2, intArraySchema);
        intArray.add(42);
        intArray.add(100);

        converted = BaseJdbcAutoSchemaSink.convertAvroField(intArray, intArraySchema);
        Assert.assertTrue(converted instanceof Object[]);
        convertedArray = (Object[]) converted;
        Assert.assertEquals(convertedArray.length, 2);
        Assert.assertEquals(convertedArray[0], 42);
        Assert.assertEquals(convertedArray[1], 100);

        // Test empty array
        GenericData.Array<String> emptyArray = new GenericData.Array<>(0, stringArraySchema);
        converted = BaseJdbcAutoSchemaSink.convertAvroField(emptyArray, stringArraySchema);
        Assert.assertTrue(converted instanceof Object[]);
        convertedArray = (Object[]) converted;
        Assert.assertEquals(convertedArray.length, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Expected GenericData.Array for ARRAY schema type.*")
    public void testConvertAvroArrayWithWrongType() {
        Schema arraySchema = createFieldAndGetSchema((builder) ->
                builder.name("field").type().array().items().stringType().noDefault());

        // Pass a non-GenericData.Array object to trigger the exception
        BaseJdbcAutoSchemaSink.convertAvroField("not an array", arraySchema);
    }


    @Test
    public void testConvertAvroNullValue() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(null, createFieldAndGetSchema((builder) ->
                builder.name("field").type().stringType().noDefault()));
        Assert.assertNull(converted);
    }


    private Schema createFieldAndGetSchema(Function<SchemaBuilder.FieldAssembler<Schema>,
            SchemaBuilder.FieldAssembler<Schema>> consumer) {
        final SchemaBuilder.FieldAssembler<Schema> record = SchemaBuilder.record("record")
                .fields();
        return consumer.apply(record).endRecord().getFields().get(0).schema();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Primitive schema is not supported.*")
    @SuppressWarnings("unchecked")
    public void testNotSupportPrimitiveSchema() {
        BaseJdbcAutoSchemaSink baseJdbcAutoSchemaSink = new BaseJdbcAutoSchemaSink() {
            @Override
            protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue,
                                            String targetSqlType) throws Exception {
                throw new UnsupportedOperationException("Array handling not implemented in test");
            }
        };
        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(org.apache.pulsar.client.api.Schema.STRING);
        Record<? extends GenericObject> record = new Record<GenericRecord>() {
            @Override
            public org.apache.pulsar.client.api.Schema<GenericRecord> getSchema() {
                return autoConsumeSchema;
            }

            @Override
            public GenericRecord getValue() {
                return null;
            }
        };
        baseJdbcAutoSchemaSink.createMutation((Record<GenericObject>) record);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testSubFieldJsonArray() throws Exception {
        BaseJdbcAutoSchemaSink baseJdbcAutoSchemaSink = new BaseJdbcAutoSchemaSink() {
            @Override
            protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue,
                                            String targetSqlType) throws Exception {
                throw new UnsupportedOperationException("Array handling not implemented in test");
            }
        };

        Field field = JdbcAbstractSink.class.getDeclaredField("jdbcSinkConfig");
        field.setAccessible(true);
        JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig();
        jdbcSinkConfig.setNullValueAction(JdbcSinkConfig.NullValueAction.FAIL);
        field.set(baseJdbcAutoSchemaSink, jdbcSinkConfig);

        TStates tStates = new TStates("tstats", Arrays.asList(
                new PC("brand1", "model1"),
                new PC("brand2", "model2")
        ));
        org.apache.pulsar.client.api.Schema<TStates> jsonSchema =
                org.apache.pulsar.client.api.Schema.JSON(TStates.class);
        GenericJsonSchema genericJsonSchema = new GenericJsonSchema(jsonSchema.getSchemaInfo());
        byte[] encode = jsonSchema.encode(tStates);
        GenericRecord genericRecord = genericJsonSchema.decode(encode);

        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(org.apache.pulsar.client.api.Schema.JSON(TStates.class));
        Record<? extends GenericObject> record = new Record<GenericRecord>() {
            @Override
            public org.apache.pulsar.client.api.Schema<GenericRecord> getSchema() {
                return genericJsonSchema;
            }

            @Override
            public GenericRecord getValue() {
                return genericRecord;
            }
        };
        JdbcAbstractSink.Mutation mutation = baseJdbcAutoSchemaSink.createMutation((Record<GenericObject>) record);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        baseJdbcAutoSchemaSink.setColumnValue(mockPreparedStatement, 0, mutation.getValues().apply("state"));
        baseJdbcAutoSchemaSink.setColumnValue(mockPreparedStatement, 1, mutation.getValues().apply("pcList"));
        verify(mockPreparedStatement).setString(0, "tstats");
        verify(mockPreparedStatement).setString(1,
                "[{\"brand\":\"brand1\",\"model\":\"model1\"},{\"brand\":\"brand2\",\"model\":\"model2\"}]");
    }

    @Test
    public void testBackwardCompatibilityNonArrayFunctionality() throws Exception {
        // Test that all existing non-array functionality still works after array support addition
        BaseJdbcAutoSchemaSink baseJdbcAutoSchemaSink = new BaseJdbcAutoSchemaSink() {
            @Override
            protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue,
                                            String targetSqlType) throws Exception {
                throw new UnsupportedOperationException("Array handling not implemented in test");
            }
        };

        // Test all primitive type conversions still work
        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField("test",
                createFieldAndGetSchema(builder -> builder.name("field").type().stringType().noDefault())), "test");

        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField(42,
                createFieldAndGetSchema(builder -> builder.name("field").type().intType().noDefault())), 42);

        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField(true,
                createFieldAndGetSchema(builder -> builder.name("field").type().booleanType().noDefault())), true);

        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField(3.14,
                createFieldAndGetSchema(builder -> builder.name("field").type().doubleType().noDefault())), 3.14);

        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField(2.5f,
                createFieldAndGetSchema(builder -> builder.name("field").type().floatType().noDefault())), 2.5f);

        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField(100L,
                createFieldAndGetSchema(builder -> builder.name("field").type().longType().noDefault())), 100L);

        // Test null handling still works
        Assert.assertNull(BaseJdbcAutoSchemaSink.convertAvroField(null,
                createFieldAndGetSchema(builder -> builder.name("field").type().stringType().noDefault())));

        // Test enum handling still works
        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField("OPTION1",
                createFieldAndGetSchema(builder -> builder.name("field").type()
                        .enumeration("TestEnum").symbols("OPTION1", "OPTION2").noDefault())), "OPTION1");

        // Test union handling still works
        Assert.assertEquals(BaseJdbcAutoSchemaSink.convertAvroField(123,
                createFieldAndGetSchema(builder -> builder.name("field").type()
                        .unionOf().intType().endUnion().noDefault())), 123);
    }

    @Test
    public void testSetColumnValueWithoutTargetSqlTypeStillWorks() throws Exception {
        BaseJdbcAutoSchemaSink baseJdbcAutoSchemaSink = new BaseJdbcAutoSchemaSink() {
            @Override
            protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue,
                                            String targetSqlType) throws Exception {
                throw new UnsupportedOperationException("Array handling not implemented in test");
            }
        };

        PreparedStatement mockStatement = mock(PreparedStatement.class);

        // Test that the old setColumnValue method still works for non-array values
        baseJdbcAutoSchemaSink.setColumnValue(mockStatement, 1, "test string");
        baseJdbcAutoSchemaSink.setColumnValue(mockStatement, 2, 42);
        baseJdbcAutoSchemaSink.setColumnValue(mockStatement, 3, true);
        baseJdbcAutoSchemaSink.setColumnValue(mockStatement, 4, null);

        // Verify the calls were made
        verify(mockStatement).setString(1, "test string");
        verify(mockStatement).setInt(2, 42);
        verify(mockStatement).setBoolean(3, true);
        verify(mockStatement).setNull(4, java.sql.Types.NULL);
    }

    @Test(expectedExceptions = Exception.class,
            expectedExceptionsMessageRegExp = "Array values require targetSqlType parameter.*")
    public void testSetColumnValueWithArrayThrowsExceptionWithoutTargetSqlType() throws Exception {
        BaseJdbcAutoSchemaSink baseJdbcAutoSchemaSink = new BaseJdbcAutoSchemaSink() {
            @Override
            protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue,
                                            String targetSqlType) throws Exception {
                throw new UnsupportedOperationException("Array handling not implemented in test");
            }
        };

        PreparedStatement mockStatement = mock(PreparedStatement.class);
        Schema arraySchema = createFieldAndGetSchema(builder ->
                builder.name("field").type().array().items().stringType().noDefault());
        GenericData.Array<String> testArray = new GenericData.Array<>(1, arraySchema);
        testArray.add("test");

        // This should throw an exception because arrays require targetSqlType
        baseJdbcAutoSchemaSink.setColumnValue(mockStatement, 1, testArray);
    }

    private static final String JSON_SCHEMA_DEFINITION = "{\"type\":\"record\",\"name\":\"Row\",\"namespace\":\"test\","
            + "\"fields\":["
            + "{\"name\":\"str\",\"type\":\"string\"},"
            + "{\"name\":\"nullableStr\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"missing\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"bool\",\"type\":\"boolean\"},"
            + "{\"name\":\"intField\",\"type\":\"int\"},"
            + "{\"name\":\"longField\",\"type\":\"long\"},"
            + "{\"name\":\"bigInt\",\"type\":\"long\"},"
            + "{\"name\":\"doubleField\",\"type\":\"double\"},"
            + "{\"name\":\"floatField\",\"type\":\"float\"},"
            + "{\"name\":\"decimalBytes\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":10,\"scale\":2}]},"
            + "{\"name\":\"nullDecimalBytes\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":10,\"scale\":2}]},"
            + "{\"name\":\"invalidDecimalBytes\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":10,\"scale\":2}]},"
            + "{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":10,\"scale\":2}},"
            + "{\"name\":\"objectBytes\",\"type\":[\"null\",{\"type\":\"bytes\"}]},"
            + "{\"name\":\"nullableBytes\",\"type\":[\"null\",\"bytes\"]},"
            + "{\"name\":\"plainBytes\",\"type\":\"bytes\"},"
            + "{\"name\":\"nested\",\"type\":{\"type\":\"record\",\"name\":\"Nested\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"},{\"name\":\"b\",\"type\":[\"null\",{\"type\":\"bytes\","
            + "\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}]}]}},"
            + "{\"name\":\"array\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}"
            + "]}";

    private static final String JSON_VALUE = "{"
            + "\"str\":\"text\","
            + "\"nullableStr\":null,"
            + "\"bool\":true,"
            + "\"intField\":42,"
            + "\"longField\":9876543210,"
            + "\"bigInt\":123456789012345678901234567890,"
            + "\"doubleField\":3.141592653589793,"
            + "\"floatField\":2.5,"
            + "\"decimalBytes\":\"AQI=\","
            + "\"nullDecimalBytes\":null,"
            + "\"invalidDecimalBytes\":\"not base64!\","
            + "\"decimal\":\"AQI=\","
            + "\"objectBytes\":\"AwQ=\","
            + "\"nullableBytes\":\"BQY=\","
            + "\"plainBytes\":\"Bwg=\","
            + "\"nested\":{\"a\":\"x\",\"b\":\"AQI=\"},"
            + "\"array\":[\"a\",\"b\"],"
            + "\"notInSchema\":\"extra\""
            + "}";

    private static final List<String> JSON_CONTAINER_FIELDS = Arrays.asList("nested", "array");

    private static final List<String> JSON_TEXTUAL_BINARY_FIELDS = Arrays.asList("decimalBytes", "invalidDecimalBytes");

    private static final List<String> JSON_SCALAR_FIELDS = Arrays.asList("str", "nullableStr", "missing", "bool",
            "intField", "longField", "bigInt", "doubleField", "floatField", "decimalBytes", "nullDecimalBytes",
            "invalidDecimalBytes", "decimal", "objectBytes", "nullableBytes", "plainBytes", "notInSchema",
            "unknown");

    @Test
    public void testJsonFieldValuesMatchGetField() throws Exception {
        BaseJdbcAutoSchemaSink sink = createSink();
        GenericJsonSchema schema = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericRecord genericRecord = schema.decode(JSON_VALUE.getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(genericRecord instanceof GenericJsonRecord);

        Function<String, Object> values = sink.createMutation(jsonRecord(schema, genericRecord)).getValues();

        List<String> fields = new ArrayList<>(JSON_SCALAR_FIELDS);
        fields.addAll(JSON_CONTAINER_FIELDS);
        for (String field : fields) {
            Object expected = genericRecord.getField(field);
            Object actual = values.apply(field);
            if (expected == null) {
                Assert.assertNull(actual, field);
            } else if (expected instanceof byte[]) {
                Assert.assertTrue(actual instanceof byte[], field);
                Assert.assertEquals((byte[]) actual, (byte[]) expected, field);
            } else if (expected instanceof GenericJsonRecord) {
                Assert.assertTrue(actual instanceof GenericJsonRecord, field);
                Assert.assertEquals(((GenericJsonRecord) actual).getJsonNode().toString(),
                        ((GenericJsonRecord) expected).getJsonNode().toString(), field);
            } else {
                Assert.assertEquals(actual.getClass(), expected.getClass(), field);
                Assert.assertEquals(actual, expected, field);
            }
        }

        Assert.assertEquals(values.apply("decimalBytes"), new byte[]{1, 2});
        Assert.assertNull(values.apply("nullDecimalBytes"));
        Assert.assertEquals(values.apply("invalidDecimalBytes"), "not base64!");
        Assert.assertEquals(values.apply("decimal"), "AQI=");
        Assert.assertEquals(values.apply("nullableBytes"), "BQY=");
        Assert.assertEquals(values.apply("bigInt"), "123456789012345678901234567890");
        Assert.assertEquals(values.apply("floatField"), 2.5d);
        Assert.assertEquals(sink.getJsonBinaryFields(jsonRecord(schema, genericRecord)),
                Set.of("decimalBytes", "nullDecimalBytes", "invalidDecimalBytes"));
    }

    @Test
    public void testJsonFieldValuesDoNotUseGetFieldForScalars() throws Exception {
        BaseJdbcAutoSchemaSink sink = createSink();
        GenericJsonSchema schema = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericRecord genericRecord = spy(schema.decode(JSON_VALUE.getBytes(StandardCharsets.UTF_8)));

        Function<String, Object> values = sink.createMutation(jsonRecord(schema, genericRecord)).getValues();
        for (String field : JSON_SCALAR_FIELDS) {
            values.apply(field);
        }
        for (String field : JSON_CONTAINER_FIELDS) {
            Assert.assertTrue(values.apply(field) instanceof GenericJsonRecord, field);
        }

        List<String> delegated = new ArrayList<>(JSON_CONTAINER_FIELDS);
        delegated.addAll(JSON_TEXTUAL_BINARY_FIELDS);
        verify(genericRecord, never()).getField(argThat((String field) -> !delegated.contains(field)));
        for (String field : delegated) {
            verify(genericRecord).getField(field);
        }
    }

    @Test
    public void testJsonRecordWithoutSchemaInfoReadsBinaryFieldsAsText() throws Exception {
        BaseJdbcAutoSchemaSink sink = createSink();
        GenericJsonSchema schema = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericRecord genericRecord = schema.newRecordBuilder()
                .set("str", "text")
                .set("decimalBytes", "AQI=")
                .build();

        Function<String, Object> values = sink.createMutation(jsonRecord(schema, genericRecord)).getValues();

        Assert.assertEquals(genericRecord.getField("decimalBytes"), "AQI=");
        Assert.assertEquals(values.apply("decimalBytes"), "AQI=");
        Assert.assertEquals(values.apply("str"), "text");
    }

    @Test
    public void testJsonBinaryFieldsAreCachedPerSchema() throws Exception {
        BaseJdbcAutoSchemaSink sink = createSink();
        GenericJsonSchema schemaV1 = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericJsonSchema schemaV2 = new GenericJsonSchema(jsonSchemaInfo("{\"type\":\"record\",\"name\":\"Row\","
                + "\"fields\":[{\"name\":\"str\",\"type\":[\"null\",{\"type\":\"bytes\",\"connect.name\":\"x\"}]}]}"));
        GenericJsonSchema otherTopicV1 = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericRecord recordV1 = schemaV1.decode(JSON_VALUE.getBytes(StandardCharsets.UTF_8));
        GenericRecord recordV2 = schemaV2.decode("{\"str\":\"AQI=\"}".getBytes(StandardCharsets.UTF_8));

        Set<String> binaryFieldsV1 = sink.getJsonBinaryFields(jsonRecord(schemaV1, recordV1));
        Assert.assertSame(sink.getJsonBinaryFields(jsonRecord(schemaV1, recordV1)), binaryFieldsV1);

        Set<String> binaryFieldsV2 = sink.getJsonBinaryFields(jsonRecord(schemaV2, recordV2));
        Assert.assertEquals(binaryFieldsV2, Set.of("str"));
        Assert.assertSame(sink.getJsonBinaryFields(jsonRecord(schemaV2, recordV2)), binaryFieldsV2);
        Assert.assertSame(sink.getJsonBinaryFields(jsonRecord(schemaV1, recordV1)), binaryFieldsV1);
        Assert.assertEquals(binaryFieldsV1, Set.of("decimalBytes", "nullDecimalBytes", "invalidDecimalBytes"));
        Assert.assertSame(sink.getJsonBinaryFields(jsonRecord(otherTopicV1, recordV1)), binaryFieldsV1);

        Assert.assertEquals(sink.createMutation(jsonRecord(schemaV2, recordV2)).getValues().apply("str"),
                new byte[]{1, 2});
    }

    @Test
    public void testAutoConsumeSchemaUsesGetField() throws Exception {
        BaseJdbcAutoSchemaSink sink = createSink();
        GenericJsonSchema schema = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericRecord genericRecord = spy(schema.decode(JSON_VALUE.getBytes(StandardCharsets.UTF_8)));
        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(schema);

        Assert.assertEquals(sink.createMutation(jsonRecord(autoConsumeSchema, genericRecord)).getValues().apply("str"),
                "text");
        verify(genericRecord).getField("str");
    }

    @Test
    public void testJsonRecordWithoutJsonNodeUsesGetField() throws Exception {
        BaseJdbcAutoSchemaSink sink = createSink();
        GenericJsonSchema schema = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericRecord genericRecord = mock(GenericRecord.class);
        when(genericRecord.getNativeObject()).thenReturn("not a JsonNode");
        when(genericRecord.getField("str")).thenReturn("value");

        Assert.assertEquals(sink.createMutation(jsonRecord(schema, genericRecord)).getValues().apply("str"), "value");
        verify(genericRecord).getField("str");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUnparseableJsonSchemaUsesGetField() throws Exception {
        BaseJdbcAutoSchemaSink sink = createSink();
        GenericJsonSchema schema = new GenericJsonSchema(jsonSchemaInfo(JSON_SCHEMA_DEFINITION));
        GenericRecord genericRecord = spy(schema.decode(JSON_VALUE.getBytes(StandardCharsets.UTF_8)));
        org.apache.pulsar.client.api.Schema<GenericRecord> unparseableSchema =
                mock(org.apache.pulsar.client.api.Schema.class);
        when(unparseableSchema.getSchemaInfo()).thenReturn(jsonSchemaInfo("{\"type\":\"record\""));
        Record<GenericObject> record = jsonRecord(unparseableSchema, genericRecord);

        Assert.assertNull(sink.getJsonBinaryFields(record));
        Assert.assertNull(sink.getJsonBinaryFields(record));
        Assert.assertEquals(sink.createMutation(record).getValues().apply("str"), "text");
        verify(genericRecord).getField("str");
    }

    private static BaseJdbcAutoSchemaSink createSink() throws Exception {
        BaseJdbcAutoSchemaSink sink = new BaseJdbcAutoSchemaSink() {
            @Override
            protected void handleArrayValue(PreparedStatement statement, int index, Object arrayValue,
                                            String targetSqlType) throws Exception {
                throw new UnsupportedOperationException("Array handling not implemented in test");
            }
        };
        Field field = JdbcAbstractSink.class.getDeclaredField("jdbcSinkConfig");
        field.setAccessible(true);
        field.set(sink, new JdbcSinkConfig());
        return sink;
    }

    private static SchemaInfo jsonSchemaInfo(String definition) {
        return SchemaInfo.builder()
                .name("test")
                .type(SchemaType.JSON)
                .schema(definition.getBytes(StandardCharsets.UTF_8))
                .properties(Collections.emptyMap())
                .build();
    }

    @SuppressWarnings("unchecked")
    private static Record<GenericObject> jsonRecord(org.apache.pulsar.client.api.Schema<GenericRecord> schema,
                                                    GenericRecord value) {
        Record<? extends GenericObject> record = new Record<GenericRecord>() {
            @Override
            public org.apache.pulsar.client.api.Schema<GenericRecord> getSchema() {
                return schema;
            }

            @Override
            public GenericRecord getValue() {
                return value;
            }
        };
        return (Record<GenericObject>) record;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class TStates {
        public String state;
        public List<PC> pcList;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class PC {
        public String brand;
        public String model;
    }


}