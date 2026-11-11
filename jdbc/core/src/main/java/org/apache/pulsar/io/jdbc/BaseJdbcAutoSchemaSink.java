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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.jdbc.JdbcUtils.ColumnId;

/**
 * An abstract Jdbc sink, which interprets input Record in generic record.
 */
@Slf4j
public abstract class BaseJdbcAutoSchemaSink extends JdbcAbstractSink<GenericObject> {

    private static final int MAX_CACHED_JSON_SCHEMAS = 64;
    private static final ObjectMapper SCHEMA_MAPPER = new ObjectMapper();

    private volatile JsonBinaryFields[] jsonBinaryFieldsCache = new JsonBinaryFields[0];
    private final Map<String, Set<String>> jsonBinaryFieldsByDefinition = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Set<String>> eldest) {
            return size() > MAX_CACHED_JSON_SCHEMAS;
        }
    };

    @Override
    public String generateUpsertQueryStatement() {
        throw new IllegalStateException("UPSERT not supported");
    }

    @Override
    public List<ColumnId> getColumnsForUpsert() {
        throw new IllegalStateException("UPSERT not supported");
    }

    /**
     * Handles array value binding for database-specific array types.
     * <p>
     * This method is called when an array value needs to be bound to a PreparedStatement
     * parameter. Implementations should convert the array value to the appropriate
     * database-specific array type and bind it to the statement using the appropriate
     * JDBC method (typically {@code PreparedStatement.setArray()}).
     * </p>
     * <p>
     * The method is invoked automatically by {@link #setColumnValue(PreparedStatement, int, Object, String)}
     * when it detects an array type (specifically {@code org.apache.avro.generic.GenericData$Array}).
     * </p>
     * <p>
     * <strong>Implementation Guidelines:</strong>
     * <ul>
     * <li>Handle null arrays by calling {@code statement.setNull(index, java.sql.Types.ARRAY)}</li>
     * <li>Convert array elements to the appropriate database-specific types</li>
     * <li>Use the targetSqlType parameter to determine the correct array element type</li>
     * <li>Provide descriptive error messages for type mismatches and unsupported types</li>
     * <li>Wrap JDBC exceptions with contextual information</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Example Usage:</strong>
     * <pre>{@code
     * // For PostgreSQL implementation:
     * if (arrayValue == null) {
     *     statement.setNull(index, java.sql.Types.ARRAY);
     *     return;
     * }
     *
     * Object[] elements = convertToObjectArray(arrayValue);
     * String postgresType = mapToPostgresType(targetSqlType);
     * Array pgArray = connection.createArrayOf(postgresType, elements);
     * statement.setArray(index, pgArray);
     * }</pre>
     * </p>
     *
     * @param statement     the PreparedStatement to bind the array value to
     * @param index         the parameter index (1-based) in the PreparedStatement
     * @param arrayValue    the array value to be bound, typically a {@code GenericData.Array} or {@code Object[]}
     * @param targetSqlType the target SQL type name for the array column (e.g., "integer", "text", "_int4")
     * @throws Exception if array conversion or binding fails, including:
     *                   <ul>
     *                   <li>{@code IllegalArgumentException} for unsupported array types or type mismatches</li>
     *                   <li>{@code SQLException} for JDBC array creation or binding failures</li>
     *                   <li>{@code UnsupportedOperationException} for databases that don't support arrays</li>
     *                   </ul>
     * @see #setColumnValue(PreparedStatement, int, Object, String)
     * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
     * @see java.sql.Connection#createArrayOf(String, Object[])
     */
    protected abstract void handleArrayValue(PreparedStatement statement, int index, Object arrayValue,
                                             String targetSqlType) throws Exception;

    @Override
    public void bindValue(PreparedStatement statement, Mutation mutation) throws Exception {
        final List<ColumnId> columns = new ArrayList<>();
        switch (mutation.getType()) {
            case INSERT:
                columns.addAll(tableDefinition.getColumns());
                break;
            case UPSERT:
                columns.addAll(getColumnsForUpsert());
                break;
            case UPDATE:
                columns.addAll(tableDefinition.getNonKeyColumns());
                columns.addAll(tableDefinition.getKeyColumns());
                break;
            case DELETE:
                columns.addAll(tableDefinition.getKeyColumns());
                break;
        }

        int index = 1;
        for (ColumnId columnId : columns) {
            String colName = columnId.getName();
            int colType = columnId.getType();
            String typeName = columnId.getTypeName();
            if (log.isDebugEnabled()) {
                log.debug("getting value for column: {} type: {}", colName, colType);
            }
            try {
                Object obj = mutation.getValues().apply(colName);
                if (obj != null) {
                    setColumnValue(statement, index++, obj, typeName);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Column {} is null", colName);
                    }
                    setColumnNull(statement, index++, colType);
                }
            } catch (NullPointerException e) {
                // With JSON schema field is omitted, so get NPE
                // In this case we want to set column to Null
                if (log.isDebugEnabled()) {
                    log.debug("Column {} is null", colName);
                }
                setColumnNull(statement, index++, colType);
            }
        }
    }

    @Override
    public Mutation createMutation(Record<GenericObject> message) {
        final GenericObject record = message.getValue();
        Function<String, Object> recordValueGetter;
        MutationType mutationType = null;
        if (message.getSchema() != null && message.getSchema() instanceof KeyValueSchema) {
            KeyValueSchema<GenericObject, GenericObject> keyValueSchema = (KeyValueSchema) message.getSchema();

            final org.apache.pulsar.client.api.Schema<GenericObject> keySchema = keyValueSchema.getKeySchema();
            final org.apache.pulsar.client.api.Schema<GenericObject> valueSchema = keyValueSchema.getValueSchema();
            KeyValue<GenericObject, GenericObject> keyValue =
                    (KeyValue<GenericObject, GenericObject>) record.getNativeObject();

            final GenericObject key = keyValue.getKey();
            final GenericObject value = keyValue.getValue();

            boolean isDelete = false;
            if (value == null) {
                switch (jdbcSinkConfig.getNullValueAction()) {
                    case DELETE:
                        isDelete = true;
                        break;
                    case FAIL:
                        throw new IllegalArgumentException("Got record with value NULL with nullValueAction=FAIL");
                    default:
                        break;
                }
            }
            Map<String, Object> data = new HashMap<>();
            fillKeyValueSchemaData(keySchema, key, data);
            if (isDelete) {
                mutationType = MutationType.DELETE;
            } else {
                fillKeyValueSchemaData(valueSchema, value, data);
            }
            recordValueGetter = (k) -> data.get(k);
        } else {
            SchemaType schemaType = message.getSchema().getSchemaInfo().getType();
            if (schemaType.isPrimitive()) {
                throw new UnsupportedOperationException("Primitive schema is not supported: " + schemaType);
            }
            recordValueGetter = schemaType == SchemaType.JSON ? createJsonValueGetter(message, record) : null;
            if (recordValueGetter == null) {
                recordValueGetter = (key) -> ((GenericRecord) record).getField(key);
            }
        }
        String action = message.getProperties().get(ACTION_PROPERTY);
        if (action != null) {
            mutationType = MutationType.valueOf(action);
        } else if (mutationType == null) {
            switch (jdbcSinkConfig.getInsertMode()) {
                case INSERT:
                    mutationType = MutationType.INSERT;
                    break;
                case UPSERT:
                    mutationType = MutationType.UPSERT;
                    break;
                case UPDATE:
                    mutationType = MutationType.UPDATE;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown insert mode: " + jdbcSinkConfig.getInsertMode());
            }
        }
        return new Mutation(mutationType, recordValueGetter);
    }

    private Function<String, Object> createJsonValueGetter(Record<GenericObject> message, GenericObject value) {
        final org.apache.pulsar.client.api.Schema<?> schema = message.getSchema();
        if (schema instanceof AutoConsumeSchema
                || !(value instanceof GenericRecord)
                || !(value.getNativeObject() instanceof JsonNode)) {
            return null;
        }
        final Set<String> binaryFields = getJsonBinaryFields(message);
        if (binaryFields == null) {
            return null;
        }
        final GenericRecord record = (GenericRecord) value;
        final JsonNode jsonNode = (JsonNode) value.getNativeObject();
        return (key) -> getJsonFieldValue(record, jsonNode, binaryFields, key);
    }

    /**
     * Returns the names of the fields that {@code GenericJsonRecord.getField} may read as binary for the schema of
     * the given record, or {@code null} if the schema definition cannot be parsed. The result is cached per schema
     * instance (one per topic and schema version) and per schema definition, so that a schema definition is parsed
     * once instead of once per field read.
     */
    @VisibleForTesting
    Set<String> getJsonBinaryFields(Record<GenericObject> message) {
        final SchemaInfo schemaInfo = message.getSchema().getSchemaInfo();
        for (JsonBinaryFields entry : jsonBinaryFieldsCache) {
            if (entry.schemaInfo() == schemaInfo) {
                return entry.binaryFields();
            }
        }
        synchronized (jsonBinaryFieldsByDefinition) {
            final String definition = schemaInfo.getSchemaDefinition();
            final Set<String> binaryFields;
            if (jsonBinaryFieldsByDefinition.containsKey(definition)) {
                binaryFields = jsonBinaryFieldsByDefinition.get(definition);
            } else {
                binaryFields = computeJsonBinaryFields(message, schemaInfo, definition);
                jsonBinaryFieldsByDefinition.put(definition, binaryFields);
            }
            final JsonBinaryFields[] cache = jsonBinaryFieldsCache;
            final int retained = Math.min(cache.length, MAX_CACHED_JSON_SCHEMAS - 1);
            final JsonBinaryFields[] updated = new JsonBinaryFields[retained + 1];
            updated[0] = new JsonBinaryFields(schemaInfo, binaryFields);
            System.arraycopy(cache, 0, updated, 1, retained);
            jsonBinaryFieldsCache = updated;
            return binaryFields;
        }
    }

    private static Set<String> computeJsonBinaryFields(Record<GenericObject> message, SchemaInfo schemaInfo,
                                                       String definition) {
        final String topic = message.getTopicName().orElse(null);
        try {
            final Set<String> binaryFields = parseJsonBinaryFields(definition);
            log.info("Reading fields of JSON records from topic {} with schema {} from the JSON tree, "
                    + "binary fields: {}", topic, schemaInfo.getName(), binaryFields.size());
            return binaryFields;
        } catch (Exception e) {
            log.warn("Cannot parse JSON schema {} of topic {}, reading fields of its records with "
                    + "GenericRecord.getField", schemaInfo.getName(), topic, e);
            return null;
        }
    }

    /**
     * Applies the rule of {@code GenericJsonRecord.isBinaryValue} to every field of the schema.
     */
    private static Set<String> parseJsonBinaryFields(String schemaDefinition) throws Exception {
        final Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        final Schema schema = parser.parse(schemaDefinition);
        final Set<String> binaryFields = new HashSet<>();
        for (Schema.Field field : schema.getFields()) {
            final JsonNode fieldSchema = SCHEMA_MAPPER.readTree(field.schema().toString());
            for (JsonNode node : fieldSchema) {
                final JsonNode type = node.get("type");
                if (type != null && ("bytes".equals(type.asText()) || "byte".equals(type.asText()))) {
                    binaryFields.add(field.name());
                    break;
                }
            }
        }
        return Set.copyOf(binaryFields);
    }

    /**
     * Returns the same value as {@code GenericJsonRecord.getField}. Containers and textual values of fields that
     * may be binary are read with {@code getField}, which decides with the schema the record carries; other values
     * are read from the JSON tree without parsing the schema definition.
     */
    private static Object getJsonFieldValue(GenericRecord record, JsonNode jsonNode, Set<String> binaryFields,
                                            String fieldName) {
        final JsonNode fn = jsonNode.get(fieldName);
        if (fn == null) {
            return null;
        }
        if (fn.isContainerNode()) {
            return record.getField(fieldName);
        } else if (fn.isBoolean()) {
            return fn.asBoolean();
        } else if (fn.isFloatingPointNumber()) {
            return fn.asDouble();
        } else if (fn.isBigInteger()) {
            if (fn.canConvertToLong()) {
                return fn.asLong();
            } else {
                return fn.asText();
            }
        } else if (fn.isNumber()) {
            return fn.numberValue();
        } else if (fn.isBinary()) {
            try {
                return fn.binaryValue();
            } catch (IOException e) {
                return fn.asText();
            }
        } else if (fn.isNull()) {
            return null;
        } else if (binaryFields.contains(fieldName)) {
            return record.getField(fieldName);
        } else {
            return fn.asText();
        }
    }

    private record JsonBinaryFields(SchemaInfo schemaInfo, Set<String> binaryFields) {
    }

    private static void setColumnNull(PreparedStatement statement, int index, int type) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Setting column value to null, statement: {}, index: {}", statement.toString(), index);
        }
        statement.setNull(index, type);

    }

    /**
     * Sets a column value in a PreparedStatement, handling various data types including arrays.
     * <p>
     * This method automatically detects the type of the value and calls the appropriate
     * PreparedStatement setter method. It supports primitive types, strings, and arrays.
     * Array support is implemented through the {@link #handleArrayValue(PreparedStatement, int, Object, String)}
     * method, which delegates to database-specific implementations.
     * </p>
     * <p>
     * <strong>Supported Types:</strong>
     * <ul>
     * <li>Primitive types: Integer, Long, Double, Float, Boolean, Short</li>
     * <li>String types: String</li>
     * <li>Binary types: ByteString</li>
     * <li>JSON types: GenericJsonRecord</li>
     * <li>Array types: GenericData.Array (requires targetSqlType parameter)</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Array Handling:</strong>
     * When an array is detected (GenericData.Array), this method calls the abstract
     * {@link #handleArrayValue(PreparedStatement, int, Object, String)} method, which
     * must be implemented by database-specific subclasses. The targetSqlType parameter
     * is essential for proper array type conversion.
     * </p>
     * <p>
     * <strong>Example Usage:</strong>
     * <pre>{@code
     * // Setting a primitive value
     * setColumnValue(statement, 1, 42, "integer");
     *
     * // Setting an array value (requires database-specific implementation)
     * GenericData.Array<Integer> intArray = ...;
     * setColumnValue(statement, 2, intArray, "integer");
     * }</pre>
     * </p>
     *
     * @param statement     the PreparedStatement to bind the value to
     * @param index         the parameter index (1-based) in the PreparedStatement
     * @param value         the value to be bound (null values are handled automatically)
     * @param targetSqlType the target SQL type name for the column, required for array types
     * @throws Exception if value binding fails, including:
     *                   <ul>
     *                   <li>Unsupported value types</li>
     *                   <li>Array conversion failures (delegated to handleArrayValue)</li>
     *                   <li>JDBC binding errors</li>
     *                   </ul>
     * @see #handleArrayValue(PreparedStatement, int, Object, String)
     * @see #setColumnValue(PreparedStatement, int, Object)
     */
    protected void setColumnValue(PreparedStatement statement, int index, Object value,
                                  String targetSqlType) throws Exception {

        log.debug("Setting column value, statement: {}, index: {}, value: {}", statement, index, value);

        // Handle null values first
        if (value == null) {
            setColumnNull(statement, index, java.sql.Types.NULL);
            return;
        }

        // Check for array types first, before other type checks
        if (value instanceof GenericData.Array || value instanceof Object[]) {
            handleArrayValue(statement, index, value, targetSqlType);
            return;
        }

        if (value instanceof Integer) {
            statement.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            statement.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            statement.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            statement.setFloat(index, (Float) value);
        } else if (value instanceof Boolean) {
            statement.setBoolean(index, (Boolean) value);
        } else if (value instanceof String) {
            statement.setString(index, (String) value);
        } else if (value instanceof Short) {
            statement.setShort(index, (Short) value);
        } else if (value instanceof ByteString) {
            statement.setBytes(index, ((ByteString) value).toByteArray());
        } else if (value instanceof GenericJsonRecord) {
            statement.setString(index, ((GenericJsonRecord) value).getJsonNode().toString());
        } else {
            throw new Exception("Not supported value type, need to add it. " + value.getClass());
        }
    }

    /**
     * Backward compatibility method for setColumnValue without targetSqlType parameter.
     * This method is provided for compatibility with existing code that may call setColumnValue
     * without the targetSqlType parameter. Arrays will not be supported when using this method.
     *
     * @param statement the PreparedStatement to bind the value to
     * @param index     the parameter index (1-based) in the PreparedStatement
     * @param value     the value to be bound
     * @throws Exception if value binding fails or if an array is encountered (arrays require targetSqlType)
     */
    protected void setColumnValue(PreparedStatement statement, int index, Object value) throws Exception {
        if (value instanceof GenericData.Array) {
            throw new Exception("Array values require targetSqlType parameter. "
                    + "Use setColumnValue(statement, index, value, targetSqlType) instead.");
        }
        setColumnValue(statement, index, value, null);
    }

    private static Object getValueFromJsonNode(final JsonNode fn) {
        if (fn == null || fn.isNull()) {
            return null;
        }
        if (fn.isContainerNode()) {
            throw new IllegalArgumentException("Container nodes are not supported, the JSON must contains only "
                    + "first level fields.");
        } else if (fn.isBoolean()) {
            return fn.asBoolean();
        } else if (fn.isFloatingPointNumber()) {
            return fn.asDouble();
        } else if (fn.isBigInteger()) {
            if (fn.canConvertToLong()) {
                return fn.asLong();
            } else {
                return fn.asText();
            }
        } else if (fn.isNumber()) {
            return fn.numberValue();
        } else {
            return fn.asText();
        }
    }

    private static void fillKeyValueSchemaData(org.apache.pulsar.client.api.Schema<GenericObject> schema,
                                               GenericObject record,
                                               Map<String, Object> data) {
        if (record == null) {
            return;
        }
        switch (schema.getSchemaInfo().getType()) {
            case JSON:
                final JsonNode jsonNode = (JsonNode) record.getNativeObject();
                final Iterator<String> fieldNames = jsonNode.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    final JsonNode nodeValue = jsonNode.get(fieldName);
                    data.put(fieldName, getValueFromJsonNode(nodeValue));
                }
                break;
            case AVRO:
                org.apache.avro.generic.GenericRecord avroNode =
                        (org.apache.avro.generic.GenericRecord) record.getNativeObject();
                for (Schema.Field field : avroNode.getSchema().getFields()) {
                    final String fieldName = field.name();
                    data.put(fieldName, convertAvroField(avroNode.get(fieldName), field.schema()));
                }
                break;
            default:
                throw new IllegalArgumentException("unexpected schema type: "
                        + schema.getSchemaInfo().getType()
                        + " with KeyValueSchema");
        }
    }

    /**
     * Converts an Avro field value to a Java object suitable for JDBC binding.
     * <p>
     * This method handles the conversion of various Avro schema types to their corresponding
     * Java representations. It supports primitive types, strings, unions, and arrays.
     * Array conversion is performed recursively, processing each array element according
     * to the array's element schema.
     * </p>
     * <p>
     * <strong>Supported Avro Types:</strong>
     * <ul>
     * <li>Primitive types: NULL, INT, LONG, DOUBLE, FLOAT, BOOLEAN</li>
     * <li>String types: STRING, ENUM</li>
     * <li>Union types: Automatically selects the non-null type from the union</li>
     * <li>Array types: Recursively converts array elements to Object[]</li>
     * </ul>
     * </p>
     * <p>
     * <strong>Array Conversion:</strong>
     * Arrays are converted by recursively processing each element according to the
     * array's element schema. The result is an Object[] that can be further processed
     * by database-specific array handling methods.
     * </p>
     * <p>
     * <strong>Example Usage:</strong>
     * <pre>{@code
     * // Convert a simple integer field
     * Schema intSchema = Schema.create(Schema.Type.INT);
     * Object result = convertAvroField(42, intSchema); // Returns Integer(42)
     *
     * // Convert an array field
     * Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
     * GenericData.Array<String> avroArray = ...;
     * Object result = convertAvroField(avroArray, arraySchema); // Returns String[]
     * }</pre>
     * </p>
     *
     * @param avroValue the Avro value to convert (may be null)
     * @param schema    the Avro schema describing the value's type
     * @return the converted Java object, or null if avroValue is null
     * @throws IllegalArgumentException      if the avroValue doesn't match the expected schema type
     * @throws UnsupportedOperationException if the schema type is not supported
     * @see org.apache.avro.Schema.Type
     * @see org.apache.avro.generic.GenericData.Array
     */
    @VisibleForTesting
    static Object convertAvroField(Object avroValue, Schema schema) {
        if (avroValue == null) {
            return null;
        }
        switch (schema.getType()) {
            case NULL:
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
                return avroValue;
            case ENUM:
            case STRING:
                return avroValue.toString(); // can be a String or org.apache.avro.util.Utf8
            case UNION:
                for (Schema s : schema.getTypes()) {
                    if (s.getType() == Schema.Type.NULL) {
                        continue;
                    }
                    return convertAvroField(avroValue, s);
                }
                throw new IllegalArgumentException("Found UNION schema but it doesn't contain any type");
            case ARRAY:
                // Handle array conversion by recursively processing array elements
                if (avroValue instanceof GenericData.Array) {
                    GenericData.Array<?> avroArray = (GenericData.Array<?>) avroValue;
                    Schema elementSchema = schema.getElementType();
                    Object[] convertedArray = new Object[avroArray.size()];

                    for (int i = 0; i < avroArray.size(); i++) {
                        convertedArray[i] = convertAvroField(avroArray.get(i), elementSchema);
                    }

                    return convertedArray;
                } else {
                    throw new IllegalArgumentException("Expected GenericData.Array for ARRAY schema type, got: "
                            + avroValue.getClass().getName());
                }
            case BYTES:
            case FIXED:
            case RECORD:
            case MAP:
            default:
                throw new UnsupportedOperationException("Unsupported avro schema type=" + schema.getType()
                        + " for value field schema " + schema.getName());
        }
    }
}

