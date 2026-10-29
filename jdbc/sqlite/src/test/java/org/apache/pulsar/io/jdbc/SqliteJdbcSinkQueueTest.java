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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.jdbc.SqliteJdbcSinkTest.Foo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for the internal queue of the JDBC sink and for what happens to queued records when the
 * sink fails or is closed. Unlike {@link SqliteJdbcSinkTest}, not repeated with the JDBC batch API.
 */
public class SqliteJdbcSinkQueueTest {
    private static final String TABLE_NAME = "TestOpenAndWriteSink";
    private final SqliteUtils sqliteUtils = new SqliteUtils(UUID.randomUUID().toString());
    private SqliteJdbcAutoSchemaSink sink;
    // See blockFlushes()
    private CountDownLatch flushBlocked;
    private CountDownLatch releaseFlush;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        sqliteUtils.setUp();
        sqliteUtils.createTable("CREATE TABLE " + TABLE_NAME
                + "(field1 TEXT, field2 TEXT, field3 INTEGER, PRIMARY KEY (field1));");
        flushBlocked = new CountDownLatch(1);
        releaseFlush = new CountDownLatch(1);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        releaseFlush.countDown();
        if (sink != null) {
            sink.close();
            sink = null;
        }
        sqliteUtils.tearDown();
    }

    /**
     * Test that a flush scheduled before another flush fails does not run once the sink is FAILED.
     * It would ack records newer than the failed batch, which the redelivered older records could
     * then overwrite (e.g. in UPSERT mode).
     */
    @Test
    public void testNoFlushAfterFatal() throws Exception {
        SinkContext sinkContext = mock(SinkContext.class);
        openSink(createConfig(2, -1), sinkContext);
        // Only the first insert fails, so a later flush would succeed
        blockFlushes(1);
        List<CompletableFuture<Boolean>> results = new ArrayList<>();
        List<Record<GenericObject>> records = createRecords(4, results);

        // The first batch starts a flush that gets stuck, the second schedules another flush
        sink.write(records.get(0));
        sink.write(records.get(1));
        Assert.assertTrue(flushBlocked.await(10, TimeUnit.SECONDS));
        sink.write(records.get(2));
        sink.write(records.get(3));
        releaseFlush.countDown();
        awaitScheduledFlushes();

        verify(sinkContext).fatal(any(SQLException.class));
        Assert.assertFalse(results.get(0).get(1, TimeUnit.SECONDS));
        Assert.assertFalse(results.get(1).get(1, TimeUnit.SECONDS));
        Assert.assertEquals(new ArrayList<>(queue()), records.subList(2, 4), "A flush ran after fatal()");
        Assert.assertFalse(results.get(2).isDone() || results.get(3).isDone());
    }

    /**
     * Sink config without a time-based flush.
     */
    private Map<String, Object> createConfig(int batchSize, int maxQueueSize) {
        Map<String, Object> conf = new HashMap<>();
        conf.put("jdbcUrl", sqliteUtils.sqliteUri());
        conf.put("tableName", TABLE_NAME);
        conf.put("key", "field3");
        conf.put("nonKey", "field1,field2");
        conf.put("batchSize", batchSize);
        conf.put("maxQueueSize", maxQueueSize);
        conf.put("timeoutMs", 0);
        return conf;
    }

    private void openSink(Map<String, Object> conf, SinkContext sinkContext) throws Exception {
        sink = new SqliteJdbcAutoSchemaSink();
        sink.open(conf, sinkContext);
    }

    @SuppressWarnings("unchecked")
    private Deque<Record<GenericObject>> queue() throws IllegalAccessException {
        return (Deque<Record<GenericObject>>) FieldUtils.readField(sink, "incomingList", true);
    }

    /**
     * Wait until the flushes scheduled so far have run. The flush executor has a single thread.
     */
    private void awaitScheduledFlushes() throws Exception {
        ((ScheduledExecutorService) FieldUtils.readField(sink, "flushExecutor", true))
                .submit(() -> { }).get(10, TimeUnit.SECONDS);
    }

    /**
     * Replace the insert statement with a mock. Each execute() counts down flushBlocked and waits
     * for releaseFlush, then the first {@code failures} calls throw.
     */
    private void blockFlushes(int failures) throws Exception {
        AtomicInteger remainingFailures = new AtomicInteger(failures);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.execute()).thenAnswer(invocation -> {
            flushBlocked.countDown();
            if (!releaseFlush.await(30, TimeUnit.SECONDS)) {
                throw new TimeoutException("Flush was never released");
            }
            if (remainingFailures.getAndDecrement() > 0) {
                throw new SQLException("Simulated database failure");
            }
            return false;
        });
        FieldUtils.writeField(sink, "insertStatement", statement, true);
    }

    /**
     * Create {@code count} records and add the result of each to {@code results}: true once it is
     * acked, false once it is failed.
     */
    private static List<Record<GenericObject>> createRecords(int count, List<CompletableFuture<Boolean>> results) {
        List<Record<GenericObject>> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            CompletableFuture<Boolean> result = new CompletableFuture<>();
            results.add(result);
            records.add(SqliteJdbcSinkTest.createMockFooRecord(new Foo("f" + i, "f2", i), new HashMap<>(), result));
        }
        return records;
    }
}
