/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.index;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.query.GridQueryProperty;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ddl.DdlStatementsProcessor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.h2.jdbc.JdbcSQLException;
import org.h2.value.DataType;

/**
 * Tests for CREATE/DROP TABLE.
 */
public class H2DynamicTableSelfTest extends AbstractSchemaSelfTest {
    /** Client node index. */
    private final static int CLIENT = 2;

    /** */
    private final static String INDEXED_CACHE_NAME = CACHE_NAME + "_idx";

    /** */
    private final static String INDEXED_CACHE_NAME_2 = INDEXED_CACHE_NAME + "_2";

    /** Data region name. */
    public static final String DATA_REGION_NAME = "my_data_region";

    /** Bad data region name. */
    public static final String DATA_REGION_NAME_BAD = "my_data_region_bad";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);

        client().addCacheConfiguration(cacheConfiguration());
        client().addCacheConfiguration(cacheConfiguration().setName(CACHE_NAME + "_async")
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_ASYNC));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        client().getOrCreateCache(cacheConfigurationForIndexing());
        client().getOrCreateCache(cacheConfigurationForIndexingInPublicSchema());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        execute("DROP TABLE IF EXISTS PUBLIC.\"Person\"");
        execute("DROP TABLE IF EXISTS PUBLIC.\"City\"");
        execute("DROP TABLE IF EXISTS PUBLIC.\"NameTest\"");

        super.afterTest();
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache, H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTable() throws Exception {
        doTestCreateTable(CACHE_NAME, null, null, null, false);
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache, H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTable() throws Exception {
        doTestCreateTable(CACHE_NAME, null, null, null, true);
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache, H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTableWithCacheGroup() throws Exception {
        doTestCreateTable(CACHE_NAME, "MyGroup", null, null, false);
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache, H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTableWithCacheGroup() throws Exception {
        doTestCreateTable(CACHE_NAME, "MyGroup", null, null, true);
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache, H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTableWithCacheGroupAndLegacyParamName() throws Exception {
        doTestCreateTable(CACHE_NAME, "MyGroup", null, null, true, false);
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache, H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTableWithCacheGroupAndLegacyParamName() throws Exception {
        doTestCreateTable(CACHE_NAME, "MyGroup", null, null, false, true);
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache from template,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTableWithWriteSyncMode() throws Exception {
        doTestCreateTable(CACHE_NAME + "_async", null, null, CacheWriteSynchronizationMode.FULL_ASYNC, false);
    }

    /**
     * Test that {@code CREATE TABLE} actually creates new cache from template,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTableWithWriteSyncMode() throws Exception {
        doTestCreateTable(CACHE_NAME + "_async", null, null, CacheWriteSynchronizationMode.FULL_ASYNC, true);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code REPLICATED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTableReplicated() throws Exception {
        doTestCreateTable("REPLICATED", null, CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code REPLICATED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTableReplicated() throws Exception {
        doTestCreateTable("REPLICATED", null, CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTablePartitioned() throws Exception {
        doTestCreateTable("PARTITIONED", null, CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTablePartitioned() throws Exception {
        doTestCreateTable("PARTITIONED", null, CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code REPLICATED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTableReplicatedCaseInsensitive() throws Exception {
        doTestCreateTable("replicated", null, CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code REPLICATED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTableReplicatedCaseInsensitive() throws Exception {
        doTestCreateTable("replicated", null, CacheMode.REPLICATED, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testCreateTablePartitionedCaseInsensitive() throws Exception {
        doTestCreateTable("partitioned", null, CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalCreateTablePartitionedCaseInsensitive() throws Exception {
        doTestCreateTable("partitioned", null, CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes, when no cache template name is given.
     * @throws Exception if failed.
     */
    public void testCreateTableNoTemplate() throws Exception {
        doTestCreateTable(null, null, CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    /**
     * Test that {@code CREATE TABLE} with reserved template cache name actually creates new {@code PARTITIONED} cache,
     * H2 table and type descriptor on all nodes, when no cache template name is given.
     * @throws Exception if failed.
     */
    public void testInternalCreateTableNoTemplate() throws Exception {
        doTestCreateTable(null, null, CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    /**
     * Test behavior depending on table name case sensitivity.
     */
    public void testTableNameCaseSensitivity() {
        doTestTableNameCaseSensitivity("Person", false);

        doTestTableNameCaseSensitivity("Person", true);
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testFullSyncWriteMode() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.FULL_SYNC,
            false, "write_synchronization_mode=full_sync");
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testFullSyncWriteModeInternal() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.FULL_SYNC,
            true, "write_synchronization_mode=full_sync");
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testPrimarySyncWriteMode() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.PRIMARY_SYNC,
            false, "write_synchronization_mode=primary_sync");
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testPrimarySyncWriteModeInternal() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.PRIMARY_SYNC,
            true, "write_synchronization_mode=primary_sync");
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testFullAsyncWriteMode() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.FULL_ASYNC,
            false, "write_synchronization_mode=full_async");
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testFullAsyncWriteModeInternal() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.FULL_ASYNC,
            true, "write_synchronization_mode=full_async");
    }

    /**
     * Test behavior only in case of cache name override.
     */
    public void testCustomCacheName() throws Exception {
        doTestCustomNames("cname", null, null, false);
    }

    /**
     * Test behavior only in case of cache name override.
     */
    public void testCustomCacheNameInternal() throws Exception {
        doTestCustomNames("cname", null, null, true);
    }

    /**
     * Test behavior only in case of key type name override.
     */
    public void testCustomKeyTypeName() throws Exception {
        doTestCustomNames(null, "keytype", null, false);
    }

    /**
     * Test behavior only in case of key type name override.
     */
    public void testCustomKeyTypeNameInternal() throws Exception {
        doTestCustomNames(null, "keytype", null, true);
    }

    /**
     * Test behavior only in case of value type name override.
     */
    public void testCustomValueTypeName() throws Exception {
        doTestCustomNames(null, null, "valtype", false);
    }

    /**
     * Test behavior only in case of value type name override.
     */
    public void testCustomValueTypeNameInternal() throws Exception {
        doTestCustomNames(null, null, "valtype", true);
    }

    /**
     * Test behavior only in case of cache and key type name override.
     */
    public void testCustomCacheAndKeyTypeName() throws Exception {
        doTestCustomNames("cname", "keytype", null, false);
    }

    /**
     * Test behavior only in case of cache and key type name override.
     */
    public void testCustomCacheAndKeyTypeNameInternal() throws Exception {
        doTestCustomNames("cname", "keytype", null, true);
    }

    /**
     * Test behavior only in case of cache and value type name override.
     */
    public void testCustomCacheAndValueTypeName() throws Exception {
        doTestCustomNames("cname", null, "valtype", false);
    }

    /**
     * Test behavior only in case of cache and value type name override.
     */
    public void testCustomCacheAndValueTypeNameInternal() throws Exception {
        doTestCustomNames("cname", null, "valtype", true);
    }

    /**
     * Test behavior only in case of key and value type name override.
     */
    public void testCustomKeyAndValueTypeName() throws Exception {
        doTestCustomNames(null, "keytype", "valtype", false);
    }

    /**
     * Test behavior only in case of key and value type name override.
     */
    public void testCustomKeyAndValueTypeNameInternal() throws Exception {
        doTestCustomNames(null, "keytype", "valtype", true);
    }

    /**
     * Test behavior only in case of cache, key, and value type name override.
     */
    public void testCustomCacheAndKeyAndValueTypeName() throws Exception {
        doTestCustomNames("cname", "keytype", "valtype", false);
    }

    /**
     * Test behavior only in case of cache, key, and value type name override.
     */
    public void testCustomCacheAndKeyAndValueTypeNameInternal() throws Exception {
        doTestCustomNames("cname", "keytype", "valtype", true);
    }

    /**
     * Test that attempting to create a cache with a pre-existing name yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDuplicateCustomCacheName() throws Exception {
        client().getOrCreateCache("new");

        try {
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    doTestCustomNames("new", null, null, false);return null;
                }
            }, IgniteSQLException.class, "Table already exists: NameTest");
        }
        finally {
            client().destroyCache("new");
        }
    }

    /**
     * Test that attempting to create a cache with a pre-existing name yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDuplicateCustomCacheNameInternal() throws Exception {
        client().getOrCreateCache("new");

        try {
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    doTestCustomNames("new", null, null, true);return null;
                }
            }, IgniteSQLException.class, "Table already exists: NameTest");
        }
        finally {
            client().destroyCache("new");
        }
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testPlainKey() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.FULL_SYNC, false);
    }

    /**
     * Test that {@code CREATE TABLE} with given write sync mode actually creates new cache as needed.
     * @throws Exception if failed.
     */
    public void testPlainKeyInternal() throws Exception {
        doTestCreateTable(null, null, null, CacheWriteSynchronizationMode.FULL_SYNC, true);
    }

    /**
     * Test that appending supplied arguments to {@code CREATE TABLE} results in creating new cache that has settings
     * as expected
     * @param cacheName Cache name, or {@code null} if the name generated by default should be used.
     * @param keyTypeName Key type name, or {@code null} if the name generated by default should be used.
     * @param valTypeName Value type name, or {@code null} if the name generated by default should be used.
     * @param useInternalCmd Use internal CREATE TABLE command with additional syntax.
     */
    private void doTestCustomNames(final String cacheName, final String keyTypeName, final String valTypeName,
        final boolean useInternalCmd) throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(useInternalCmd, new Callable<Void>() {
            @Override public Void call() {

                GridStringBuilder b = new GridStringBuilder("CREATE TABLE \"NameTest\" (id int primary key, x varchar) ");

                if (useInternalCmd)
                    b.a("wrap_key wrap_value");
                else
                    b.a("WITH wrap_key,wrap_value");

                assert !F.isEmpty(cacheName) || !F.isEmpty(keyTypeName) || !F.isEmpty(valTypeName);

                if (!F.isEmpty(cacheName)) {
                    if (useInternalCmd)
                        b.a(" cache_name=\"").a(cacheName).a('"');
                    else
                        b.a(",\"cache_name=").a(cacheName).a('"');
                }

                if (!F.isEmpty(keyTypeName)) {
                    if (useInternalCmd)
                        b.a(" key_type=\"").a(keyTypeName).a('"');
                    else
                        b.a(",\"key_type=").a(keyTypeName).a('"');
                }

                if (!F.isEmpty(valTypeName)) {
                    if (useInternalCmd)
                        b.a(" value_type=\"").a(valTypeName).a('"');
                    else
                        b.a(",\"value_type=").a(valTypeName).a('"');
                }

                String res = b.toString();

                if (res.endsWith(","))
                    res = res.substring(0, res.length() - 1);

                execute(client(), res);

                String resCacheName = U.firstNotNull(cacheName, cacheName("NameTest"));

                IgniteInternalCache<BinaryObject, BinaryObject> cache = client().cachex(resCacheName);

                assertNotNull(cache);

                CacheConfiguration ccfg = cache.configuration();

                assertEquals(1, ccfg.getQueryEntities().size());

                QueryEntity e = (QueryEntity)ccfg.getQueryEntities().iterator().next();

                if (!F.isEmpty(keyTypeName))
                    assertEquals(keyTypeName, e.getKeyType());
                else
                    assertTrue(e.getKeyType().startsWith("SQL_PUBLIC"));

                if (!F.isEmpty(valTypeName))
                    assertEquals(valTypeName, e.getValueType());
                else
                    assertTrue(e.getValueType().startsWith("SQL_PUBLIC"));

                execute(client(), "INSERT INTO \"NameTest\" (id, x) values (1, 'a')");

                List<List<?>> qres = execute(client(), "SELECT id, x from \"NameTest\"");

                assertEqualsCollections(Collections.singletonList(Arrays.asList(1, "a")), qres);

                BinaryObject key = client().binary().builder(e.getKeyType()).setField("ID", 1).build();

                BinaryObject val = (BinaryObject)client().cache(resCacheName).withKeepBinary().get(key);

                BinaryObject exVal = client().binary().builder(e.getValueType()).setField("X", "a").build();

                assertEquals(exVal, val);

                return null;
            }
        });
    }

    /**
     * Perform a check on given table name considering case sensitivity.
     * @param tblName Table name to check.
     * @param sensitive Whether table should be created w/case sensitive name or not.
     */
    private void doTestTableNameCaseSensitivity(String tblName, boolean sensitive) {
        String tblNameSql = (sensitive ? '"' + tblName + '"' : tblName);

        // This one should always work.
        assertTableNameIsValid(tblNameSql, tblNameSql);

        if (sensitive) {
            assertTableNameIsNotValid(tblNameSql, tblName.toUpperCase());

            assertTableNameIsNotValid(tblNameSql, tblName.toLowerCase());
        }
        else {
            assertTableNameIsValid(tblNameSql, '"' + tblName.toUpperCase() + '"');

            assertTableNameIsValid(tblNameSql, tblName.toUpperCase());

            assertTableNameIsValid(tblNameSql, tblName.toLowerCase());
        }
    }

    /**
     * Check that given variant of table name works for DML and DDL contexts, as well as selects.
     * @param tblNameToCreate Name of the table to use in {@code CREATE TABLE}.
     * @param checkedTblName Table name to use in actual checks.
     */
    private void assertTableNameIsValid(String tblNameToCreate, String checkedTblName) {
        info("Checking table name variant for validity: " + checkedTblName);

        execute("create table if not exists " + tblNameToCreate + " (id int primary key, name varchar)");

        execute("MERGE INTO " + checkedTblName + " (id, name) values (1, 'A')");

        execute("SELECT * FROM " + checkedTblName);

        execute("DROP TABLE " + checkedTblName);
    }

    /**
     * Check that given variant of table name does not work for DML and DDL contexts, as well as selects.
     * @param tblNameToCreate Name of the table to use in {@code CREATE TABLE}.
     * @param checkedTblName Table name to use in actual checks.
     */
    private void assertTableNameIsNotValid(String tblNameToCreate, String checkedTblName) {
        info("Checking table name variant for invalidity: " + checkedTblName);

        execute("create table if not exists " + tblNameToCreate + " (id int primary key, name varchar)");

        assertCommandThrowsTableNotFound(checkedTblName.toUpperCase(),
            "MERGE INTO " + checkedTblName + " (id, name) values (1, 'A')");

        assertCommandThrowsTableNotFound(checkedTblName.toUpperCase(), "SELECT * FROM " + checkedTblName);

        assertDdlCommandThrowsTableNotFound(checkedTblName.toUpperCase(), "DROP TABLE " + checkedTblName);
    }

    /**
     * Check that given (non DDL) command throws an exception as expected.
     * @param checkedTblName Table name to expect in error message.
     * @param cmd Command to execute.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertCommandThrowsTableNotFound(String checkedTblName, final String cmd) {
        final Throwable e = GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
            @Override public Object call() throws Exception {
                execute(cmd);

                return null;
            }
        }, JdbcSQLException.class);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @SuppressWarnings("ConstantConditions")
            @Override public Object call() throws Exception {
                throw (Exception)e.getCause();
            }
        }, JdbcSQLException.class, "Table \"" + checkedTblName + "\" not found");
    }

    /**
     * Check that given DDL command throws an exception as expected.
     * @param checkedTblName Table name to expect in error message.
     * @param cmd Command to execute.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertDdlCommandThrowsTableNotFound(String checkedTblName, final String cmd) {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @SuppressWarnings("ConstantConditions")
            @Override public Object call() throws Exception {
                execute(cmd);

                return null;
            }
        }, IgniteSQLException.class, "Table doesn't exist: " + checkedTblName);
    }

    /**
     * Test that {@code CREATE TABLE} with given template cache name actually creates new cache,
     * H2 table and type descriptor on all nodes, optionally with cache type check.
     * @param tplCacheName Template cache name.
     * @param cacheGrp Cache group name, or {@code null} if no group is set.
     * @param cacheMode Expected cache mode, or {@code null} if no check is needed.
     * @param writeSyncMode Expected write sync mode, or {@code null} if no check is needed.
     * @param useInternalCmd use internal CREATE TABLE command instead of H2 one.
     * @param additionalParams Supplemental parameters to append to {@code CREATE TABLE} SQL.
     */
    private void doTestCreateTable(String tplCacheName, String cacheGrp, CacheMode cacheMode,
        CacheWriteSynchronizationMode writeSyncMode, boolean useInternalCmd, String... additionalParams) throws Exception {
        doTestCreateTable(tplCacheName, cacheGrp, cacheMode, writeSyncMode, false, useInternalCmd, additionalParams);
    }

    /**
     * Test that {@code CREATE TABLE} with given template cache name actually creates new cache,
     * H2 table and type descriptor on all nodes, optionally with cache type check.
     * @param tplCacheName Template cache name.
     * @param cacheGrp Cache group name, or {@code null} if no group is set.
     * @param cacheMode Expected cache mode, or {@code null} if no check is needed.
     * @param writeSyncMode Expected write sync mode, or {@code null} if no check is needed.
     * @param useLegacyCacheGrpParamName Whether legacy (harder-to-read) cache group param name should be used.
     * @param useInternalCmd use internal CREATE TABLE command instead of H2 one.
     * @param additionalParams Supplemental parameters to append to {@code CREATE TABLE} SQL.
     */
    private void doTestCreateTable(final String tplCacheName, final String cacheGrp, final CacheMode cacheMode,
        final CacheWriteSynchronizationMode writeSyncMode, final boolean useLegacyCacheGrpParamName, final boolean useInternalCmd,
        final String... additionalParams)
        throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(useInternalCmd, new Callable<Void>() {
            @Override public Void call() throws Exception {

                String cacheGrpParamName = useLegacyCacheGrpParamName ? "cacheGroup" : "cache_group";

                String sql = "CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) ";

                if (useInternalCmd) {
                    sql += (F.isEmpty(tplCacheName) ? "" : "template=\"" + tplCacheName + "\" ") + "backups=10 atomicity=atomic " +
                        (F.isEmpty(cacheGrp) ? "" : " " + cacheGrpParamName + "=\"" + cacheGrp + '"');

                    for (String p : additionalParams)
                        sql += " " + p;
                }
                else {
                    sql += "WITH " +
                        (F.isEmpty(tplCacheName) ? "" : "\"template=" + tplCacheName + "\",") + "\"backups=10,atomicity=atomic\"" +
                        (F.isEmpty(cacheGrp) ? "" : ",\"" + cacheGrpParamName + '=' + cacheGrp + '"');

                    for (String p : additionalParams)
                        sql += ",\"" + p + "\"";
                }

                execute(sql);

                String cacheName = cacheName("Person");

                for (int i = 0; i < 4; i++) {
                    IgniteEx node = grid(i);

                    assertNotNull(node.cache(cacheName));

                    DynamicCacheDescriptor cacheDesc = node.context().cache().cacheDescriptor(cacheName);

                    assertNotNull(cacheDesc);

                    if (cacheMode == CacheMode.REPLICATED)
                        assertEquals(Integer.MAX_VALUE, cacheDesc.cacheConfiguration().getBackups());
                    else
                        assertEquals(10, cacheDesc.cacheConfiguration().getBackups());

                    assertEquals(CacheAtomicityMode.ATOMIC, cacheDesc.cacheConfiguration().getAtomicityMode());

                    assertTrue(cacheDesc.sql());

                    assertEquals(cacheGrp, cacheDesc.groupDescriptor().groupName());

                    if (cacheMode != null)
                        assertEquals(cacheMode, cacheDesc.cacheConfiguration().getCacheMode());

                    if (writeSyncMode != null)
                        assertEquals(writeSyncMode, cacheDesc.cacheConfiguration().getWriteSynchronizationMode());

                    List<String> colNames = new ArrayList<>(5);

                    List<Class<?>> colTypes = new ArrayList<>(5);

                    List<String> pkColNames = new ArrayList<>(2);

                    try (Connection c = connect(node)) {
                        try (ResultSet rs = c.getMetaData().getColumns(null, QueryUtils.DFLT_SCHEMA, "Person", null)) {
                            for (int j = 0; j < 5; j++) {
                                assertTrue(rs.next());

                                colNames.add(rs.getString("COLUMN_NAME"));

                                try {
                                    colTypes.add(Class.forName(DataType.getTypeClassName(DataType
                                        .convertSQLTypeToValueType(rs.getInt("DATA_TYPE")))));
                                }
                                catch (ClassNotFoundException e) {
                                    throw new AssertionError(e);
                                }
                            }

                            assertFalse(rs.next());
                        }

                        try (ResultSet rs = c.getMetaData().getPrimaryKeys(null, QueryUtils.DFLT_SCHEMA, "Person")) {
                            for (int j = 0; j < 2; j++) {
                                assertTrue(rs.next());

                                pkColNames.add(rs.getString("COLUMN_NAME"));
                            }

                            assertFalse(rs.next());
                        }
                    }

                    assertEqualsCollections(F.asList("id", "city", "name", "surname", "age"), colNames);

                    assertEqualsCollections(F.<Class<?>>asList(Integer.class, String.class, String.class, String.class,
                        Integer.class), colTypes);

                    assertEqualsCollections(F.asList("id", "city"), pkColNames);
                }

                return null;
            }
        });
    }

    /**
     * Test that attempting to specify negative number of backups yields exception.
     */
    public void testNegativeBackups() {
        assertCreateTableWithParamsThrows("bAckUPs = -5  ",
            "\"BACKUPS\" cannot be negative: -5", false);
    }

    /**
     * Test that attempting to specify negative number of backups yields exception.
     */
    public void testNegativeBackupsInternal() {
        assertCreateTableWithParamsThrows("bAckUPs = -5  ",
            "Number of backups should be positive: -5", true);
    }

    /**
     * Test that attempting to omit mandatory value of BACKUPS parameter yields an error.
     */
    public void testEmptyBackups() {
        assertCreateTableWithParamsThrows(" bAckUPs =  ",
            "Parameter value cannot be empty: BACKUPS", false);
    }

    /**
     * Test that attempting to omit mandatory value of BACKUPS parameter yields an error.
     */
    public void testEmptyBackupsInternal() {
        assertCreateTableWithParamsThrows(" bAckUPs =  ",
            "Unexpected end of command (expected: \"[integer]\"", true);
    }

    /**
     * Test that attempting to omit mandatory value of ATOMICITY parameter yields an error.
     */
    public void testEmptyAtomicity() {
        assertCreateTableWithParamsThrows("AtomicitY=  ",
            "Parameter value cannot be empty: ATOMICITY", false);
    }

    /**
     * Test that attempting to omit mandatory value of ATOMICITY parameter yields an error.
     */
    public void testEmptyAtomicityInternal() {
        assertCreateTableWithParamsThrows("AtomicitY=  ",
            "re:Unexpected end of command.*expected:.*TRANSACTIONAL.*ATOMIC", true);
    }

    /**
     * Test that providing an invalid value of ATOMICITY parameter yields an error.
     */
    public void testInvalidAtomicity() {
        assertCreateTableWithParamsThrows("atomicity=InvalidValue",
            "Invalid value of \"ATOMICITY\" parameter (should be either TRANSACTIONAL or ATOMIC): InvalidValue", false);
    }

    /**
     * Test that providing an invalid value of ATOMICITY parameter yields an error.
     */
    public void testInvalidAtomicityInternal() {
        assertCreateTableWithParamsThrows("atomicity=InvalidValue",
            "re:Unexpected token:.*INVALIDVALUE.*expected:.*TRANSACTIONAL.*ATOMIC", true);
    }

    /**
     * Test that attempting to omit mandatory value of CACHEGROUP parameter yields an error.
     */
    public void testEmptyCacheGroup() {
        assertCreateTableWithParamsThrows("cache_group=",
            "Parameter value cannot be empty: CACHE_GROUP", false);
    }

    /**
     * Test that attempting to omit mandatory value of CACHEGROUP parameter yields an error.
     */
    public void testEmptyCacheGroupInternal() {
        assertCreateTableWithParamsThrows("cache_group=",
            "Unexpected end of command (expected: \"[string]\")",
            true);
    }

    /**
     * Test that attempting to omit mandatory value of WRITE_SYNCHRONIZATION_MODE parameter yields an error.
     */
    public void testEmptyWriteSyncMode() {
        assertCreateTableWithParamsThrows("write_synchronization_mode=",
            "Parameter value cannot be empty: WRITE_SYNCHRONIZATION_MODE", false);
    }

    /**
     * Test that attempting to omit mandatory value of WRITE_SYNCHRONIZATION_MODE parameter yields an error.
     */
    public void testEmptyWriteSyncModeInternal() {
        assertCreateTableWithParamsThrows("write_synchronization_mode=",
            "re:Unexpected end of command.*expected.*FULL_SYNC.*FULL_ASYNC.*PRIMARY_SYNC",
            true);
    }

    /**
     * Test that attempting to provide invalid value of WRITE_SYNCHRONIZATION_MODE parameter yields an error.
     */
    public void testInvalidWriteSyncMode() {
        assertCreateTableWithParamsThrows("write_synchronization_mode=invalid",
            "Invalid value of \"WRITE_SYNCHRONIZATION_MODE\" parameter " +
                "(should be FULL_SYNC, FULL_ASYNC, or PRIMARY_SYNC): invalid", false);
    }

    /**
     * Test that attempting to provide invalid value of WRITE_SYNCHRONIZATION_MODE parameter yields an error.
     */
    public void testInvalidWriteSyncModeInternal() {
        assertCreateTableWithParamsThrows("write_synchronization_mode=invalid",
            "re:Unexpected token:.*INVALID.*expected:.*FULL_SYNC.*FULL_ASYNC.*PRIMARY_SYNC",
            true);
    }

    /**
     * Test that attempting to {@code CREATE TABLE} that already exists does not yield an error if the statement
     *     contains {@code IF NOT EXISTS} clause.
     * @throws Exception if failed.
     */
    public void testCreateTableIfNotExists() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                    "\"template=cache\"");

                execute("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                    "\"template=cache\"");

                return null;
            }
        });
    }

    /**
     * Test that attempting to {@code CREATE TABLE} that already exists does not yield an error if the statement
     *     contains {@code IF NOT EXISTS} clause.
     * @throws Exception if failed.
     */
    public void testInternalCreateTableIfNotExists() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() throws Exception {

                execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                    "template=\"cache\"");

                execute("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                    "template=\"cache\"");

                return null;
            }
        });
    }

    /**
     * Test that attempting to {@code CREATE TABLE} that already exists yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCreateExistingTable() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
                        " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                        "\"template=cache\"");

                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar" +
                                ", \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                                "\"template=cache\"");

                            return null;
                        }
                    }, IgniteSQLException.class, "Table already exists: Person");
                }
                finally {
                    execute("DROP TABLE \"Person\"");
                }

                return null;
            }
        });
    }

    /**
     * Test that attempting to {@code CREATE TABLE} that already exists yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testInternalCreateExistingTable() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
                        " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                        "template=\"cache\"");

                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar" +
                                ", \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                                "template=\"cache\"");

                            return null;
                        }
                    }, IgniteSQLException.class, "Table already exists: Person");
                }
                finally {
                    execute("DROP TABLE \"Person\"");
                }

                return null;
            }
        });
    }

    /**
     * Test that {@code DROP TABLE} actually removes specified cache and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testDropTable() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    execute("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
                        " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                        "\"template=cache\"");
                }
                finally {
                    execute("DROP TABLE \"Person\"");
                }

                for (int i = 0; i < 4; i++) {
                    IgniteEx node = grid(i);

                    assertNull(node.cache("Person"));

                    QueryTypeDescriptorImpl desc = type(node, "Person", "Person");

                    assertNull(desc);
                }

                return null;
            }
        });
    }

    /**
     * Test that {@code DROP TABLE} actually removes specified cache and type descriptor on all nodes.
     * @throws Exception if failed.
     */
    public void testInternalDropTable() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    execute("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
                        " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                        "template=\"cache\"");
                }
                finally {
                    execute("DROP TABLE \"Person\"");
                }

                for (int i = 0; i < 4; i++) {
                    IgniteEx node = grid(i);

                    assertNull(node.cache("Person"));

                    QueryTypeDescriptorImpl desc = type(node, "Person", "Person");

                    assertNull(desc);
                }

                return null;
            }
        });
    }

    /**
     * Test that attempting to execute {@code DROP TABLE} via API of cache being dropped yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCacheSelfDrop() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    execute("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
                        " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                        "\"template=cache\"");

                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {

                            client().cache(QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, "Person"))
                                .query(new SqlFieldsQuery("DROP TABLE \"Person\"")).getAll();

                            return null;
                        }

                    }, IgniteSQLException.class, "DROP TABLE cannot be called from the same cache that holds the table " +
                        "being dropped");
                }
                finally {
                    execute("DROP TABLE \"Person\"");
                }

                return null;
            }
        });
    }

    /**
     * Test that attempting to execute {@code DROP TABLE} via API of cache being dropped yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCacheSelfDropInternal() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    execute("CREATE TABLE IF NOT EXISTS \"Person\" (\"id\" int, \"city\" varchar," +
                        " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                        "template=\"cache\"");

                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            client().cache(QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, "Person"))
                                .query(new SqlFieldsQuery("DROP TABLE \"Person\"")).getAll();

                            return null;
                        }
                    }, IgniteSQLException.class, "DROP TABLE cannot be called from the same cache that holds the table " +
                        "being dropped");

                }
                finally {
                    execute("DROP TABLE \"Person\"");
                }

                return null;
            }
        });
    }

    /**
     * Test that attempting to {@code DROP TABLE} that does not exist does not yield an error if the statement contains
     *     {@code IF EXISTS} clause.
     *
     * @throws Exception if failed.
     */
    public void testDropMissingTableIfExists() throws Exception {
        execute("DROP TABLE IF EXISTS \"City\"");
    }

    /**
     * Test that attempting to {@code DROP TABLE} that does not exist yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDropMissingTable() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                execute("DROP TABLE \"City\"");

                return null;
            }
        }, IgniteSQLException.class, "Table doesn't exist: City");
    }

    /**
     * Check that {@code DROP TABLE} for caches not created with {@code CREATE TABLE} yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDropNonDynamicTable() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                execute("DROP TABLE PUBLIC.\"Integer\"");

                return null;
            }
        }, IgniteSQLException.class,
        "Only cache created with CREATE TABLE may be removed with DROP TABLE [cacheName=cache_idx_2]");
    }

    /**
     * Test that attempting to destroy via cache API a cache created via SQL yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDestroyDynamicSqlCache() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                    "\"template=cache\"");

                GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            client().destroyCache(cacheName("Person"));

                            return null;
                        }
                    }, CacheException.class,
                    "Only cache created with cache API may be removed with direct call to destroyCache");

                return null;
            }
        });
    }

    /**
     * Test that attempting to destroy via cache API a cache created via SQL yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDestroyDynamicSqlCacheInternal() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar," +
                    " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                    "template=\"cache\"");

                GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            client().destroyCache(cacheName("Person"));

                            return null;
                        }
                    }, CacheException.class,
                    "Only cache created with cache API may be removed with direct call to destroyCache");

                return null;
            }
        });
    }

    /**
     * Test that attempting to start a node that has a cache with the name already present in the grid and whose
     * SQL flag does not match that of cache with the same name that is already started, yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testSqlFlagCompatibilityCheck() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar, \"name\" varchar, \"surname\" varchar, " +
                    "\"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH \"template=cache\"");

                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        String cacheName = cacheName("Person");

                        Ignition.start(clientConfiguration(5).setCacheConfiguration(new CacheConfiguration(cacheName)));

                        return null;
                    }
                }, IgniteException.class, "Cache configuration mismatch (local cache was created via Ignite API, while " +
                    "remote cache was created via CREATE TABLE): SQL_PUBLIC_Person");

                return null;
            }
        });
    }

    /**
     * Test that attempting to start a node that has a cache with the name already present in the grid and whose
     * SQL flag does not match that of cache with the same name that is already started, yields an error.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testSqlFlagCompatibilityCheckInternal() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar, \"name\" varchar, \"surname\" varchar, " +
                    "\"age\" int, PRIMARY KEY (\"id\", \"city\")) template=\"cache\"");

                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        String cacheName = cacheName("Person");

                        Ignition.start(clientConfiguration(5).setCacheConfiguration(new CacheConfiguration(cacheName)));

                        return null;
                    }
                }, IgniteException.class, "Cache configuration mismatch (local cache was created via Ignite API, while " +
                    "remote cache was created via CREATE TABLE): SQL_PUBLIC_Person");

                return null;
            }
        });
    }

    /**
     * Tests index name conflict check in discovery thread.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testIndexNameConflictCheckDiscovery() throws Exception {
        execute(grid(0), "CREATE TABLE \"Person\" (id int primary key, name varchar)");

        execute(grid(0), "CREATE INDEX \"idx\" ON \"Person\" (\"name\")");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                QueryEntity e = new QueryEntity();

                e.setTableName("City");
                e.setKeyFields(Collections.singleton("name"));
                e.setFields(new LinkedHashMap<>(Collections.singletonMap("name", String.class.getName())));
                e.setIndexes(Collections.singleton(new QueryIndex("name").setName("idx")));
                e.setKeyType("CityKey");
                e.setValueType("City");

                queryProcessor(client()).dynamicTableCreate("PUBLIC", e, CacheMode.PARTITIONED.name(), null, null, null,
                    null, CacheAtomicityMode.ATOMIC, null, 10, false);

                return null;
            }
        }, SchemaOperationException.class, "Index already exists: idx");
    }

    /**
     * Tests table name conflict check in {@link DdlStatementsProcessor}.
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testTableNameConflictCheckSql() throws Exception {
        execute(grid(0), "CREATE TABLE \"Person\" (id int primary key, name varchar)");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override  public Object call() throws Exception {
                execute(client(), "CREATE TABLE \"Person\" (id int primary key, name varchar)");

                return null;
            }
        }, IgniteSQLException.class, "Table already exists: Person");
    }

    /**
     * @throws Exception if failed.
     */
    public void testAffinityKey() throws Exception {
        checkAffinityKey(false);
    }

    /**
     * @throws Exception if failed.
     */
    public void testAffinityKeyInternal() throws Exception {
        checkAffinityKey(true);
    }

    /**
     * @throws Exception if failed.
     */
    public void checkAffinityKey(final boolean useInternalCmd) throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(useInternalCmd, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    if (useInternalCmd)
                        execute("CREATE TABLE \"City\" (\"name\" varchar primary key, \"code\" int) wrap_key wrap_value " +
                            "affinity_key=\"name\"");
                    else
                        execute("CREATE TABLE \"City\" (\"name\" varchar primary key, \"code\" int) WITH wrap_key,wrap_value," +
                            "\"affinity_key='name'\"");

                    assertAffinityCacheConfiguration("City", "name");

                    execute("INSERT INTO \"City\" (\"name\", \"code\") values ('A', 1), ('B', 2), ('C', 3)");

                    List<String> cityNames = Arrays.asList("A", "B", "C");

                    List<Integer> cityCodes = Arrays.asList(1, 2, 3);

                    // We need unique name for this table to avoid conflicts with existing binary metadata.
                    if (useInternalCmd)
                        execute("CREATE TABLE \"Person2\" (\"id\" int, \"city\" varchar," +
                            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                            "wrap_key wrap_value template=\"cache\" affinity_key=\"city\"");
                    else
                        execute("CREATE TABLE \"Person2\" (\"id\" int, \"city\" varchar," +
                            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                            "wrap_key,wrap_value,\"template=cache,affinity_key='city'\"");

                    assertAffinityCacheConfiguration("Person2", "city");

                    Random r = new Random();

                    Map<Integer, Integer> personId2cityCode = new HashMap<>();

                    for (int i = 0; i < 100; i++) {
                        int cityIdx = r.nextInt(3);

                        String cityName = cityNames.get(cityIdx);

                        int cityCode = cityCodes.get(cityIdx);

                        personId2cityCode.put(i, cityCode);

                        queryProcessor(client()).querySqlFieldsNoCache(new SqlFieldsQuery("insert into \"Person2\"(\"id\", " +
                            "\"city\") values (?, ?)").setArgs(i, cityName), true).getAll();
                    }

                    List<List<?>> res = queryProcessor(client()).querySqlFieldsNoCache(new SqlFieldsQuery("select \"id\", " +
                        "c.\"code\" from \"Person2\" p left join \"City\" c on p.\"city\" = c.\"name\" where c.\"name\" " +
                        "is not null"), true).getAll();

                    assertEquals(100, res.size());

                    for (int i = 0; i < 100; i++) {
                        assertNotNull(res.get(i).get(0));

                        assertNotNull(res.get(i).get(1));

                        int id = (Integer)res.get(i).get(0);

                        int code = (Integer)res.get(i).get(1);

                        assertEquals((int)personId2cityCode.get(id), code);
                    }
                }
                finally {
                    execute("drop table \"City\"");
                    execute("drop table \"Person2\"");
                }

                return null;
            }
        });
    }

    /**
     * Tests data region.
     *
     * @throws Exception If failed.
     */
    public void testDataRegion() throws Exception {
        checkDataRegion(false);
    }

    /**
     * Tests data region.
     *
     * @throws Exception If failed.
     */
    public void testDataRegionInternal() throws Exception {
        checkDataRegion(true);
    }

    /**
     * Test data region.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ThrowableNotThrown", "unchecked"})
    public void checkDataRegion(final boolean useInternalCmd) throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(useInternalCmd, new Callable<Void>() {
            @Override public Void call() {

                try {
                    // Empty region name.
                    if (useInternalCmd)
                        GridTestUtils.assertThrows(log, new Callable<Object>() {
                            @Override public Void call() throws Exception {
                                execute("CREATE TABLE TEST_DATA_REGION (name varchar primary key, code int) data_region=");

                                return null;
                            }
                        }, IgniteSQLException.class, "Unexpected end of command (expected: \"[string]\")");
                    else
                        GridTestUtils.assertThrows(log, new Callable<Object>() {
                            @Override public Void call() throws Exception {
                                execute("CREATE TABLE TEST_DATA_REGION (name varchar primary key, code int) WITH \"data_region=\"");

                                return null;
                            }
                        }, IgniteSQLException.class, "Parameter value cannot be empty: DATA_REGION");

                    // Valid region name.
                    if (useInternalCmd)
                        execute("CREATE TABLE TEST_DATA_REGION (name varchar primary key, code int) data_region=\"" +
                            DATA_REGION_NAME + "\"");
                    else
                        execute("CREATE TABLE TEST_DATA_REGION (name varchar primary key, code int) WITH \"data_region=" +
                            DATA_REGION_NAME + "\"");

                    CacheConfiguration ccfg =
                        client().cache("SQL_PUBLIC_TEST_DATA_REGION").getConfiguration(CacheConfiguration.class);

                    assertEquals(DATA_REGION_NAME, ccfg.getDataRegionName());
                }
                finally {
                    execute("DROP TABLE IF EXISTS TEST_DATA_REGION");
                }

                return null;
            }
        });
    }

    /**
     * Test various cases of affinity key column specification.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testAffinityKeyCaseSensitivity() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                try {
                    execute("CREATE TABLE \"A\" (\"name\" varchar primary key, \"code\" int) WITH wrap_key,wrap_value," +
                        "\"affinity_key='name'\"");

                    assertAffinityCacheConfiguration("A", "name");

                    execute("CREATE TABLE \"B\" (name varchar primary key, \"code\" int) WITH wrap_key,wrap_value," +
                        "\"affinity_key=name\"");

                    assertAffinityCacheConfiguration("B", "NAME");

                    execute("CREATE TABLE \"C\" (name varchar primary key, \"code\" int) WITH wrap_key,wrap_value," +
                        "\"affinity_key=NamE\"");

                    assertAffinityCacheConfiguration("C", "NAME");

                    execute("CREATE TABLE \"D\" (\"name\" varchar primary key, \"code\" int) WITH wrap_key,wrap_value," +
                        "\"affinity_key=NAME\"");

                    assertAffinityCacheConfiguration("D", "name");

                    // Error arises because user has specified case sensitive affinity column name
                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            execute("CREATE TABLE \"E\" (name varchar primary key, \"code\" int) WITH wrap_key,wrap_value," +
                                "\"affinity_key='Name'\"");

                            return null;
                        }
                    }, IgniteSQLException.class, "Affinity key column with given name not found: Name");

                    // Error arises because user declares case insensitive affinity column name while having two 'name'
                    // columns whose names are equal in ignore case.
                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            execute("CREATE TABLE \"E\" (\"name\" varchar, \"Name\" int, val int, primary key(\"name\", " +
                                "\"Name\")) WITH \"affinity_key=name\"");

                            return null;
                        }
                    }, IgniteSQLException.class, "Ambiguous affinity column name, use single quotes for case sensitivity: name");

                    execute("CREATE TABLE \"E\" (\"name\" varchar, \"Name\" int, val int, primary key(\"name\", " +
                        "\"Name\")) WITH wrap_key,wrap_value,\"affinityKey='Name'\"");

                    assertAffinityCacheConfiguration("E", "Name");

                }
                finally {
                    execute("drop table a");

                    execute("drop table b");

                    execute("drop table c");

                    execute("drop table d");

                    execute("drop table e");
                }

                return null;
            }
        });
    }

    /**
     * Test various cases of affinity key column specification.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testAffinityKeyCaseSensitivityInternal() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    execute("CREATE TABLE \"A\" (\"name\" varchar primary key, \"code\" int) wrap_key wrap_value " +
                        "affinity_key=\"name\"");

                    assertAffinityCacheConfiguration("A", "name");

                    execute("CREATE TABLE \"B\" (name varchar primary key, \"code\" int) wrap_key wrap_value " +
                        "affinity_key=name");

                    assertAffinityCacheConfiguration("B", "NAME");

                    execute("CREATE TABLE \"C\" (name varchar primary key, \"code\" int) wrap_key wrap_value " +
                        "affinity_key=NamE");

                    assertAffinityCacheConfiguration("C", "NAME");

                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                execute("CREATE TABLE \"D\" (\"name\" varchar primary key, \"code\" int) " +
                                    "wrap_key wrap_value affinity_key=NAME");

                                return null;
                            }
                        }, IgniteSQLException.class, "Affinity key column with given name not found: NAME");

                    // Error arises because user has specified case sensitive affinity column name
                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            execute("CREATE TABLE \"E\" (name varchar primary key, \"code\" int) wrap_key wrap_value " +
                                "affinity_key=\"Name\"");

                            return null;
                        }
                        }, IgniteSQLException.class, "Affinity key column with given name not found: Name");

                    // Error arises because user declares case insensitive affinity column name while having two 'name'
                    // columns whose names are equal in ignore case.
                    GridTestUtils.assertThrows(null, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            execute("CREATE TABLE \"E\" (\"name\" varchar, \"Name\" int, val int, primary key(\"name\", " +
                                "\"Name\")) affinity_key=name");

                            return null;
                        }
                        }, IgniteSQLException.class, "Affinity key column with given name not found: NAME");

                    execute("CREATE TABLE \"E\" (\"name\" varchar, \"Name\" int, val int, primary key(\"name\", " +
                        "\"Name\")) wrap_key wrap_value affinity_key=\"Name\"");

                    assertAffinityCacheConfiguration("E", "Name");

                }
                finally {
                    execute("drop table if exists a");

                    execute("drop table if exists b");

                    execute("drop table if exists c");

                    execute("drop table if exists d");

                    execute("drop table if exists e");
                }

                return null;
            }
        });
    }

    /**
     * Tests that attempting to specify an affinity key that actually is a value column yields an error.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testAffinityKeyNotKeyColumn() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                // Error arises because user has specified case sensitive affinity column name
                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        execute("CREATE TABLE \"E\" (name varchar primary key, \"code\" int) WITH \"affinity_key=code\"");

                        return null;
                    }
                }, IgniteSQLException.class, "Affinity key column must be one of key columns: code");

                return null;
            }
        });
    }

    /**
     * Tests that attempting to specify an affinity key that actually is a value column yields an error.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testAffinityKeyNotKeyColumnInternal() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                // Error arises because user has specified case sensitive affinity column name
                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        execute("CREATE TABLE \"E\" (name varchar primary key, \"code\" int) affinity_key=code");

                        return null;
                    }
                }, IgniteSQLException.class, "Affinity key column with given name not found: CODE");

                return null;
            }
        });
    }

    /**
     * Tests that attempting to specify an affinity key that actually is a value column yields an error.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testAffinityKeyNotFound() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                // Error arises because user has specified case sensitive affinity column name
                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        execute("CREATE TABLE \"E\" (name varchar primary key, \"code\" int) WITH \"affinity_key=missing\"");

                        return null;
                    }
                }, IgniteSQLException.class, "Affinity key column with given name not found: missing");

                return null;
            }
        });
    }

    /**
     * Tests that attempting to specify an affinity key that actually is a value column yields an error.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testAffinityKeyNotFoundInternal() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                // Error arises because user has specified case sensitive affinity column name
                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        execute("CREATE TABLE \"E\" (name varchar primary key, \"code\" int) affinity_key=missing");

                        return null;
                    }
                }, IgniteSQLException.class, "Affinity key column with given name not found: MISSING");

                return null;
            }
        });
    }

    /**
     * Tests behavior on sequential create and drop of a table and its index.
     */
    public void testTableAndIndexRecreate() {
        execute("drop table if exists \"PUBLIC\".t");

        // First let's check behavior without index name set
        execute("create table \"PUBLIC\".t (a int primary key, b varchar(30))");

        fillRecreatedTable();

        execute("create index on \"PUBLIC\".t (b desc)");
        execute("drop table \"PUBLIC\".t");

        assertNull(client().cache(cacheName("t")));

        execute("create table \"PUBLIC\".t (a int primary key, b varchar(30))");

        fillRecreatedTable();

        execute("create index on \"PUBLIC\".t (b desc)");
        execute("drop table \"PUBLIC\".t");

        assertNull(client().cache("t"));

        // And now let's do the same for the named index
        execute("create table \"PUBLIC\".t (a int primary key, b varchar(30))");

        fillRecreatedTable();

        execute("create index namedIdx on \"PUBLIC\".t (b desc)");
        execute("drop table \"PUBLIC\".t");

        assertNull(client().cache("t"));

        execute("create table \"PUBLIC\".t (a int primary key, b varchar(30))");

        fillRecreatedTable();

        execute("create index namedIdx on \"PUBLIC\".t (b desc)");
        execute("drop table \"PUBLIC\".t");
    }

    /**
     * @throws Exception If test failed.
     */
    public void testQueryLocalWithRecreate() throws Exception {
        checkQueryLocalWithRecreate(false);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testQueryLocalWithRecreateInternal() throws Exception {
        checkQueryLocalWithRecreate(true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void checkQueryLocalWithRecreate(final boolean useInternalCmd) throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(useInternalCmd, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    if (useInternalCmd)
                        execute("CREATE TABLE A(id int primary key, name varchar, surname varchar) cache_name=\"cache\" " +
                            "template=replicated");
                    else
                        execute("CREATE TABLE A(id int primary key, name varchar, surname varchar) WITH \"cache_name=cache," +
                            "template=replicated\"");

                    // In order for local queries to work, let's use non client node.
                    IgniteInternalCache cache = grid(0).cachex("cache");

                    assertNotNull(cache);

                    executeLocal(cache.context(), "INSERT INTO A(id, name, surname) values (1, 'X', 'Y')");

                    assertEqualsCollections(Collections.singletonList(Arrays.asList(1, "X", "Y")),
                        executeLocal(cache.context(), "SELECT id, name, surname FROM A"));

                    execute("DROP TABLE A");

                    if (useInternalCmd)
                        execute("CREATE TABLE A(id int primary key, name varchar, surname varchar) cache_name=\"cache\"");
                    else
                        execute("CREATE TABLE A(id int primary key, name varchar, surname varchar) WITH \"cache_name=cache\"");

                    cache = grid(0).cachex("cache");

                    assertNotNull(cache);

                    executeLocal(cache.context(), "INSERT INTO A(id, name, surname) values (1, 'X', 'Y')");
                }
                finally {
                    execute("DROP TABLE A");
                }

                return null;
            }
        });
    }

    /**
     * Test that it's impossible to create tables with same name regardless of key/value wrapping settings.
     */
    public void testWrappedAndUnwrappedKeyTablesInteroperability() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                execute("create table a (id int primary key, x varchar)");

                try {
                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_key",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_value",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_key,wrap_value",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                execute("create table a (id int primary key, x varchar) with wrap_key");

                try {
                    assertDdlCommandThrows("create table a (id int primary key, x varchar)",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_value",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_key,wrap_value",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                execute("create table a (id int primary key, x varchar) with wrap_value");

                try {
                    assertDdlCommandThrows("create table a (id int primary key, x varchar)",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_key",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_key,wrap_value",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                execute("create table a (id int primary key, x varchar) with wrap_value");

                try {
                    assertDdlCommandThrows("create table a (id int primary key, x varchar)",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_value",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) with wrap_key",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                return null;
            }
        });
    }

    /**
     * Test that it's impossible to create tables with same name regardless of key/value wrapping settings.
     */
    public void testWrappedAndUnwrappedKeyTablesInteroperabilityInternal() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() throws Exception {

                execute("create table a (id int primary key, x varchar)");

                try {

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_key",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_value",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_key wrap_value",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                execute("create table a (id int primary key, x varchar) wrap_key");

                try {
                    assertDdlCommandThrows("create table a (id int primary key, x varchar)",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_value",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_key wrap_value",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                execute("create table a (id int primary key, x varchar) wrap_value");

                try {
                    assertDdlCommandThrows("create table a (id int primary key, x varchar)",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_key",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_key wrap_value",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                execute("create table a (id int primary key, x varchar) wrap_key wrap_value");

                try {
                    assertDdlCommandThrows("create table a (id int primary key, x varchar)",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_value",
                        "Table already exists: A");

                    assertDdlCommandThrows("create table a (id int primary key, x varchar) wrap_key",
                        "Table already exists: A");

                }
                finally {
                    execute("drop table a");
                }

                return null;
            }
        });
    }

    /**
     * Test that it's possible to create tables with matching key and/or value primitive types.
     */
    public void testDynamicTablesInteroperability() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                try {
                    execute("create table a (id int primary key, x varchar) with \"wrap_value=false\"");

                    execute("create table b (id long primary key, y varchar) with \"wrap_value=false\"");

                    execute("create table c (id int primary key, z long) with \"wrap_value=false\"");

                    execute("create table d (id int primary key, w varchar) with \"wrap_value=false\"");
                }
                finally {
                    execute("drop table a");

                    execute("drop table b");

                    execute("drop table c");

                    execute("drop table d");
                }

                return null;
            }
        });
    }

    /**
     * Test that it's possible to create tables with matching key and/or value primitive types.
     */
    public void testDynamicTablesInteroperabilityInternal() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {
                try {
                    execute("create table a (id int primary key, x varchar) wrap_value=false");

                    execute("create table b (id long primary key, y varchar) wrap_value=false");

                    execute("create table c (id int primary key, z long) wrap_value=false");

                    execute("create table d (id int primary key, w varchar) wrap_value=false");
                }
                finally {
                    execute("drop table a");

                    execute("drop table b");

                    execute("drop table c");

                    execute("drop table d");
                }

                return null;
            }
        });
    }

    /**
     * Test that when key or value has more than one column, wrap=false is forbidden.
     */
    public void testWrappingAlwaysOnWithComplexObjects() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                assertDdlCommandThrows("create table a (id int, x varchar, c long, primary key(id, c)) with \"wrap_key=false\"",
                    "WRAP_KEY cannot be false when composite primary key exists.");

                assertDdlCommandThrows("create table a (id int, x varchar, c long, primary key(id)) with \"wrap_value=false\"",
                    "WRAP_VALUE cannot be false when multiple non-primary key columns exist.");

                return null;
            }
        });
    }

    /**
     * Test that when key or value has more than one column, wrap=false is forbidden.
     */
    public void testWrappingAlwaysOnWithComplexObjectsInternal() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                assertDdlCommandThrows("create table a (id int, x varchar, c long, primary key(id, c)) wrap_key=false",
                    "WRAP_KEY cannot be false when composite primary key exists.");

                assertDdlCommandThrows("create table a (id int, x varchar, c long, primary key(id)) wrap_value=false",
                    "WRAP_VALUE cannot be false when multiple non-primary key columns exist.");

                return null;
            }
        });
    }

    /**
     * Test behavior when neither key nor value should be wrapped.
     * @throws SQLException if failed.
     */
    public void testNoWrap() throws Exception {
        doTestKeyValueWrap(false, false, false);
    }

    /**
     * Test behavior when only key is wrapped.
     * @throws SQLException if failed.
     */
    public void testKeyWrap() throws Exception {
        doTestKeyValueWrap(true, false, false);
    }

    /**
     * Test behavior when only value is wrapped.
     * @throws SQLException if failed.
     */
    public void testValueWrap() throws Exception {
        doTestKeyValueWrap(false, true, false);
    }

    /**
     * Test behavior when both key and value is wrapped.
     * @throws SQLException if failed.
     */
    public void testKeyAndValueWrap() throws Exception {
        doTestKeyValueWrap(true, true, false);
    }

    /**
     * Test behavior when neither key nor value should be wrapped.
     * @throws SQLException if failed.
     */
    public void testNoWrapInternal() throws Exception {
        doTestKeyValueWrap(false, false, true);
    }

    /**
     * Test behavior when only key is wrapped.
     * @throws SQLException if failed.
     */
    public void testKeyWrapInternal() throws Exception {
        doTestKeyValueWrap(true, false, true);
    }

    /**
     * Test behavior when only value is wrapped.
     * @throws SQLException if failed.
     */
    public void testValueWrapInternal() throws Exception {
        doTestKeyValueWrap(false, true, true);
    }

    /**
     * Test behavior when both key and value is wrapped.
     * @throws SQLException if failed.
     */
    public void testKeyAndValueWrapInternal() throws Exception {
        doTestKeyValueWrap(true, true, true);
    }

    /**
     * Test behavior for given combination of wrap flags.
     * @param wrapKey Whether key wrap should be enforced.
     * @param wrapVal Whether value wrap should be enforced.
     * @param useInternalCmd Use internal CREATE TABLE command instead of H2 one.
     * @throws SQLException if failed.
     */
    private void doTestKeyValueWrap(final boolean wrapKey, final boolean wrapVal, final boolean useInternalCmd)
        throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() throws Exception {

                try {
                    String sql;

                    if (useInternalCmd) {
                        sql = String.format("CREATE TABLE T (\"id\" int primary key, \"x\" varchar) " +
                            "wrap_key=%b wrap_value=%b", wrapKey, wrapVal);

                        if (wrapKey)
                            sql += " key_type=tkey";

                        if (wrapVal)
                            sql += " value_type=tval";
                    }
                    else {
                        sql = String.format("CREATE TABLE T (\"id\" int primary key, \"x\" varchar) WITH " +
                            "\"wrap_key=%b,wrap_value=%b\"", wrapKey, wrapVal);

                        if (wrapKey)
                            sql += ",\"key_type=tkey\"";

                        if (wrapVal)
                            sql += ",\"value_type=tval\"";
                    }

                    execute(sql);

                    execute("INSERT INTO T(\"id\", \"x\") values(1, 'a')");

                    LinkedHashMap<String, String> resCols = new LinkedHashMap<>();

                    List<Object> resData = new ArrayList<>();

                    try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
                        try (ResultSet colsRs = conn.getMetaData().getColumns(null, QueryUtils.DFLT_SCHEMA, "T", ".*")) {
                            while (colsRs.next())
                                resCols.put(colsRs.getString("COLUMN_NAME"),
                                    DataType.getTypeClassName(DataType.convertSQLTypeToValueType(colsRs
                                        .getShort("DATA_TYPE"))));
                        }

                        try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM T")) {
                            try (ResultSet dataRs = ps.executeQuery()) {
                                assertTrue(dataRs.next());

                                for (int i = 0; i < dataRs.getMetaData().getColumnCount(); i++)
                                    resData.add(dataRs.getObject(i + 1));
                            }
                        }
                    }

                    LinkedHashMap<String, String> expCols = new LinkedHashMap<>();

                    expCols.put("id", Integer.class.getName());
                    expCols.put("x", String.class.getName());

                    assertEquals(expCols, resCols);

                    assertEqualsCollections(Arrays.asList(1, "a"), resData);

                    Object key = createKeyForWrapTest(1, wrapKey);

                    Object val = client().cache(cacheName("T")).withKeepBinary().get(key);

                    assertNotNull(val);

                    assertEquals(createValueForWrapTest("a", wrapVal), val);
                }
                finally {
                    execute("DROP TABLE IF EXISTS T");
                }

                return null;
            }
        });
    }

    /**
     * @param key Key to wrap.
     * @param wrap Whether key should be wrapped.
     * @return (optionally wrapped) key.
     */
    private Object createKeyForWrapTest(int key, boolean wrap) {
        if (!wrap)
            return key;

        return client().binary().builder("tkey").setField("id", key).build();
    }

    /**
     * @param val Value to wrap.
     * @param wrap Whether value should be wrapped.
     * @return (optionally wrapped) value.
     */
    private Object createValueForWrapTest(String val, boolean wrap) {
        if (!wrap)
            return val;

        return client().binary().builder("tval").setField("x", val).build();
    }

    /**
     * Fill re-created table with data.
     */
    private void fillRecreatedTable() {
        for (int j = 1; j < 10; j++) {
            String s = Integer.toString(j);
            execute("insert into \"PUBLIC\".t (a,b) values (" + s + ", '" + s + "')");
        }
    }

    /**
     * Check that dynamic cache created with {@code CREATE TABLE} is correctly configured affinity wise.
     * @param cacheName Cache name to check.
     * @param affKeyFieldName Expected affinity key field name.
     */
    private void assertAffinityCacheConfiguration(String cacheName, String affKeyFieldName) {
        String actualCacheName = cacheName(cacheName);

        Collection<GridQueryTypeDescriptor> types = client().context().query().types(actualCacheName);

        assertEquals(1, types.size());

        GridQueryTypeDescriptor type = types.iterator().next();

        assertTrue(type.name().startsWith(actualCacheName));
        assertEquals(cacheName, type.tableName());
        assertEquals(affKeyFieldName, type.affinityKey());

        GridH2Table tbl = ((IgniteH2Indexing)queryProcessor(client()).getIndexing()).dataTable("PUBLIC", cacheName);

        assertNotNull(tbl);

        assertNotNull(tbl.getAffinityKeyColumn());

        assertEquals(affKeyFieldName, tbl.getAffinityKeyColumn().columnName);
    }

    /**
     * Execute {@code CREATE TABLE} w/given params.
     * @param params Engine parameters.
     * @param useInternalCmd Use internal CREATE TABLE command
     */
    private void createTableWithParams(final String params, final boolean useInternalCmd) throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(useInternalCmd, new Callable<Void>() {
            @Override public Void call() throws Exception {

                if (useInternalCmd)
                    execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar" +
                        ", \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                        "template=cache " + params);
                else
                    execute("CREATE TABLE \"Person\" (\"id\" int, \"city\" varchar" +
                        ", \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                        "\"template=cache," + params + '"');

                return null;
            }
        });
    }

    /**
     * Test that {@code CREATE TABLE} in non-public schema causes an exception.
     *
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testCreateTableInNonPublicSchema() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(false, new Callable<Void>() {
            @Override public Void call() {

                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        execute("CREATE TABLE \"cache_idx\".\"Person\" (\"id\" int, \"city\" varchar," +
                            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) WITH " +
                            "\"template=cache\"");

                        return null;
                    }
                }, IgniteSQLException.class, "CREATE TABLE can only be executed on PUBLIC schema.");

                return null;
            }
        });
    }

    /**
     * Test that {@code CREATE TABLE} in non-public schema causes an exception.
     *
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testInternalCreateTableInNonPublicSchema() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                GridTestUtils.assertThrows(null, new Callable<Object>() {
                    @Override public Object call() throws Exception {

                        execute("CREATE TABLE \"cache_idx\".\"Person\" (\"id\" int, \"city\" varchar," +
                            " \"name\" varchar, \"surname\" varchar, \"age\" int, PRIMARY KEY (\"id\", \"city\")) " +
                            "template=cache");

                        return null;
                    }
                }, IgniteSQLException.class, "CREATE TABLE can only be executed on PUBLIC schema.");

                return null;
            }
        });
    }

    /**
     * Execute {@code CREATE TABLE} w/given params expecting a particular error.
     * @param params Engine parameters.
     * @param expErrMsg Expected error message.
     * @param useInternalCmd Use internal CREATE TABLE command
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertCreateTableWithParamsThrows(final String params, String expErrMsg, final boolean useInternalCmd) {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                createTableWithParams(params, useInternalCmd);

                return null;
            }
        }, IgniteSQLException.class, expErrMsg);
    }

    /**
     * Test that arbitrary command yields specific error.
     * @param cmd Command.
     * @param expErrMsg Expected error message.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertDdlCommandThrows(final String cmd, String expErrMsg) {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                execute(cmd);

                return null;
            }
        }, IgniteSQLException.class, expErrMsg);
    }

    /**
     * Test that {@code DROP TABLE} on non-public schema causes an exception.
     *
     * @throws Exception if failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testDropTableNotPublicSchema() throws Exception {
       assertDdlCommandThrows("DROP TABLE \"cache_idx\".\"Person\"",
           "DROP TABLE can only be executed on PUBLIC schema.");
    }

    /**
     * Test that {@link IgniteH2Indexing#tables(String)} method
     * only returns tables belonging to given cache.
     *
     * @throws Exception if failed.
     */
    public void testGetTablesForCache() throws Exception {
        try {
            execute("create table t1(id int primary key, name varchar)");
            execute("create table t2(id int primary key, name varchar)");

            IgniteH2Indexing h2Idx = (IgniteH2Indexing)grid(0).context().query().getIndexing();

            String cacheName = cacheName("T1");

            Collection<H2TableDescriptor> col = GridTestUtils.invoke(h2Idx, "tables", cacheName);

            assertNotNull(col);

            H2TableDescriptor[] tables = col.toArray(new H2TableDescriptor[col.size()]);

            assertEquals(1, tables.length);

            assertEquals(tables[0].table().getName(), "T1");
        }
        finally {
            execute("drop table t1 if exists");
            execute("drop table t2 if exists");
        }
    }

    /**
     * Tests for reserved columns definitions in the internal CREATE TABLE command.
     *
     * @throws AssertionError If failed.
     */
    public void testReservedColumnNamesInternal() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                assertDdlCommandThrows(
                    "CREATE TABLE tbl (_KEY INT)",
                    "Direct specification of _KEY and _VAL columns is forbidden");

                assertDdlCommandThrows(
                    "CREATE TABLE tbl (_val INT)",
                    "Direct specification of _KEY and _VAL columns is forbidden");

                return null;
            }
        });
    }

    /**
     * Tests for primary key column definition errors in the internal CREATE TABLE command.
     *
     * @throws AssertionError If failed.
     */
    public void testPrimaryKeyColumnsInternal() throws Exception {
        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                assertDdlCommandThrows(
                    "CREATE TABLE tbl (a INT)",
                    "No PRIMARY KEY columns specified");

                assertDdlCommandThrows(
                    "CREATE TABLE tbl (a INT, b VARCHAR, PRIMARY KEY (a, b))",
                    "Table must have at least one non PRIMARY KEY column.");

                return null;
            }
        });
    }

    /**
     * Tests for wrapping-related parameters for the internal CREATE TABLE command.
     *
     * @throws AssertionError If failed.
     */
    public void testCreateTableWrapParamsInternal() throws Exception {

        GridTestUtils.runWithH2FallbackDisabled(true, new Callable<Void>() {
            @Override public Void call() {

                assertDdlCommandThrows(
                    "CREATE TABLE tbl (a INT, b LONG, c VARCHAR, PRIMARY KEY (a)) WRAP_KEY=0 KEY_TYPE KeyType",
                    "WRAP_KEY cannot be false when KEY_TYPE is set.");

                assertDdlCommandThrows(
                    "CREATE TABLE tbl (a INT, b LONG, c VARCHAR, d INT, PRIMARY KEY (a, b, c)) WRAP_KEY WRAP_VALUE=0 VALUE_TYPE ValueType",
                    "WRAP_VALUE cannot be false when VALUE_TYPE is set.");

                assertDdlCommandThrows(
                    "CREATE TABLE tbl (a INT, b LONG, c VARCHAR, d INT, PRIMARY KEY (a, b)) KEY_TYPE KvType VALUE_TYPE KvType",
                    "Key and value type names should be different for CREATE TABLE: KVTYPE");

                return null;
            }
        });
    }

    /**
     * Execute DDL statement on client node.
     *
     * @param sql Statement.
     */
    private void execute(String sql) {
        execute(client(), sql);
    }

    /**
     * Check that a property in given descriptor is present and has parameters as expected.
     * @param desc Descriptor.
     * @param name Property name.
     * @param type Expected property type.
     * @param isKey {@code true} if the property is expected to belong to key, {@code false} is it's expected to belong
     *     to value.
     */
    private void assertProperty(QueryTypeDescriptorImpl desc, String name, Class<?> type, boolean isKey) {
        GridQueryProperty p = desc.property(name);

        assertNotNull(name, p);

        assertEquals(type, p.type());

        assertEquals(isKey, p.key());
    }

    /**
     * Get configurations to be used in test.
     *
     * @return Configurations.
     * @throws Exception If failed.
     */
    private List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
            serverConfiguration(0),
            serverConfiguration(1),
            clientConfiguration(2),
            serverConfiguration(3)
        );
    }

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return commonConfiguration(idx);
    }

    /**
     * Create client configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration().setName(DATA_REGION_NAME);

        cfg.setMarshaller(new BinaryMarshaller());
        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDataRegionConfigurations(dataRegionCfg));

        return optimize(cfg);
    }

    /**
     * Execute DDL statement on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    private List<List<?>> execute(Ignite node, String sql) {
        return queryProcessor(node).querySqlFieldsNoCache(new SqlFieldsQuery(sql).setSchema("PUBLIC"), true).getAll();
    }

    /**
     * Execute DDL statement on given node.
     *
     * @param sql Statement.
     */
    private List<List<?>> executeLocal(GridCacheContext cctx, String sql) {
        return queryProcessor(cctx.grid()).querySqlFields(cctx, new SqlFieldsQuery(sql).setLocal(true), true).getAll();
    }

    /**
     * @return Client node.
     */
    private IgniteEx client() {
        return grid(CLIENT);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setSqlEscapeAll(true);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        return ccfg;
    }

    /**
     * @return Cache configuration with query entities - unfortunately, we need this to enable indexing at all.
     */
    private CacheConfiguration cacheConfigurationForIndexing() {
        CacheConfiguration<?, ?> ccfg = cacheConfiguration();

        ccfg.setName(INDEXED_CACHE_NAME);

        ccfg.setQueryEntities(Collections.singletonList(
            new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(Integer.class.getName())
        ));

        return ccfg;
    }

    /**
     * @return Cache configuration with query entities in {@code PUBLIC} schema.
     */
    private CacheConfiguration cacheConfigurationForIndexingInPublicSchema() {
        return cacheConfigurationForIndexing()
            .setName(INDEXED_CACHE_NAME_2)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setNodeFilter(F.not(new DynamicIndexAbstractSelfTest.NodeFilter()));
    }

    /**
     * Get cache name.
     *
     * @param tblName Table name.
     * @return Cache name.
     */
    private static String cacheName(String tblName) {
        return QueryUtils.createTableCacheName("PUBLIC", tblName);
    }
}
