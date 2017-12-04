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

import java.util.Arrays;
import java.util.HashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Simple index creation benchmark.
 */
public class IndexSimpleBenchmarkTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Index name. */
    private static final String INDEX_NAME = "IDX";

    /** Keys count. */
    private static final int KEYS = 6_000_000;

    /** Fields count. */
    private static final int FIELDS = 10;

    /** Fields count. */
    private static final String TABLE_NAME = "VALUE";

    /** Field name prefix. */
    private static final String FIELD_PREF = "FIELD_";

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 3600 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(new CacheConfiguration().setName(CACHE_NAME).setQueryEntities(Arrays.asList(
            new QueryEntity(Long.class.getName(), TABLE_NAME).setTableName(TABLE_NAME)
                .addQueryField(FIELD_PREF + 0, String.class.getName(), null))));

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setWalMode(WALMode.LOG_ONLY)
            .setCheckpointFrequency(Long.MAX_VALUE)
            .setWriteThrottlingEnabled(true)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setName("HugeCheckpoint")
                    .setPersistenceEnabled(true)
                    .setMaxSize(4L * 1024 * 1024 * 1024)
                    .setInitialSize(4L * 1024 * 1024 * 1024)
                    .setCheckpointPageBufferSize(256 * 1024 * 1024)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        startGrid();

        grid().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception On error.
     */
    public void testBenchmark() throws Exception {
        fillData();

        System.out.println("+++ warmup");
        createIdx();

        System.out.println("+++ benchmark");
        while (true) {
            GridCacheMapEntry.OFFHEAP_HACK = false;

            createIdx();

            GridCacheMapEntry.OFFHEAP_HACK = true;

            createIdx();
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void createIdx() throws IgniteCheckedException {
        long t0 = System.currentTimeMillis();

        grid().context().query().dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TABLE_NAME,
            new QueryIndex(FIELD_PREF + 0).setName(INDEX_NAME), false, 0).get();

        System.out.println("+++ Index build (HACK=" + GridCacheMapEntry.OFFHEAP_HACK + "): "
            + ((System.currentTimeMillis() - t0) / 1000.0));

        grid().context().query().dynamicIndexDrop(CACHE_NAME, CACHE_NAME, INDEX_NAME, false).get();

        System.gc();

        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * @throws Exception On error.
     */
    private void fillData() throws Exception {
        int batchSize = 100_000;

        for (long i = 0; i < KEYS / batchSize; ++i) {
            System.out.println("Fill data: " + i * batchSize);

            HashMap<Long, BinaryObject> data = new HashMap<>(batchSize);

            for (int j = 0; j < batchSize; ++j) {
                BinaryObjectBuilder bob = grid().binary().builder(TABLE_NAME);

                for (int fld = 0; fld < FIELDS; ++fld)
                    bob.setField(FIELD_PREF + fld, "value_" + fld + "_" + i);

                data.put(i * batchSize + j, bob.build());
            }

            grid().cache(CACHE_NAME).putAll(data);
        }
    }

}
