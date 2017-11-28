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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
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

    /** Keys count. */
    private static final int KEYS = 1_500_0;

    /** Fields count. */
    private static final int FIELDS = 10;

    /** Fields count. */
    private static final String TABLE_NAME = "Value";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(new CacheConfiguration().setName(CACHE_NAME).setQueryEntities(Arrays.asList(
            new QueryEntity(Integer.class.getName(), TABLE_NAME)
                .addQueryField("field_0", String.class.getName(), null))));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception On error.
     */
    public void testBenchmark() throws Exception {
        IgniteEx ig = grid();

        fillData();

        System.out.println("+++ Begin creating index");

        long t0 = System.currentTimeMillis();

        ig.context().query().dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TABLE_NAME, new QueryIndex("field_0"), false);

        System.out.println("Index build: " + ((System.currentTimeMillis() - t0) / 1000.0));
    }

    /**
     * @throws Exception On error.
     */
    private void fillData() throws Exception {
        try(IgniteDataStreamer streamer = grid().dataStreamer(CACHE_NAME)) {
            for (int i = 0; i < KEYS; ++i) {
                BinaryObjectBuilder bob = grid().binary().builder(TABLE_NAME);

                for (int fld = 0; fld < FIELDS; ++fld)
                    bob.setField("field_" + fld, "value_" + fld + "_" + i);

                streamer.addData(i, bob.build());

                if (i % 100_000 == 0)
                    System.out.println("Fill data: " + i);
            }
        }
    }

}
