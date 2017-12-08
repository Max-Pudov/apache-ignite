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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for correct distributed partitioned queries.
 */
@SuppressWarnings("unchecked")
public class IgniteSqlGroupConcatNotCollocatedTest extends GridCommonAbstractTest {
    /** */
    private static final int CLIENT = 7;

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(
            new CacheConfiguration(CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction().setPartitions(8))
                .setQueryEntities(Arrays.asList(new QueryEntity(Key.class, Value.class))));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);

        Ignition.setClientMode(true);
        try {
            startGrid(CLIENT);
        }
        finally {
            Ignition.setClientMode(false);
        }

        IgniteCache c = grid(CLIENT).cache(CACHE_NAME);

        int k = 0;

        for (int grp = 1; grp < 7; ++grp) {
            for (int i = 0; i < grp; ++i) {
                c.put(new Key(k, grp), new Value(k, "" + (char)('A' + k)));

                k++;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testGroupConcatException() {
        final IgniteCache c = ignite(CLIENT).cache(CACHE_NAME);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() {
                c.query(new SqlFieldsQuery("select grp, GROUP_CONCAT(str0) from Value group by grp")).getAll();

                return null;
            }
        }, IgniteException.class, "GROUP_CONCAT is unsupported for not collocated data");
    }

    /**
     *
     */
    public static class Key {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        private int grp;

        /**
         * @param id Id.
         * @param grp Group.
         */
        public Key(int id, int grp) {
            this.id = id;
            this.grp = grp;
        }
    }

    /**
     *
     */
    public static class Value {
        /** */
        @QuerySqlField
        private String str0;

        /** */
        @QuerySqlField
        private String str1;

        /** */
        @QuerySqlField
        private String strId;

        /**
         * @param id Id.
         * @param str String value.
         */
        public Value(int id, String str) {
            str0 = str;
            str1 = str + "_1";
            strId = "id#" + id;
        }
    }
}
