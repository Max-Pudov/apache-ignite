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

package org.apache.ignite.internal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.cache.eviction.fifo.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class GridCacheConcurrentEvictionsSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Replicated cache. */
    private CacheMode mode = REPLICATED;

    /** */
    private CacheEvictionPolicy<?, ?> plc;

    /** */
    private CacheEvictionPolicy<?, ?> nearPlc;

    /** */
    private int warmUpPutsCnt;

    /** */
    private int iterCnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionConfiguration().setDefaultTxIsolation(READ_COMMITTED);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(mode);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setDistributionMode(PARTITIONED_ONLY);

        cc.setEvictionPolicy(plc);
        cc.setNearEvictionPolicy(nearPlc);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        plc = null;
        nearPlc = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentPutsFifoLocal() throws Exception {
        mode = LOCAL;
        plc = new CacheFifoEvictionPolicy<Object, Object>(1000);
        nearPlc = null;
        warmUpPutsCnt = 100000;
        iterCnt = 100000;

        checkConcurrentPuts();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentPutsLruLocal() throws Exception {
        mode = LOCAL;
        plc = new CacheLruEvictionPolicy<Object, Object>(1000);
        nearPlc = null;
        warmUpPutsCnt = 100000;
        iterCnt = 100000;

        checkConcurrentPuts();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkConcurrentPuts() throws Exception {
        try {
            Ignite ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.jcache(null);

            // Warm up.
            for (int i = 0; i < warmUpPutsCnt; i++) {
                cache.put(i, i);

                if (i != 0 && i % 1000 == 0)
                    info("Warm up puts count: " + i);
            }

            info("Cache size: " + cache.size());

            cache.removeAll();

            final AtomicInteger idx = new AtomicInteger();

            int threadCnt = 30;

            long start = System.currentTimeMillis();

            IgniteInternalFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        for (int i = 0; i < iterCnt; i++) {
                            int j = idx.incrementAndGet();

                            cache.put(j, j);

                            if (i != 0 && i % 10000 == 0)
                                // info("Puts count: " + i);
                                info("Stats [putsCnt=" + i + ", size=" + cache.size() + ']');
                        }

                        return null;
                    }
                },
                threadCnt
            );

            fut.get();

            info("Test results [threadCnt=" + threadCnt + ", iterCnt=" + iterCnt + ", cacheSize=" + cache.size() +
                ", duration=" + (System.currentTimeMillis() - start) + ']');
        }
        finally {
            stopAllGrids();
        }
    }
}
