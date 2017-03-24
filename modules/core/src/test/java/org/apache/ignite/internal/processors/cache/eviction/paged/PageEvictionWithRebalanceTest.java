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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public abstract class PageEvictionWithRebalanceTest extends PageEvictionAbstractTest {
    /**
     *
     */
    public void testEvictionWithRebalance() throws Exception {
        startGridsMultiThreaded(4);

        CacheConfiguration<Object, Object> cfg = cacheConfig("evict-rebalance", null, CacheMode.PARTITIONED,
            CacheAtomicityMode.ATOMIC, CacheWriteSynchronizationMode.PRIMARY_SYNC);

        IgniteCache<Object, Object> cache = ignite(0).getOrCreateCache(cfg);

        for (int i = 1; i <= ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            if (r.nextInt() % 5 == 0)
                cache.put(i, new TestObject(PAGE_SIZE / 4 - 50 + r.nextInt(5000))); // Fragmented object.
            else
                cache.put(i, new TestObject(r.nextInt(PAGE_SIZE / 4 - 50))); // Fits in one page.

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries put: " + i);
        }

        int size = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + size);

        // More than half of entries evicted, no OutOfMemory occurred, success.
        assertTrue(size < ENTRIES);

        for (int i = 3; i >= 1; i--) {
            stopGrid(i);

            cache.rebalance().get();

            awaitPartitionMapExchange();

            int rebalanceSize = cache.size(CachePeekMode.PRIMARY);

            System.out.println(">>> Size after rebalance: " + rebalanceSize);

            assertTrue(rebalanceSize < size);

            size = rebalanceSize;
        }
    }

    /**
     *
     */
    public static class RandomLru extends PageEvictionWithRebalanceTest {
        /** {@inheritDoc} */
        @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
            return setEvictionMode(DataPageEvictionMode.RANDOM_LRU, super.getConfiguration(gridName));
        }
    }

    /**
     *
     */
    public static class Random2Lru extends PageEvictionWithRebalanceTest {
        /** {@inheritDoc} */
        @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
            return setEvictionMode(DataPageEvictionMode.RANDOM_2_LRU, super.getConfiguration(gridName));
        }
    }
}
