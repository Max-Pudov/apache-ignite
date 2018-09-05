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
package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 * Tests for a lazy MVCC processor start.
 */
@SuppressWarnings("unchecked")
public class CacheMvccProcessorLazyStartTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreconfiguredCacheMvccNotStarted() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        IgniteConfiguration cfg = getConfiguration();
        cfg.setCacheConfiguration(ccfg);

        IgniteEx node = startGrid(cfg);

        IgniteCache cache = node.cache(ccfg.getName());

        cache.put(1, 1);
        cache.put(1, 2);

        assertFalse(mvccEnabled(node));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreconfiguredCacheMvccStarted() throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteConfiguration cfg = getConfiguration();
        cfg.setCacheConfiguration(ccfg);

        IgniteEx node = startGrid(cfg);

        IgniteCache cache = node.cache(ccfg.getName());

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccRestartedWithDynamicCache() throws Exception {
        persistence = true;

        IgniteEx node = startGrid(1);

        assertFalse(mvccEnabled(node));

        node.cluster().active(true);

        assertFalse(mvccEnabled(node));

        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteCache cache = node.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));

        stopGrid(1);

        node = startGrid(1);

        node.cluster().active(true);

        assertTrue(mvccEnabled(node));

        cache = node.cache(ccfg.getName());

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccStartedWithDynamicCache() throws Exception {
        IgniteEx node = startGrid(1);

        assertFalse(mvccEnabled(node));

        CacheConfiguration ccfg = cacheConfiguration(CacheMode.PARTITIONED, CacheWriteSynchronizationMode.FULL_SYNC, 0, 1);

        IgniteCache cache = node.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));

        stopGrid(1);

        node = startGrid(1);

        // Should not be started because we do not have persistence enabled
        assertFalse(mvccEnabled(node));

        cache = node.createCache(ccfg);

        cache.put(1, 1);
        cache.put(1, 2);

        assertTrue(mvccEnabled(node));
    }

    /**
     * @param node Node.
     * @return {@code True} if {@link MvccProcessor} is started.
     */
    private boolean mvccEnabled(IgniteEx node) {
        return node.context().coordinators().mvccEnabled();
    }
}
