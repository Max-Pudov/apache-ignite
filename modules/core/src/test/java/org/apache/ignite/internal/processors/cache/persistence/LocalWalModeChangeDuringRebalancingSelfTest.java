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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class LocalWalModeChangeDuringRebalancingSelfTest extends GridCommonAbstractTest {
    /** */
    private static boolean disableWalDuringRebalancing = true;

    /** */
    private final AtomicReference<CountDownLatch> supplyMessageLatch = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(200 * 1024 * 1024)
                        .setInitialSize(200 * 1024 * 1024)
                )
                // Test verifies checkpoint count, so it is essencial that no checkpoint is triggered by timeout
                .setCheckpointFrequency(999_999_999_999L)
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                // Test checks internal state before and after rebalance, so it is configured to be triggered manually
                .setRebalanceDelay(-1)
        );

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        CountDownLatch latch0 = supplyMessageLatch.get();

                        if (latch0 != null)
                            try {
                                latch0.await();
                            }
                            catch (InterruptedException ex) {
                                throw new IgniteException(ex);
                            }
                    }
                }

                super.sendMessage(node, msg);
            }

            @Override public void sendMessage(ClusterNode node, Message msg,
                IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
                if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage) {
                    int grpId = ((GridDhtPartitionSupplyMessage)((GridIoMessage)msg).message()).groupId();

                    if (grpId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        CountDownLatch latch0 = supplyMessageLatch.get();

                        if (latch0 != null)
                            try {
                                latch0.await();
                            }
                            catch (InterruptedException ex) {
                                throw new IgniteException(ex);
                            }
                    }
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        System.setProperty(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING,
            Boolean.toString(disableWalDuringRebalancing));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        CountDownLatch latch = supplyMessageLatch.get();

        if (latch != null) {
            while (latch.getCount() > 0)
                latch.countDown();

            supplyMessageLatch.set(null);
        }

        stopAllGrids();

        cleanPersistenceDir();

        disableWalDuringRebalancing = true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalDisabledDuringRebalancing() throws Exception {
        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalNotDisabledIfParameterSetToFalse() throws Exception {
        disableWalDuringRebalancing = false;

        doTestSimple();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestSimple() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        long newIgniteStartedTimestamp = System.currentTimeMillis();

        ignite.cluster().setBaselineTopology(4);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertEquals(!disableWalDuringRebalancing, grpCtx.walEnabled());

        long rebalanceStartedTimestamp = System.currentTimeMillis();

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue(grpCtx.walEnabled());

        long rebalanceFinishedTimestamp = System.currentTimeMillis();

        for (Integer k = 0; k < 1000; k++)
            assertEquals("k=" + k, k, cache.get(k));

        GridCacheDatabaseSharedManager.CheckpointHistory cpHistory =
            ((GridCacheDatabaseSharedManager)newIgnite.context().cache().context().database()).checkpointHistory();

        int checkpointsBeforeNodeStarted = 0;
        int checkpointsBeforeRebalance = 0;
        int checkpointsAfterRebalance = 0;

        for (Long timestamp : cpHistory.checkpoints()) {
            if (timestamp < newIgniteStartedTimestamp)
                checkpointsBeforeNodeStarted++;
            else if (timestamp >= newIgniteStartedTimestamp && timestamp < rebalanceStartedTimestamp)
                checkpointsBeforeRebalance++;
            else if (timestamp >= rebalanceStartedTimestamp && timestamp <= rebalanceFinishedTimestamp)
                checkpointsAfterRebalance++;
        }

        assertEquals(1, checkpointsBeforeNodeStarted); // checkpoint on start
        assertEquals(0, checkpointsBeforeRebalance);
        assertEquals(disableWalDuringRebalancing ? 1 : 0, checkpointsAfterRebalance); // checkpoint if WAL was re-activated
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalAndGlobalWalStateInterdependence() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        ignite.cluster().setBaselineTopology(ignite.cluster().nodes());

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertFalse(grpCtx.walEnabled());

        ignite.cluster().disableWal(DEFAULT_CACHE_NAME);

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertFalse(grpCtx.walEnabled()); // WAL is globally disabled

        ignite.cluster().enableWal(DEFAULT_CACHE_NAME);

        assertTrue(grpCtx.walEnabled());
    }

    /**
     * @throws Exception If failed.
     */
    public void testParallelExchangeDuringRebalance() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 10_000; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        CountDownLatch latch = new CountDownLatch(1);

        supplyMessageLatch.set(latch);

        ignite.cluster().setBaselineTopology(ignite.cluster().nodes());

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        assertFalse(grpCtx.walEnabled());

        startGrid(4); // Trigger exchange

        assertFalse(grpCtx.walEnabled());

        latch.countDown();

        assertFalse(grpCtx.walEnabled());

        for (Ignite g : G.allGrids())
            g.cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue(grpCtx.walEnabled());
    }

    /**
     * @throws Exception If failed.
     */
    public void testParallelExchangeDuringCheckpoint() throws Exception {

    }
}
