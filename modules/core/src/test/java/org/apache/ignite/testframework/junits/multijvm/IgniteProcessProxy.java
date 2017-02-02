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

package org.apache.ignite.testframework.junits.multijvm;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cache.GridCacheUtilityKey;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.internal.util.lang.IgnitePredicateX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite proxy for ignite instance at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteProcessProxy extends IgniteNodeProxyBase implements IgniteEx {
    /**
     * @param cfg Configuration.
     * @param log Logger.
     * @param locJvmGrid Local JVM grid.
     */
    public IgniteProcessProxy(IgniteConfiguration cfg, IgniteLogger log, Ignite locJvmGrid)
        throws Exception {
        super(cfg, log, locJvmGrid, NodeProcessParameters.DFLT);
    }

    /**
     * @param gridName Grid name.
     * @return Instance by name or exception wiil be thrown.
     */
    public static IgniteProcessProxy ignite(String gridName) {
        IgniteNodeProxyBase res = gridProxies.get(gridName);

        if (res == null)
            throw new IgniteIllegalStateException("Grid instance was not properly started " +
                "or was already stopped: " + gridName + ". All known grid instances: " + gridProxies.keySet());

        return (IgniteProcessProxy)res;
    }

    /**
     * Gracefully shut down the Grid.
     *
     * @param gridName Grid name.
     * @param cancel If {@code true} then all jobs currently will be cancelled.
     */
    public static void stop(String gridName, boolean cancel) {
        IgniteNodeProxyBase proxy = gridProxies.get(gridName);

        if (proxy != null) {
            proxy.remoteCompute().run(new StopGridTask(gridName, cancel));

            gridProxies.remove(gridName, proxy);
        }
    }

    /**
     * Forcefully shut down the Grid.
     *
     * @param gridName Grid name.
     */
    public static void kill(String gridName) {
        IgniteNodeProxyBase proxy = gridProxies.get(gridName);

        A.notNull(gridName, "gridName");

        try {
            proxy.getProcess().kill();
        }
        catch (Exception e) {
            U.error(proxy.log, "Exception while killing " + gridName, e);
        }

        gridProxies.remove(gridName, proxy);
    }

    /**
     * @param locNodeId ID of local node the requested grid instance is managing.
     * @return An instance of named grid. This method never returns {@code null}.
     * @throws IgniteIllegalStateException Thrown if grid was not properly initialized or grid instance was stopped or
     * was not started.
     */
    public static Ignite ignite(UUID locNodeId) {
        A.notNull(locNodeId, "locNodeId");

        for (IgniteNodeProxyBase ignite : gridProxies.values()) {
            if (ignite.getId().equals(locNodeId))
                return (IgniteProcessProxy)ignite;
        }

        throw new IgniteIllegalStateException("Grid instance with given local node ID was not properly " +
            "started or was stopped: " + locNodeId);
    }

    /**
     * Kill all running processes.
     */
    public static void killAll() {
        for (IgniteNodeProxyBase ignite : gridProxies.values()) {
            try {
                ignite.getProcess().kill();
            }
            catch (Exception e) {
                U.error(ignite.log, "Killing failed.", e);
            }
        }

        gridProxies.clear();
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger log() {
        return log;
    }

    /** {@inheritDoc} */
    @Override public IgniteConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override public <K extends GridCacheUtilityKey, V> IgniteInternalCache<K, V> utilityCache() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> IgniteInternalCache<K, V> cachex(@Nullable String name) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K, V> IgniteInternalCache<K, V> cachex() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteInternalCache<?, ?>> cachesx(
        @Nullable IgnitePredicate<? super IgniteInternalCache<?, ?>>... p) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean eventUserRecordable(int type) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean allEventsUserRecordable(int[] types) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isJmxRemoteEnabled() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isRestartEnabled() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFileSystem igfsx(@Nullable String name) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Hadoop hadoop() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteClusterEx cluster() {
        return new IgniteClusterProcessProxy(this);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String latestVersion() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return remoteCompute().call(new NodeTask());
    }

    /** {@inheritDoc} */
    @Override public GridKernalContext context() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCompute compute(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteMessaging message(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events() {
        return new IgniteEventsProcessProxy(this);
    }

    /** {@inheritDoc} */
    @Override public IgniteEvents events(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteServices services(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ExecutorService executorService(ClusterGroup grp) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteScheduler scheduler() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override  public <K, V> IgniteCache<K, V> createNearCache(
        @Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg)
    {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> getOrCreateNearCache(@Nullable String cacheName,
        NearCacheConfiguration<K, V> nearCfg) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void destroyCaches(Collection<String> cacheNames) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteCache<K, V> cache(@Nullable final String name) {
        return new IgniteCacheProcessProxy<>(name, this);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() {
        return locJvmGrid.cacheNames();
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        throw new UnsupportedOperationException("Transactions can't be supported automatically in multi JVM mode.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteDataStreamer<K, V> dataStreamer(@Nullable String cacheName) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem fileSystem(String name) {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> fileSystems() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override  public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create)
        throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteAtomicReference<T> atomicReference(String name, @Nullable T initVal,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override  public <T, S> IgniteAtomicStamped<T, S> atomicStamped(
        String name,
        @Nullable T initVal,
        @Nullable S initStamp,
        boolean create) throws IgniteException
    {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe,
        boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteLock reentrantLock(String name, boolean failoverSafe,
        boolean fair, boolean create) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteQueue<T> queue(String name, int cap,
        @Nullable CollectionConfiguration cfg) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T> IgniteSet<T> set(String name, @Nullable CollectionConfiguration cfg) throws IgniteException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        throw new UnsupportedOperationException("Operation isn't supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        final CountDownLatch rmtNodeStoppedLatch = new CountDownLatch(1);

        locJvmGrid.events().localListen(new IgnitePredicateX<Event>() {
            @Override public boolean applyx(Event e) {
                if (((DiscoveryEvent)e).eventNode().id().equals(id)) {
                    rmtNodeStoppedLatch.countDown();

                    return false;
                }

                return true;
            }
        }, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);

        compute().run(new StopGridTask(localJvmGrid().name(), true));

        try {
            assert U.await(rmtNodeStoppedLatch, 15, TimeUnit.SECONDS) : "NodeId=" + id;
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }

        try {
            getProcess().kill();
        }
        catch (Exception e) {
            X.printerr("Could not kill process after close.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public <K> Affinity<K> affinity(String cacheName) {
        return new AffinityProcessProxy<>(cacheName, this);
    }

    /**
     *
     */
    private static class NodeTask implements IgniteCallable<ClusterNode> {
        /** Ignite. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public ClusterNode call() throws Exception {
            return ((IgniteEx)ignite).localNode();
        }
    }
}
