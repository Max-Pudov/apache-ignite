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

package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeProxyBase;
import org.apache.ignite.testframework.junits.multijvm.NodeProcessParameters;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Abstract class for Hadoop tests.
 */
public abstract class HadoopAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** REST port. */
    protected static final int REST_PORT = ConnectorConfiguration.DFLT_TCP_PORT;

    /** IGFS name. */
    protected static final String igfsName = null;

    /** IGFS name. */
    protected static final String igfsMetaCacheName = "meta";

    /** IGFS name. */
    protected static final String igfsDataCacheName = "data";

    /** IGFS block size. */
    protected static final int igfsBlockSize = 1024;

    /** IGFS block group size. */
    protected static final int igfsBlockGroupSize = 8;

    /** Initial REST port. */
    private int restPort = REST_PORT;

    /** Secondary file system REST endpoint configuration. */
    protected static final IgfsIpcEndpointConfiguration SECONDARY_REST_CFG;

    static {
        SECONDARY_REST_CFG = new IgfsIpcEndpointConfiguration();

        SECONDARY_REST_CFG.setType(IgfsIpcEndpointType.TCP);
        SECONDARY_REST_CFG.setPort(11500);
    }


    /** Initial classpath. */
    private static String initCp;

    /** {@inheritDoc} */
    @Override protected final void beforeTestsStarted() throws Exception {
        HadoopFileSystemsUtils.clearFileSystemCache();

        // Add surefire classpath to regular classpath.
        initCp = System.getProperty("java.class.path");

        String surefireCp = System.getProperty("surefire.test.class.path");

        if (surefireCp != null)
            System.setProperty("java.class.path", initCp + File.pathSeparatorChar + surefireCp);

        super.beforeTestsStarted();

        beforeTestsStarted0();
    }

    /**
     * Performs additional initialization in the beginning of test class execution.
     * @throws Exception If failed.
     */
    protected void beforeTestsStarted0() throws Exception {
        // noop
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        // Restore classpath.
        System.setProperty("java.class.path", initCp);

        initCp = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setHadoopConfiguration(hadoopConfiguration(gridName));

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        // Secondary Fs has its own filesystem config :
        if (igfsEnabled() && !gridName.contains("secondary")) {
            cfg.setCacheConfiguration(metaCacheConfiguration(), dataCacheConfiguration());

            cfg.setFileSystemConfiguration(igfsConfiguration());
        }

        if (restEnabled()) {
            ConnectorConfiguration clnCfg = new ConnectorConfiguration();

            clnCfg.setPort(restPort++);

            cfg.setConnectorConfiguration(clnCfg);
        }

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Hadoop configuration.
     */
    public HadoopConfiguration hadoopConfiguration(String gridName) {
        HadoopConfiguration cfg = new HadoopConfiguration();

        cfg.setMaxParallelTasks(3);

        return cfg;
    }

    /**
     * @return IGFS configuration.
     */
    public FileSystemConfiguration igfsConfiguration() throws Exception {
        FileSystemConfiguration cfg = new FileSystemConfiguration();

        cfg.setName(igfsName);
        cfg.setBlockSize(igfsBlockSize);
        cfg.setDataCacheName(igfsDataCacheName);
        cfg.setMetaCacheName(igfsMetaCacheName);
        cfg.setFragmentizerEnabled(false);

        return cfg;
    }

    /**
     * @return IGFS meta cache configuration.
     */
    public CacheConfiguration metaCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(igfsMetaCacheName);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return IGFS data cache configuration.
     */
    protected CacheConfiguration dataCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName(igfsDataCacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(igfsBlockGroupSize));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return {@code True} if IGFS is enabled on Hadoop nodes.
     */
    protected boolean igfsEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @return {@code True} if REST is enabled on Hadoop nodes.
     */
    protected boolean restEnabled() {
        return false;
    }

    /**
     * @return Number of nodes to start.
     */
    protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected Ignite startRemoteGrid(String gridName, IgniteConfiguration cfg,
        GridSpringResourceContext ctx) throws Exception {
        if (ctx != null)
            throw new UnsupportedOperationException("Starting of grid at another jvm by context doesn't supported.");

        if (cfg == null)
            cfg = optimize(getConfiguration(gridName));

        System.out.println("############## Starting remote grid [" +gridName + "]");

        int gridIdx = getTestGridIndex(gridName);

        IgniteEx node0 = grid(0);

        new IgniteNodeProxyBase(cfg, log, node0, getRemoteNodeParameters(gridIdx));

        return null;
    }

    /**
     * Gets special JVM parameters for specified remote node.
     *
     * @param nodeIdx Th enode index.
     * @return The node parameters.
     */
    NodeProcessParameters getRemoteNodeParameters(int nodeIdx) {
        assert nodeIdx > 0;

        return NodeProcessParameters.DFLT;
    }

    /** {@inheritDoc} */
    @Override protected void checkTopology(int cnt) throws Exception {
        if (isMultiJvm()) {
            for (int j = 0; j < 10; j++) {
                boolean topOk = true;

                for (int i = 0; i < cnt; i++) {
                    // Special treatment of remote nodes:
                    int size = i == 0 ? grid(i).cluster().nodes().size()
                        : IgniteNodeProxyBase.ignite(getTestGridName(i)).nodes().size();

                    if (cnt != size) {
                        U.warn(log, "Grid size is incorrect (will re-run check in 1000 ms) " +
                            "[name=" + grid(i).name() + ", size=" + size + ']');

                        topOk = false;

                        break;
                    }
                }

                if (topOk)
                    return;
                else
                    Thread.sleep(1000);
            }

            throw new Exception("Failed to wait for proper topology: " + cnt);
        }
        else
            super.checkTopology(cnt);
    }

    /**
     * @param cfg Config.
     */
    protected void setupFileSystems(Configuration cfg) {
        cfg.set("fs.defaultFS", igfsScheme());
        cfg.set("fs.igfs.impl", org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl", IgniteHadoopFileSystem.
            class.getName());

        HadoopFileSystemsUtils.setupFileSystems(cfg);
    }

    /**
     * @return IGFS scheme for test.
     */
    protected String igfsScheme() {
        return "igfs://@/";
    }
}