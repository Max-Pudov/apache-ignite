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

package org.apache.ignite.client;

/**
 * Configuration required to initialize {@link TcpClientChannel}.
 */
final class ClientChannelConfiguration {
    /** Host. */
    private String host;

    /** Port. */
    private int port;

    /** Ssl mode. */
    private SslMode sslMode;

    /** Tcp no delay. */
    private boolean tcpNoDelay;

    /** Timeout. */
    private int timeout;

    /** Send buffer size. */
    private int sndBufSize;

    /** Receive buffer size. */
    private int rcvBufSize;

    /** Ssl client certificate key store path. */
    private String sslClientCertKeyStorePath;

    /** Ssl client certificate key store type. */
    private String sslClientCertKeyStoreType;

    /** Ssl client certificate key store password. */
    private String sslClientCertKeyStorePwd;

    /** Ssl trust certificate key store path. */
    private String sslTrustCertKeyStorePath;

    /** Ssl trust certificate key store type. */
    private String sslTrustCertKeyStoreType;

    /** Ssl trust certificate key store password. */
    private String sslTrustCertKeyStorePwd;

    /** Ssl key algorithm. */
    private String sslKeyAlgorithm;

    /** Ssl protocol. */
    private String sslProto;

    /** Ssl trust all. */
    private boolean sslTrustAll;

    /** Credential provider. */
    private CredentialsProvider credProvider;

    /**
     * Constructor.
     */
    ClientChannelConfiguration(IgniteClientConfiguration cfg) {
        this.host = cfg.getHost();
        this.port = cfg.getPort();
        this.sslMode = cfg.getSslMode();
        this.tcpNoDelay = cfg.isTcpNoDelay();
        this.timeout = cfg.getTimeout();
        this.sndBufSize = cfg.getSendBufferSize();
        this.rcvBufSize = cfg.getReceiveBufferSize();
        this.sslClientCertKeyStorePath = cfg.getSslClientCertificateKeyStorePath();
        this.sslClientCertKeyStoreType = cfg.getSslClientCertificateKeyStoreType();
        this.sslClientCertKeyStorePwd = cfg.getSslClientCertificateKeyStorePassword();
        this.sslTrustCertKeyStorePath = cfg.getSslTrustCertificateKeyStorePath();
        this.sslTrustCertKeyStoreType = cfg.getSslTrustCertificateKeyStoreType();
        this.sslTrustCertKeyStorePwd = cfg.getSslTrustCertificateKeyStorePassword();
        this.sslKeyAlgorithm = cfg.getSslKeyAlgorithm();
        this.sslProto = cfg.getSslProtocol();
        this.sslTrustAll = cfg.isSslTrustAll();
        this.credProvider = cfg.getCredentialsProvider();
    }

    /**
     * @return Host.
     */
    public String getHost() {
        return host;
    }

    /**
     * @param newVal Host.
     */
    public ClientChannelConfiguration setHost(String newVal) {
        host = newVal;

        return this;
    }

    /**
     * @return Port.
     */
    public int getPort() {
        return port;
    }

    /**
     * @param newVal Port.
     */
    public ClientChannelConfiguration setPort(int newVal) {
        port = newVal;

        return this;
    }

    /**
     * @return SSL Mode.
     */
    public SslMode getSslMode() {
        return sslMode;
    }

    /**
     * @return Tcp no delay.
     */
    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * @return Timeout.
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @return Send buffer size.
     */
    public int getSendBufferSize() {
        return sndBufSize;
    }

    /**
     * @return Receive buffer size.
     */
    public int getReceiveBufferSize() {
        return rcvBufSize;
    }

    /**
     * @return Ssl client certificate key store path.
     */
    public String getSslClientCertificateKeyStorePath() {
        return sslClientCertKeyStorePath;
    }

    /**
     * @return Ssl client certificate key store type.
     */
    public String getSslClientCertificateKeyStoreType() {
        return sslClientCertKeyStoreType;
    }

    /**
     * @return Ssl client certificate key store password.
     */
    public String getSslClientCertificateKeyStorePassword() {
        return sslClientCertKeyStorePwd;
    }

    /**
     * @return Ssl trust certificate key store path.
     */
    public String getSslTrustCertificateKeyStorePath() {
        return sslTrustCertKeyStorePath;
    }

    /**
     * @return Ssl trust certificate key store type.
     */
    public String getSslTrustCertificateKeyStoreType() {
        return sslTrustCertKeyStoreType;
    }

    /**
     * @return Ssl trust certificate key store password.
     */
    public String getSslTrustCertificateKeyStorePassword() {
        return sslTrustCertKeyStorePwd;
    }

    /**
     * @return Ssl key algorithm.
     */
    public String getSslKeyAlgorithm() {
        return sslKeyAlgorithm;
    }

    /**
     * @return SSL Protocol.
     */
    public String getSslProtocol() {
        return sslProto;
    }

    /**
     * @return SSL Trust All.
     */
    public boolean isSslTrustAll() {
        return sslTrustAll;
    }

    /**
     * @return Credentials provider.
     */
    public CredentialsProvider getCredentialsProvider() {
        return credProvider;
    }
}
