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

package org.apache.ignite.internal.jdbc.thin;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.nio.file.FileSystems;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.processors.odbc.ClientListenerNioListener;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.ClientListenerRequest;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBatchExecuteRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryFetchRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcRequest;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcResponse;
import org.apache.ignite.internal.util.ipc.loopback.IpcClientTcpEndpoint;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * JDBC IO layer implementation based on blocking IPC streams.
 */
public class JdbcThinTcpIo {
    /** Version 2.1.0. */
    private static final ClientListenerProtocolVersion VER_2_1_0 = ClientListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    private static final ClientListenerProtocolVersion VER_2_1_5 = ClientListenerProtocolVersion.create(2, 1, 5);

    /** Version 2.3.1. */
    private static final ClientListenerProtocolVersion VER_2_3_0 = ClientListenerProtocolVersion.create(2, 3, 0);

    /** Version 2.4.0. */
    private static final ClientListenerProtocolVersion VER_2_4_0 = ClientListenerProtocolVersion.create(2, 4, 0);

    /** Current version. */
    private static final ClientListenerProtocolVersion CURRENT_VER = VER_2_4_0;

    /** Initial output stream capacity for handshake. */
    private static final int HANDSHAKE_MSG_SIZE = 13;

    /** Initial output for query message. */
    private static final int DYNAMIC_SIZE_MSG_CAP = 256;

    /** Maximum batch query count. */
    private static final int MAX_BATCH_QRY_CNT = 32;

    /** Initial output for query fetch message. */
    private static final int QUERY_FETCH_MSG_SIZE = 13;

    /** Initial output for query fetch message. */
    private static final int QUERY_META_MSG_SIZE = 9;

    /** Initial output for query close message. */
    private static final int QUERY_CLOSE_MSG_SIZE = 9;

    /** Trust all certificates manager. */
    private final static X509TrustManager TRUST_ALL_MANAGER = new X509TrustManager() {
        @Override public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        @Override public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
        }

        @Override public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
        }
    };

    /** Connection properties. */
    private final ConnectionProperties connProps;

    /** Endpoint. */
    private IpcClientTcpEndpoint endpoint;

    /** Output stream. */
    private BufferedOutputStream out;

    /** Input stream. */
    private BufferedInputStream in;

    /** Closed flag. */
    private boolean closed;

    /** Ignite server version. */
    private IgniteProductVersion igniteVer;

    /**
     * Constructor.
     *
     * @param connProps Connection properties.
     */
    JdbcThinTcpIo(ConnectionProperties connProps) {
        this.connProps = connProps;
    }

    /**
     * @throws SQLException On connection error or reject.
     * @throws IOException On IO error in handshake.
     */
    public void start() throws SQLException, IOException {
        Socket sock;

        if (connProps.isUseSSL()) {
            try {
                SSLSocketFactory sslSocketFactory = getSSLSocketFactory();

                SSLSocket sock0 = (SSLSocket)sslSocketFactory.createSocket(connProps.getHost(), connProps.getPort());

                sock0.setUseClientMode(true);

                sock0.startHandshake();

                sock = sock0;
            }
            catch (IOException e) {
                throw new SQLException("Failed to SSL connect to server [host=" + connProps.getHost() +
                    ", port=" + connProps.getPort() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }
        else {
            sock = new Socket();

            try {
                sock.connect(new InetSocketAddress(connProps.getHost(), connProps.getPort()));
            }
            catch (IOException e) {
                throw new SQLException("Failed to connect to server [host=" + connProps.getHost() +
                    ", port=" + connProps.getPort() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        if (connProps.getSocketSendBuffer() != 0)
            sock.setSendBufferSize(connProps.getSocketSendBuffer());

        if (connProps.getSocketReceiveBuffer() != 0)
            sock.setReceiveBufferSize(connProps.getSocketReceiveBuffer());

        sock.setTcpNoDelay(connProps.isTcpNoDelay());

        try {
            endpoint = new IpcClientTcpEndpoint(sock);

            out = new BufferedOutputStream(endpoint.outputStream());
            in = new BufferedInputStream(endpoint.inputStream());
        }
        catch (IgniteCheckedException e) {
            throw new SQLException("Failed to connect to server [host=" + connProps.getHost() +
                ", port=" + connProps.getPort() + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }

        handshake(CURRENT_VER);
    }

    /**
     * Used for versions: 2.1.5 and 2.3.0. The protocol version is changed but handshake format isn't changed.
     *
     * @param ver JDBC client version.
     * @throws IOException On IO error.
     * @throws SQLException On connection reject.
     */
    public void handshake(ClientListenerProtocolVersion ver) throws IOException, SQLException {
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte) ClientListenerRequest.HANDSHAKE);

        writer.writeShort(ver.major());
        writer.writeShort(ver.minor());
        writer.writeShort(ver.maintenance());

        writer.writeByte(ClientListenerNioListener.JDBC_CLIENT);

        writer.writeBoolean(connProps.isDistributedJoins());
        writer.writeBoolean(connProps.isEnforceJoinOrder());
        writer.writeBoolean(connProps.isCollocated());
        writer.writeBoolean(connProps.isReplicatedOnly());
        writer.writeBoolean(connProps.isAutoCloseServerCursor());
        writer.writeBoolean(connProps.isLazy());
        writer.writeBoolean(connProps.isSkipReducerOnUpdate());

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()),
            null, null, false);

        boolean accepted = reader.readBoolean();

        if (accepted) {
            if (reader.available() > 0) {
                byte maj = reader.readByte();
                byte min = reader.readByte();
                byte maintenance = reader.readByte();

                String stage = reader.readString();

                long ts = reader.readLong();
                byte[] hash = reader.readByteArray();

                igniteVer = new IgniteProductVersion(maj, min, maintenance, stage, ts, hash);
            }
            else
                igniteVer = new IgniteProductVersion((byte)2, (byte)0, (byte)0, "Unknown", 0L, null);
        }
        else {
            short maj = reader.readShort();
            short min = reader.readShort();
            short maintenance = reader.readShort();

            String err = reader.readString();

            ClientListenerProtocolVersion srvProtocolVer = ClientListenerProtocolVersion.create(maj, min, maintenance);

            if (VER_2_3_0.equals(srvProtocolVer) || VER_2_1_5.equals(srvProtocolVer))
                handshake(srvProtocolVer);
            else if (VER_2_1_0.equals(srvProtocolVer))
                handshake_2_1_0();
            else {
                throw new SQLException("Handshake failed [driverProtocolVer=" + CURRENT_VER +
                    ", remoteNodeProtocolVer=" + srvProtocolVer + ", err=" + err + ']',
                    SqlStateCode.CONNECTION_REJECTED);
            }
        }
    }

    /**
     * Compatibility handshake for server version 2.1.0
     *
     * @throws IOException On IO error.
     * @throws SQLException On connection reject.
     */
    private void handshake_2_1_0() throws IOException, SQLException {
        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(HANDSHAKE_MSG_SIZE),
            null, null);

        writer.writeByte((byte) ClientListenerRequest.HANDSHAKE);

        writer.writeShort(VER_2_1_0.major());
        writer.writeShort(VER_2_1_0.minor());
        writer.writeShort(VER_2_1_0.maintenance());

        writer.writeByte(ClientListenerNioListener.JDBC_CLIENT);

        writer.writeBoolean(connProps.isDistributedJoins());
        writer.writeBoolean(connProps.isEnforceJoinOrder());
        writer.writeBoolean(connProps.isCollocated());
        writer.writeBoolean(connProps.isReplicatedOnly());
        writer.writeBoolean(connProps.isAutoCloseServerCursor());

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()),
            null, null, false);

        boolean accepted = reader.readBoolean();

        if (accepted)
            igniteVer = new IgniteProductVersion((byte)2, (byte)1, (byte)0, "Unknown", 0L, null);
        else {
            short maj = reader.readShort();
            short min = reader.readShort();
            short maintenance = reader.readShort();

            String err = reader.readString();

            ClientListenerProtocolVersion ver = ClientListenerProtocolVersion.create(maj, min, maintenance);

            throw new SQLException("Handshake failed [driverProtocolVer=" + CURRENT_VER +
                ", remoteNodeProtocolVer=" + ver + ", err=" + err + ']', SqlStateCode.CONNECTION_REJECTED);
        }
    }

    /**
     * @param req Request.
     * @return Server response.
     * @throws IOException In case of IO error.
     */
    @SuppressWarnings("unchecked")
    JdbcResponse sendRequest(JdbcRequest req) throws IOException {
        int cap = guessCapacity(req);

        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, new BinaryHeapOutputStream(cap), null, null);

        req.writeBinary(writer);

        send(writer.array());

        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, new BinaryHeapInputStream(read()), null, null, false);

        JdbcResponse res = new JdbcResponse();

        res.readBinary(reader);

        return res;
    }

    /**
     * Try to guess request capacity.
     *
     * @param req Request.
     * @return Expected capacity.
     */
    private static int guessCapacity(JdbcRequest req) {
        int cap;

        if (req instanceof JdbcBatchExecuteRequest) {
            int cnt = Math.min(MAX_BATCH_QRY_CNT, ((JdbcBatchExecuteRequest)req).queries().size());

            cap = cnt * DYNAMIC_SIZE_MSG_CAP;
        }
        else if (req instanceof JdbcQueryCloseRequest)
            cap = QUERY_CLOSE_MSG_SIZE;
        else if (req instanceof JdbcQueryMetadataRequest)
            cap = QUERY_META_MSG_SIZE;
        else if (req instanceof JdbcQueryFetchRequest)
            cap = QUERY_FETCH_MSG_SIZE;
        else
            cap = DYNAMIC_SIZE_MSG_CAP;

        return cap;
    }

    /**
     * @param req JDBC request bytes.
     * @throws IOException On error.
     */
    private void send(byte[] req) throws IOException {
        int size = req.length;

        out.write(size & 0xFF);
        out.write((size >> 8) & 0xFF);
        out.write((size >> 16) & 0xFF);
        out.write((size >> 24) & 0xFF);

        out.write(req);

        out.flush();
    }

    /**
     * @return Bytes of a response from server.
     * @throws IOException On error.
     */
    private byte[] read() throws IOException {
        byte[] sizeBytes = read(4);

        int msgSize  = (((0xFF & sizeBytes[3]) << 24) | ((0xFF & sizeBytes[2]) << 16)
            | ((0xFF & sizeBytes[1]) << 8) + (0xFF & sizeBytes[0]));

        return read(msgSize);
    }

    /**
     * @param size Count of bytes to read from stream.
     * @return Read bytes.
     * @throws IOException On error.
     */
    private byte [] read(int size) throws IOException {
        int off = 0;

        byte[] data = new byte[size];

        while (off != size) {
            int res = in.read(data, off, size - off);

            if (res == -1)
                throw new IOException("Failed to read incoming message (not enough data).");

            off += res;
        }

        return data;
    }

    /**
     * Close the client IO.
     */
    public void close() {
        if (closed)
            return;

        // Clean up resources.
        U.closeQuiet(out);
        U.closeQuiet(in);

        if (endpoint != null)
            endpoint.close();

        closed = true;
    }

    /**
     * @return Connection properties.
     */
    public ConnectionProperties connectionProperties() {
        return connProps;
    }

    /**
     * @return Ignite server version.
     */
    IgniteProductVersion igniteVersion() {
        return igniteVer;
    }

    /**
     * @return SSL socket factory.
     * @throws SQLException On error.
     */
    private SSLSocketFactory getSSLSocketFactory() throws SQLException {
        String sslFactory = connProps.getSslFactory();
        String cliCertKeyStoreUrl = connProps.getSslClientCertificateKeyStoreUrl();
        String cliCertKeyStorePwd = connProps.getSslClientCertificateKeyStorePassword();
        String cliCertKeyStoreType = connProps.getSslClientCertificateKeyStoreType();
        String trustCertKeyStoreUrl = connProps.getSslTrustCertificateKeyStoreUrl();
        String trustCertKeyStorePwd = connProps.getSslTrustCertificateKeyStorePassword();
        String trustCertKeyStoreType = connProps.getSslTrustCertificateKeyStoreType();
        String sslProtocol = connProps.sslProtocol();

        if (!F.isEmpty(sslFactory)) {
            try {
                Class<Factory<SSLSocketFactory>> cls = (Class<Factory<SSLSocketFactory>>)getClass()
                    .getClassLoader().loadClass(sslFactory);

                Factory<SSLSocketFactory> f = cls.newInstance();

                return f.create();
            }
            catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new SQLException("Could not fount SSL factory class: " + sslFactory,
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        if (cliCertKeyStoreUrl == null && cliCertKeyStorePwd == null && cliCertKeyStoreType == null
            && trustCertKeyStoreUrl == null && trustCertKeyStorePwd == null && trustCertKeyStoreType == null
            && connProps.sslProtocol() == null) {
            try {
                return SSLContext.getDefault().getSocketFactory();
            }
            catch (NoSuchAlgorithmException e) {
                throw new SQLException("Could not create default SSL context",
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }

        if (cliCertKeyStoreUrl == null)
            cliCertKeyStoreUrl = System.getProperty("javax.net.ssl.keyStore");

        if (cliCertKeyStorePwd == null)
            cliCertKeyStorePwd = System.getProperty("javax.net.ssl.keyStorePassword");

        if (cliCertKeyStoreType == null)
            cliCertKeyStoreType = System.getProperty("javax.net.ssl.keyStoreType", "JKS");

        if (trustCertKeyStoreUrl == null)
            trustCertKeyStoreUrl = System.getProperty("javax.net.ssl.trustStore");

        if (trustCertKeyStorePwd == null)
            cliCertKeyStorePwd = System.getProperty("javax.net.ssl.trustStorePassword");

        if (trustCertKeyStoreType == null)
            trustCertKeyStoreType = System.getProperty("javax.net.ssl.trustStoreType", "JKS");

        if (sslProtocol == null)
            sslProtocol = "TLS";

        if (!F.isEmpty(cliCertKeyStoreUrl))
            cliCertKeyStoreUrl = checkAndConvertUrl(cliCertKeyStoreUrl);

        if (!F.isEmpty(trustCertKeyStoreUrl))
            trustCertKeyStoreUrl = checkAndConvertUrl(trustCertKeyStoreUrl);

        TrustManagerFactory tmf;
        KeyManagerFactory kmf;

        KeyManager[] kms = null;

        try {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        }
        catch (NoSuchAlgorithmException e) {
            throw new SQLException("Default algorithm definitions for TrustManager and/or KeyManager are invalid." +
                " Check java security properties file.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }

        InputStream ksInputStream = null;

        try {
            if (!F.isEmpty(cliCertKeyStoreUrl) && !F.isEmpty(cliCertKeyStoreType)) {
                KeyStore clientKeyStore = KeyStore.getInstance(cliCertKeyStoreType);

                URL ksURL = new URL(cliCertKeyStoreUrl);

                char[] password = (cliCertKeyStorePwd == null) ? new char[0] : cliCertKeyStorePwd.toCharArray();

                ksInputStream = ksURL.openStream();

                clientKeyStore.load(ksInputStream, password);

                kmf.init(clientKeyStore, password);

                kms = kmf.getKeyManagers();
            }
        }
        catch (UnrecoverableKeyException e) {
            throw new SQLException("Could not recover keys from client keystore.",
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (NoSuchAlgorithmException e) {
            throw new SQLException("Unsupported keystore algorithm.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (KeyStoreException e) {
            throw new SQLException("Could not create client KeyStore instance.",
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (CertificateException e) {
            throw new SQLException("Could not load client key store. [storeType=" + cliCertKeyStoreType
                + ", cliStoreUrl=" + cliCertKeyStoreUrl + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (MalformedURLException e) {
            throw new SQLException("Invalid client key store URL. [url=" + cliCertKeyStoreUrl + ']',
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (IOException e) {
            throw new SQLException("Could not open client key store.[url=" + cliCertKeyStoreUrl + ']',
                SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        finally {
            if (ksInputStream != null) {
                try {
                    ksInputStream.close();
                }
                catch (IOException e) {
                    // can't close input stream, but keystore can be properly initialized
                    // so we shouldn't throw this exception
                }
            }
        }

        InputStream tsInputStream = null;

        List<TrustManager> tms;

        if (connProps.isSslTrustAll())
            tms = Collections.<TrustManager>singletonList(TRUST_ALL_MANAGER);
        else {
            tms = new ArrayList<>();

            try {
                KeyStore trustKeyStore = null;

                if (!F.isEmpty(trustCertKeyStoreUrl) && !F.isEmpty(trustCertKeyStoreType)) {
                    char[] trustStorePassword = (trustCertKeyStorePwd == null) ? new char[0]
                        : trustCertKeyStorePwd.toCharArray();

                    tsInputStream = new URL(trustCertKeyStoreUrl).openStream();

                    trustKeyStore = KeyStore.getInstance(trustCertKeyStoreType);

                    trustKeyStore.load(tsInputStream, trustStorePassword);
                }

                tmf.init(trustKeyStore);

                TrustManager[] origTms = tmf.getTrustManagers();

                Collections.addAll(tms, origTms);
            }
            catch (NoSuchAlgorithmException e) {
                throw new SQLException("Unsupported keystore algorithm.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (KeyStoreException e) {
                throw new SQLException("Could not create trust KeyStore instance.",
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (CertificateException e) {
                throw new SQLException("Could not load trusted key store. [storeType=" + trustCertKeyStoreType +
                    ", cliStoreUrl=" + trustCertKeyStoreUrl + ']', SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (MalformedURLException e) {
                throw new SQLException("Invalid trusted key store URL. [url=" + trustCertKeyStoreUrl + ']',
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            catch (IOException e) {
                throw new SQLException("Could not open trusted key store. [url=" + cliCertKeyStoreUrl + ']',
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
            finally {
                if (tsInputStream != null) {
                    try {
                        tsInputStream.close();
                    }
                    catch (IOException e) {
                        // can't close input stream, but keystore can be properly initialized
                        // so we shouldn't throw this exception
                    }
                }
            }
        }

        assert tms.size() != 0;

        try {
            SSLContext sslContext = SSLContext.getInstance(sslProtocol);

            sslContext.init(kms, tms.toArray(new TrustManager[tms.size()]), null);

            return sslContext.getSocketFactory();
        }
        catch (NoSuchAlgorithmException e) {
            throw new SQLException(connProps.sslProtocol() + " is not a valid SSL protocol.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
        catch (KeyManagementException e) {
            throw new SQLException("Cannot init SSL context.", SqlStateCode.CLIENT_CONNECTION_FAILED, e);
        }
    }

    /**
     * @param url URL or path to check and convert to URL.
     * @return URL.
     * @throws SQLException If URL is invalid.
     */
    private String checkAndConvertUrl(String url) throws SQLException {
        try {
            return new URL(url).toString();
        }
        catch (MalformedURLException e) {
            try {
                return FileSystems.getDefault().getPath(url).toUri().toURL().toString();
            }
            catch (MalformedURLException e1) {
                throw new SQLException("Invalid keystore UR: " + url,
                    SqlStateCode.CLIENT_CONNECTION_FAILED, e);
            }
        }
    }
}