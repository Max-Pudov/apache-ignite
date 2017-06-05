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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequest;

/**
 * JDBC request.
 */
public class JdbcRequest extends SqlListenerRequest implements JdbcRawBinarylizable {
    /** Execute sql query request. */
    static final byte QRY_EXEC = 2;

    /** Fetch query results request. */
    static final byte QRY_FETCH = 3;

    /** Close query request. */
    static final byte QRY_CLOSE = 4;

    /** Get query result's columns metadata request. */
    static final byte QRY_META = 5;

    /** Gte tables metadata request. */
    static final byte META_TABLES = 6;

    /** Get columns metadata request. */
    static final byte META_COLUMNS = 7;

    /** Get indexes metadata request. */
    static final byte META_INDEXES = 8;

    /** Get SQL query parameters metadata request. */
    static final byte META_PARAMS = 9;

    /** Get primary keys metadata request. */
    static final byte META_PRIMARY_KEYS = 10;

    /** Request type. */
    private byte type;

    /**
     * @param type Command type.
     */
    public JdbcRequest(byte type) {
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        writer.writeByte(type);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        // No-op.
    }

    /**
     * @return Request type.
     */
    public byte type() {
        return type;
    }

    /**
     * @param reader Binary reader.
     * @return Request object.
     * @throws BinaryObjectException On error.
     */
    public static JdbcRequest readRequest(BinaryReaderExImpl reader) throws BinaryObjectException {
        int reqType = reader.readByte();

        JdbcRequest req;

        switch(reqType) {
            case QRY_EXEC:
                req = new JdbcQueryExecuteRequest();

                break;

            case QRY_FETCH:
                req = new JdbcQueryFetchRequest();

                break;

            case QRY_META:
                req = new JdbcQueryMetadataRequest();

                break;

            case QRY_CLOSE:
                req = new JdbcQueryCloseRequest();

                break;

            case META_TABLES:
                req = new JdbcMetaTablesRequest();

                break;

            case META_COLUMNS:
                req = new JdbcMetaColumnsRequest();

                break;

            case META_INDEXES:
                req = new JdbcMetaIndexesRequest();

                break;

            case META_PARAMS:
                req = new JdbcMetaParamsRequest();

                break;

            case META_PRIMARY_KEYS:
                req = new JdbcMetaPrimaryKeysRequest();

                break;

            default:
                throw new IgniteException("Unknown SQL listener request ID: [request ID=" + reqType + ']');
        }

        req.readBinary(reader);

        return req;
    }
}
