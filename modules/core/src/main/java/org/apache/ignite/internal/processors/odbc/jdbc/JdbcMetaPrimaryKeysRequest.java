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

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * JDBC get primary keys metadata request.
 */
public class JdbcMetaPrimaryKeysRequest extends JdbcRequest {
    /** Cache name. */
    private String schema;

    /** Table name. */
    private String tbl;

    /**
     * Default constructor is used for deserialization.
     */
    JdbcMetaPrimaryKeysRequest() {
        super(META_PRIMARY_KEYS);
    }

    /**
     * @param schema Cache name.
     * @param tblName Table name.
     */
    public JdbcMetaPrimaryKeysRequest(String schema, String tblName) {
        super(META_PRIMARY_KEYS);

        this.schema = schema;
        this.tbl = tblName;
    }

    /**
     * @return Schema name.
     */
    @Nullable public String schema() {
        return schema;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriterExImpl writer) throws BinaryObjectException {
        super.writeBinary(writer);

        writer.writeString(schema);
        writer.writeString(tbl);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReaderExImpl reader) throws BinaryObjectException {
        super.readBinary(reader);

        schema = reader.readString();
        tbl = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcMetaPrimaryKeysRequest.class, this);
    }
}