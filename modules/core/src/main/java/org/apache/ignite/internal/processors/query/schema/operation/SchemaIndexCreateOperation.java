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

package org.apache.ignite.internal.processors.query.schema.operation;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.UUID;

/**
 * Schema index create operation.
 */
public class SchemaIndexCreateOperation extends SchemaIndexAbstractOperation {
    /** */
    private static final long serialVersionUID = 0L;

    /** Table name. */
    private final String tblName;

    /** Index. */
    @GridToStringInclude
    private final QueryIndex idx;

    /** Ignore operation if index exists. */
    private final boolean ifNotExists;
    private boolean noLogging;

    /**
     * Constructs SchemaIndexCreateOperation object.
     *
     * @param opId Operation id.
     * @param cacheName Cache name.
     * @param schemaName Schame name.
     * @param tblName Table name.
     * @param idx Index params.
     * @param ifNotExists Ignore operation if index exists.
     * @param noLogging Disable WAL during index creation.
     */
    public SchemaIndexCreateOperation(UUID opId, String cacheName, String schemaName, String tblName, QueryIndex idx,
        boolean ifNotExists, boolean noLogging) {
        super(opId, cacheName, schemaName);

        this.tblName = tblName;
        this.idx = idx;
        this.ifNotExists = ifNotExists;
        this.noLogging = noLogging;
    }

    /** {@inheritDoc} */
    @Override public String indexName() {
        return QueryUtils.indexName(tblName, idx);
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Index params.
     */
    public QueryIndex index() {
        return idx;
    }

    /**
     * @return Ignore operation if index exists.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Indicates if WAL should be not written to during index creation.
     *
     * @return true if the WAL should be disabled, false otherwise.
     */
    public boolean noLogging() {
        return noLogging;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaIndexCreateOperation.class, this, "parent", super.toString());
    }
}
