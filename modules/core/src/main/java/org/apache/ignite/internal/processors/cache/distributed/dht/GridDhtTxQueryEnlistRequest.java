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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridDirectTransient;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class GridDhtTxQueryEnlistRequest extends GridCacheIdMessage {
    /** */
    private static final int BATCH_SIZE = 1024;

    /** */
    private IgniteUuid dhtFutId;

    /** */
    private int batchId;

    /** DHT tx version. */
    private GridCacheVersion lockVer;

    /** */
    private GridCacheOperation op;

    /** */
    private int mvccOpCnt;

    /** */
    @GridDirectTransient
    private List<T2<KeyCacheObject, CacheObject>> rows;

    /** */
    @GridToStringExclude
    private KeyCacheObject[] keys;

    /**  */
    @GridToStringExclude
    private CacheObject[] vals;

    /**
     *
     */
    public GridDhtTxQueryEnlistRequest() {
    }

    /**
     * @param cacheId Cache id.
     * @param dhtFutId DHT future id.
     * @param lockVer Lock version.
     * @param op Operation.
     * @param batchId Batch id.
     */
    GridDhtTxQueryEnlistRequest(int cacheId,
        IgniteUuid dhtFutId,
        GridCacheVersion lockVer,
        GridCacheOperation op,
        int batchId,
        int mvccOpCnt) {
        this.cacheId = cacheId;
        this.dhtFutId = dhtFutId;
        this.lockVer = lockVer;
        this.op = op;
        this.batchId = batchId;
        this.mvccOpCnt = mvccOpCnt;
    }

    /**
     * Adds row to batch.
     *
     * @param key Key.
     * @param val Value.
     */
    public void addRow(KeyCacheObject key, CacheObject val) {
        if (rows == null)
            rows = new ArrayList<>();

        rows.add(new T2<>(key, val));
    }

    /**
     * @return {@code True} if request ready to be sent (batch is full).
     */
    public boolean ready() {
        return rows != null && rows.size() == BATCH_SIZE;
    }

    /**
     * Returns request rows number.
     *
     * @return Request rows number.
     */
    public int batchSize() {
        return rows == null ? 0 : rows.size();
    }

    /**
     * @return Dht future id.
     */
    public IgniteUuid dhtFutureId() {
        return dhtFutId;
    }

    /**
     * @return Lock version.
     */
    public GridCacheVersion version() {
        return lockVer;
    }

    /**
     * @return Mvcc operation counter.
     */
    public int operationCounter() {
        return mvccOpCnt;
    }

    /**
     * @return Rows.
     */
    public List<T2<KeyCacheObject, CacheObject>> rows() {
        return rows;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation op() {
        return op;
    }

    /**
     * @return Keys.
     */
    public List<KeyCacheObject> keys() {
        return Arrays.asList(keys);
    }

    /**
     * @return Batch id.
     */
    public int batchId() {
        return batchId;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext cctx = ctx.cacheContext(cacheId);
        CacheObjectContext objCtx = cctx.cacheObjectContext();

        if (rows != null && keys == null) {
            boolean storeVals = op != GridCacheOperation.DELETE;

            int len = rows.size();

            keys = new KeyCacheObject[len];

            if (storeVals)
                vals = new CacheObject[len];

            for (int i = 0; i < len; i++) {
                T2<KeyCacheObject, CacheObject> row = rows.get(i);

                keys[i] = row.get1();
                keys[i].prepareMarshal(objCtx);

                if (storeVals) {
                    vals[i] = row.get2();
                    vals[i].prepareMarshal(objCtx);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (keys != null) {
            CacheObjectContext objCtx = ctx.cacheContext(cacheId).cacheObjectContext();

            int len = keys.length;

            rows = new ArrayList<>(len);

            for (int i = 0; i < len; i++) {
                KeyCacheObject key = keys[i];
                key.finishUnmarshal(objCtx, ldr);

                CacheObject val = null;

                if (op != GridCacheOperation.DELETE) {
                    val = vals[i];
                    val.finishUnmarshal(objCtx, ldr);
                }

                rows.add(new T2<>(key, val));
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 155;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeInt("batchId", batchId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeIgniteUuid("dhtFutId", dhtFutId))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeObjectArray("keys", keys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 6:
                if (!writer.writeMessage("lockVer", lockVer))
                    return false;

                writer.incrementState();

            case 7:
                if (!writer.writeInt("mvccOpCnt", mvccOpCnt))
                    return false;

                writer.incrementState();

            case 8:
                if (!writer.writeByte("op", op != null ? (byte)op.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeObjectArray("vals", vals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 3:
                batchId = reader.readInt("batchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                dhtFutId = reader.readIgniteUuid("dhtFutId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                keys = reader.readObjectArray("keys", MessageCollectionItemType.MSG, KeyCacheObject.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 6:
                lockVer = reader.readMessage("lockVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 7:
                mvccOpCnt = reader.readInt("mvccOpCnt");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 8:
                byte opOrd;

                opOrd = reader.readByte("op");

                if (!reader.isLastRead())
                    return false;

                op = GridCacheOperation.fromOrdinal(opOrd);

                reader.incrementState();

            case 9:
                vals = reader.readObjectArray("vals", MessageCollectionItemType.MSG, CacheObject.class);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtTxQueryEnlistRequest.class);
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 10;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxQueryEnlistRequest.class, this);
    }
}
