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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.Externalizable;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 * Request for single partition info.
 */
public abstract class GridDhtPartitionsAbstractMessage extends GridCacheMessage {
    /** */
    public static final IgniteProductVersion PART_MAP_COMPRESS_SINCE = IgniteProductVersion.fromString("1.6.11");

    /** */
    protected static final byte COMPRESSED_FLAG_MASK = 1;

    /** */
    private static final long serialVersionUID = 0L;

    /** Exchange ID. */
    private GridDhtPartitionExchangeId exchId;

    /** Last used cache version. */
    private GridCacheVersion lastVer;

    /** */
    private byte flags;

    /**
     * Required by {@link Externalizable}.
     */
    protected GridDhtPartitionsAbstractMessage() {
        // No-op.
    }

    /**
     * @param exchId Exchange ID.
     * @param lastVer Last version.
     */
    GridDhtPartitionsAbstractMessage(GridDhtPartitionExchangeId exchId, @Nullable GridCacheVersion lastVer) {
        this.exchId = exchId;
        this.lastVer = lastVer;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return Integer.MIN_VALUE;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean partitionExchangeMessage() {
        return true;
    }

    /**
     * @return Exchange ID. {@code Null} if message doesn't belong to exchange process.
     */
    @Nullable public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @param cacheId Cache ID.
     * @return Parition update counters.
     */
    public abstract Map<Integer, T2<Long, Long>> partitionUpdateCounters(int cacheId);

    /**
     * @return Last used version among all nodes.
     */
    @Nullable public GridCacheVersion lastVersion() {
        return lastVer;
    }

    /**
     * @return {@code True} if message data is compressed.
     */
    protected final boolean compressed() {
        return (flags & COMPRESSED_FLAG_MASK) != 0;
    }

    /**
     * @param compressed {@code True} if message data is compressed.
     */
    protected final void compressed(boolean compressed) {
        flags = compressed ? (byte)(flags | COMPRESSED_FLAG_MASK) : (byte)(flags & ~COMPRESSED_FLAG_MASK);
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
                if (!writer.writeMessage("exchId", exchId))
                    return false;

                writer.incrementState();

            case 4:
                if (!writer.writeByte("flags", flags))
                    return false;

                writer.incrementState();

            case 5:
                if (!writer.writeMessage("lastVer", lastVer))
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
                exchId = reader.readMessage("exchId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 4:
                flags = reader.readByte("flags");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 5:
                lastVer = reader.readMessage("lastVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(GridDhtPartitionsAbstractMessage.class);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionsAbstractMessage.class, this, super.toString());
    }
}
