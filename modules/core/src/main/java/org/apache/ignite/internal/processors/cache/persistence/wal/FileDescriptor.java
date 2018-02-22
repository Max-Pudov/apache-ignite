/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;

/**
 * WAL file descriptor.
 */
public class FileDescriptor implements Comparable<FileDescriptor>, AbstractWalRecordsIterator.AbstractFileDescriptor {
    /** */
    private static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** */
    protected final File file;

    /** Absolute WAL segment file index */
    protected final long idx;

    /**
     * Creates file descriptor. Index is restored from file name
     *
     * @param file WAL segment file.
     */
    public FileDescriptor(@NotNull File file) {
        this(file, null);
    }

    /**
     * @param file WAL segment file.
     * @param idx Absolute WAL segment file index. For null value index is restored from file name
     */
    public FileDescriptor(@NotNull File file, @Nullable Long idx) {
        this.file = file;

        String fileName = file.getName();

        assert fileName.contains(WAL_SEGMENT_FILE_EXT);

        this.idx = idx == null ? Long.parseLong(fileName.substring(0, 16)) : idx;
    }

    /**
     * @param segment Segment index.
     * @return Segment file name.
     */
    public static String fileName(long segment) {
        SB b = new SB();

        String segmentStr = Long.toString(segment);

        for (int i = segmentStr.length(); i < 16; i++)
            b.a('0');

        b.a(segmentStr).a(WAL_SEGMENT_FILE_EXT);

        return b.toString();
    }

    /**
     * @param segment Segment number as integer.
     * @return Segment number as aligned string.
     */
    private static String segmentNumber(long segment) {
        SB b = new SB();

        String segmentStr = Long.toString(segment);

        for (int i = segmentStr.length(); i < 16; i++)
            b.a('0');

        b.a(segmentStr);

        return b.toString();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(FileDescriptor o) {
        return Long.compare(idx, o.idx);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FileDescriptor))
            return false;

        FileDescriptor that = (FileDescriptor)o;

        return idx == that.idx;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(idx ^ (idx >>> 32));
    }

    /**
     * @return Absolute WAL segment file index
     */
    public long getIdx() {
        return idx;
    }

    /**
     * @return absolute pathname string of this file descriptor pathname.
     */
    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }

    /** {@inheritDoc} */
    @Override public boolean isCompressed() {
        return file.getName().endsWith(".zip");
    }

    /** {@inheritDoc} */
    @Override public File file() {
        return file;
    }

    /** {@inheritDoc} */
    @Override public long idx() {
        return idx;
    }
}
