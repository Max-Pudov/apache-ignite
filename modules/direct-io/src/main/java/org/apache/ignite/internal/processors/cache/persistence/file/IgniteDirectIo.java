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

package org.apache.ignite.internal.processors.cache.persistence.file;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

public class IgniteDirectIo {

    static {
        Native.register(Platform.C_LIBRARY_NAME);
    }

    public static native int open(String pathname, int flags, int mode);


    /**
     * See "man 2 close"
     *
     * @param fd The file descriptor of the file to close
     *
     * @return 0 on success, -1 on error
     */
    public static native int close(int fd); // musn't forget to do this

    /**
     * @param fd
     * @param buf
     * @param count
     * @param offset
     * @return
     */
    public static native NativeLong pwrite(int fd, Pointer buf, NativeLong count, NativeLong offset);


    public static native NativeLong pread(int fd, Pointer buf, NativeLong count, NativeLong offset);

    /*
    public int pwrite(int fd, AlignedDirectByteBuffer buf, long offset) throws IOException {

        // must always write to end of current block
        // To handle writes past the logical file size,
        // we will later truncate.
        final int start = buf.position();
        assert start == blockStart(start);
        final int toWrite = blockEnd(buf.limit()) - start;

        int n = pwrite(fd, buf.pointer().share(start), new NativeLong(toWrite), new NativeLong(offset)).intValue();
        if (n < 0) {
            throw new IOException("error writing file at offset " + offset + ": " + DirectIoLib.getLastError());
        }
        return n;
    }*/
}
