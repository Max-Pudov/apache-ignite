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
package org.apache.ignite.configuration;

import java.io.Serializable;
import org.apache.ignite.internal.pagemem.PageMemory;

/**
 * Configuration bean used for creating {@link PageMemory} instances.
 */
public final class MemoryPolicyConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default MemoryPolicyConfiguration flag.
     */
    private boolean dflt;

    /** */
    private String name;

    /**
     * Size in bytes of {@link PageMemory} to be created.
     */
    private long size;

    /** */
    private String tmpFsPath;

    /**
     *
     */
    public boolean isDefault() {
        return dflt;
    }

    /**
     * @param dflt Default flag.
     */
    public void setDefault(boolean dflt) {
        this.dflt = dflt;
    }

    /**
     *
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     */
    public long getSize() {
        return size;
    }

    /**
     * @param size Size of {@link PageMemory} in bytes.
     */
    public void setSize(long size) {
        this.size = size;
    }

    /**
     *
     */
    public String getTmpFsPath() {
        return tmpFsPath;
    }

    /**
     * @param tmpFsPath File path if memory-mapped file should be used.
     */
    public void setTmpFsPath(String tmpFsPath) {
        this.tmpFsPath = tmpFsPath;
    }
}
