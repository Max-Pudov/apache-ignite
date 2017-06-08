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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Serializable;

/**
 * Key used to store in utility cache information about created data structures.
 */
public class DataStructureDefinitionKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data structure name. */
    private String name;

    /**
     * @param name Data structure name.
     */
    public DataStructureDefinitionKey(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DataStructureDefinitionKey key2 = (DataStructureDefinitionKey)o;

        return name != null ? name.equals(key2.name) : key2.name == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
