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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
class UserManagementOperation implements Serializable {
    /** User. */
    private final User usr;

    /** Operation type. */
    private final OperationType type;

    /** Operation Id. */
    private final UUID id = UUID.randomUUID();

    /**
     * @param usr User.
     * @param type Action type.
     */
    public UserManagementOperation(User usr, OperationType type) {
        this.usr = usr;
        this.type = type;
    }

    /**
     * @return User/
     */
    public User user() {
        return usr;
    }

    /**
     * @return Operation type.
     */
    public OperationType type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        UserManagementOperation op = (UserManagementOperation)o;

        return F.eq(id, op.id);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UserManagementOperation.class, this);
    }

    /**
     * User action type.
     */
    public enum OperationType {ADD, UPDATE, REMOVE};
}
