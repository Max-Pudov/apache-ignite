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

package org.apache.ignite.client;

/**
 * Common thin client unchecked exception.
 */
public class IgniteClientError extends RuntimeException {
    /** Serial version uid. */
    private static final long serialVersionUID = -1930515656936185076L;

    /** Event. */
    private final SystemEvent evt;

    /**
     * Constructs a new exception with {@code null} as its detail message.
     */
    IgniteClientError(SystemEvent evt) {
        this.evt = evt;
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param msg the detail message.
     */
    IgniteClientError(SystemEvent evt, String msg) {
        super(msg);

        this.evt = evt;
    }

    /**
     * Constructs a new exception with the specified cause and a detail
     * message of <tt>(cause==null ? null : cause.toString())</tt>.
     *
     * @param cause the cause.
     */
    IgniteClientError(SystemEvent evt, Throwable cause) {
        super(cause);

        this.evt = evt;
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param msg the detail message.
     * @param cause the cause.
     */
    IgniteClientError(SystemEvent evt, String msg, Throwable cause) {
        super(msg, cause);

        this.evt = evt;
    }

    /**
     * Constructs a new exception with the specified detail message,
     * cause, suppression enabled or disabled, and writable stack
     * trace enabled or disabled.
     *
     * @param msg the detail message.
     * @param cause the cause.
     * @param enableSuppression whether or not suppression is enabled
     *                          or disabled
     * @param writableStackTrace whether or not the stack trace should
     *                           be writable
     */
    IgniteClientError(
        SystemEvent evt,
        String msg,
        Throwable cause,
        boolean enableSuppression,
        boolean writableStackTrace
    ) {
        super(msg, cause, enableSuppression, writableStackTrace);

        this.evt = evt;
    }

    /**
     * @return Event that caused this exception.
     */
    public SystemEvent getEvent() {
        return evt;
    }
}
