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

package org.apache.ignite.mxbean;

import java.util.List;

/**
 * MXBean interface that provides access to ODBC\JDBC\Thin client connections.
 */
@MXBeanDescription("MBean that provides access to client connections.")
public interface ClientProcessorMXBean {
    /**
     * Returns list of active sessions.
     *
     * @return Sessions.
     */
    @MXBeanDescription("List of active sessions.")
    List<String> getActiveSessions();

    /**
     * Drop all active sessions.
     */
    @MXBeanDescription("List of active sessions.")
    void dropAllSessions();
}
