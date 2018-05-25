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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccIteratorWithConcurrentJdbcTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccLocalEntriesWithConcurrentJdbcTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedBackupsTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSelectForUpdateQueryTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSqlQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSqlTxQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSqlTxQueriesWithReducerTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedBackupsTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSelectForUpdateQueryTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSqlQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSqlTxQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSqlTxQueriesWithReducerTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccScanQueryWithConcurrentJdbcTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSizeWithConcurrentJdbcTransactionTest;

/**
 *
 */
public class IgniteCacheMvccSqlTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("IgniteCache SQL MVCC Test Suite");

        suite.addTestSuite(CacheMvccSizeWithConcurrentJdbcTransactionTest.class);
        suite.addTestSuite(CacheMvccScanQueryWithConcurrentJdbcTransactionTest.class);
        suite.addTestSuite(CacheMvccLocalEntriesWithConcurrentJdbcTransactionTest.class);
        suite.addTestSuite(CacheMvccIteratorWithConcurrentJdbcTransactionTest.class);
        suite.addTestSuite(CacheMvccPartitionedSqlQueriesTest.class);
        suite.addTestSuite(CacheMvccReplicatedSqlQueriesTest.class);
        suite.addTestSuite(CacheMvccPartitionedSqlTxQueriesTest.class);
        suite.addTestSuite(CacheMvccReplicatedSqlTxQueriesTest.class);
        suite.addTestSuite(CacheMvccPartitionedSqlTxQueriesWithReducerTest.class);
        suite.addTestSuite(CacheMvccReplicatedSqlTxQueriesWithReducerTest.class);
        suite.addTestSuite(CacheMvccPartitionedSelectForUpdateQueryTest.class);
        suite.addTestSuite(CacheMvccReplicatedSelectForUpdateQueryTest.class);
        suite.addTestSuite(CacheMvccPartitionedBackupsTest.class);
        suite.addTestSuite(CacheMvccReplicatedBackupsTest.class);

        return suite;
    }
}