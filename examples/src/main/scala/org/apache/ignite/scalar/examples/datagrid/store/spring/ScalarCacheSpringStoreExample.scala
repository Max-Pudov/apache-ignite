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

package org.apache.ignite.scalar.examples.datagrid.store.spring

import java.lang.{Long => JavaLong}
import java.util.UUID
import javax.cache.configuration.{Factory, FactoryBuilder}

import org.apache.ignite.cache.CacheAtomicityMode._
import org.apache.ignite.cache.store.CacheStoreSessionListener
import org.apache.ignite.cache.store.spring.CacheSpringStoreSessionListener
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.examples.{ExampleNodeStartup, ExamplesUtils}
import org.apache.ignite.examples.util.DbH2ServerStartup
import org.apache.ignite.scalar.examples.model.Person
import org.apache.ignite.scalar.scalar
import org.apache.ignite.scalar.scalar._
import org.apache.ignite.{IgniteCache, Ignition}

/**
  * Demonstrates usage of cache with underlying persistent store configured.
  * <p>
  * This example uses [[ScalarCacheSpringStoreExample]] as a persistent store.
  * <p>
  * To start the example, you should:
  * <ul>
  * <li>Start H2 database TCP server using [[DbH2ServerStartup]].</li>
  * <li>Start a few nodes using [[ExampleNodeStartup]].</li>
  * <li>Start example using [[ScalarCacheSpringStoreExample]].</li>
  * </ul>
  * <p>
  * Remote nodes can be started with [[ExampleNodeStartup]] in another JVM which will
  * start node with `examples/config/example-ignite.xml` configuration.
  */
object ScalarCacheSpringStoreExample extends App {
    /** Configuration file name. */
    private val CONFIG = "examples/config/example-ignite.xml"

    /** Cache name. */
    private val CACHE_NAME = ScalarCacheSpringStoreExample.getClass.getSimpleName
    
    /** Heap size required to run this example. */
    val MIN_MEMORY = 1024 * 1024 * 1024
    
    /** Number of entries to load. */
    private val ENTRY_COUNT = 100000

    /** Global person ID to use across entire example. */
    private val id = Math.abs(UUID.randomUUID.getLeastSignificantBits)

    ExamplesUtils.checkMinMemory(MIN_MEMORY)

    scalar(CONFIG) {
        println()
        println(">>> Cache store example started.")

        val cacheCfg = new CacheConfiguration[JavaLong, Person](CACHE_NAME)

        // Set atomicity as transaction, since we are showing transactions in example.
        cacheCfg.setAtomicityMode(TRANSACTIONAL)

        // Configure Dummy store.
        cacheCfg.setCacheStoreFactory(FactoryBuilder.factoryOf(classOf[ScalarCacheSpringPersonStore]))

        // Configure Spring session listener.
        cacheCfg.setCacheStoreSessionListenerFactories(new Factory[CacheStoreSessionListener]() {
            override def create(): CacheStoreSessionListener = {
                val listener = new CacheSpringStoreSessionListener

                listener.setDataSource(ScalarCacheSpringPersonStore.DATA_SRC)

                listener
            }
        })

        cacheCfg.setReadThrough(true)
        cacheCfg.setWriteThrough(true)

        val cache = createCache$[JavaLong, Person](cacheCfg)

        try {
            loadCache(cache)

            executeTransaction(cache)
        }
        finally {
            if (cache != null)
                cache.close()
        }
    }

    /**
     * Makes initial cache loading.
     *
     * @param cache Cache to load.
     */
    private def loadCache(cache: IgniteCache[JavaLong, Person]) {
        val start = System.currentTimeMillis

        cache.loadCache(null, Integer.valueOf(ENTRY_COUNT))

        val end = System.currentTimeMillis

        println(">>> Loaded " + cache.size() + " keys with backups in " + (end - start) + "ms.")
    }

    /**
      * Executes transaction with read/write-through to persistent store.
      *
      * @param cache Cache to execute transaction on.
      */
    private def executeTransaction(cache: IgniteCache[JavaLong, Person]) {
        val tx = Ignition.ignite.transactions.txStart

        try {
            var value = cache.get(id)

            System.out.println("Read value: " + value)

            value = cache.getAndPut(id, new Person(id, "Isaac", "Newton"))

            System.out.println("Overwrote old value: " + value)

            value = cache.get(id)

            System.out.println("Read value: " + value)

            tx.commit()
        } finally {
            tx.close()
        }

        System.out.println("Read value after commit: " + cache.get(id))
    }
}
