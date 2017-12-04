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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;

/**
 * Simple index creation benchmark.
 */
public class CreateIndexBenchmark {
    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Index name. */
    private static final String INDEX_NAME = "IDX";

    /** Fields count. */
    private static final int FIELDS = 10;

    /** Iterations. */
    private static final int ITERATIONS = 10;

    /** Fields count. */
    private static final String TABLE_NAME = "VALUE";

    /** Field name prefix. */
    private static final String FIELD_PREF = "FIELD_";

    /** Ignite. */
    private static IgniteEx ignite;

    /** Keys. */
    private static long keys;

    /** Keys. */
    private static int parallel;

    /**
     * @param args Cmdline args.
     * @throws Exception On error.
     */
    public static void main(String [] args) throws Exception {
        String cfg = args[0];

        keys = Long.parseLong(args[1]);

        parallel = Integer.parseInt(args[2]);

        try (Ignite ig = Ignition.start(cfg)) {
            ignite = (IgniteEx)ig;

            ignite.active(true);

            ((GridCacheDatabaseSharedManager)(ignite.context().cache().context().database()))
                .enableCheckpoints(false).get();

            testBenchmark();
        }
    }

    /**
     * @throws Exception On error.
     */
    private static void testBenchmark() throws Exception {
        fillData();

        System.out.println("+++ Warmup");
        createIdx();

        double hackRes [] = new double[ITERATIONS];
        double withoutHackRes [] = new double[ITERATIONS];

        System.out.println("+++ Benchmark");
        for (int i = 0; i < ITERATIONS; ++i) {
            GridCacheMapEntry.OFFHEAP_HACK = false;

            withoutHackRes[i] = createIdx();

            GridCacheMapEntry.OFFHEAP_HACK = true;

            hackRes[i] = createIdx();
        }

        System.out.println("+++ HACK = false AVG: " + (float)avg(withoutHackRes));
        System.out.println("+++ HACK = true  AVG: " + (float)avg(hackRes));
    }

    /**
     * @param arr Array.
     * @return Average.
     */
    private static double avg(double [] arr) {
        double sum = 0;

        for (double d: arr)
            sum += d;

        return sum / arr.length;
    }

    /**
     * @throws IgniteCheckedException If failed.
     * @return Index creation time.
     */
    private static double createIdx() throws IgniteCheckedException {
        long t0 = System.currentTimeMillis();

        ignite.context().query().dynamicIndexCreate(CACHE_NAME, CACHE_NAME, TABLE_NAME,
            new QueryIndex(FIELD_PREF + 0).setName(INDEX_NAME), false, parallel).get();

        double time = (System.currentTimeMillis() - t0) / 1000.0;

        System.out.println("+++ Index build (HACK=" + GridCacheMapEntry.OFFHEAP_HACK + "): "
            + (float)time);

        ignite.context().query().dynamicIndexDrop(CACHE_NAME, CACHE_NAME, INDEX_NAME, false).get();

        System.gc();

        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.gc();

        return time;
    }

    /**
     * @throws Exception On error.
     */
    private static void fillData() throws Exception {
        int batchSize = 100_000;

        for (long i = 0; i < keys / batchSize; ++i) {
            System.out.println("Fill data: " + i * batchSize);

            Map<Long, BinaryObject> data = new HashMap<>(batchSize);

            for (int j = 0; j < batchSize; ++j) {
                BinaryObjectBuilder bob = ignite.binary().builder(TABLE_NAME);

                for (int fld = 0; fld < FIELDS; ++fld)
                    bob.setField(FIELD_PREF + fld, "value_" + fld + "_" + i);

                data.put(i * batchSize + j, bob.build());
            }

            ignite.cache(CACHE_NAME).putAll(data);
        }
    }
}
