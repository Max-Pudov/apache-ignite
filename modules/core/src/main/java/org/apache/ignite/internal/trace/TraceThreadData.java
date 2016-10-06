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

package org.apache.ignite.internal.trace;

import java.util.ArrayList;

/**
 * Thread-local tracing data.
 */
public class TraceThreadData {
    /** Mutex. */
    private final Object mux = new Object();

    /** Thread group. */
    private final TraceThreadGroup grp;

    /** Thread. */
    private final Thread thread;

    /** Int value 0. */
    private int int0;

    /** Int value 1. */
    private int int1;

    /** Int value 2. */
    private int int2;

    /** Integers. */
    private int[] ints;

    /** Long value 0. */
    private long long0;

    /** Long value 1. */
    private long long1;

    /** Long value 2. */
    private long long2;

    /** Longs. */
    private long[] longs;

    /** Object value 0. */
    private Object obj0;

    /** Object value 1. */
    private Object obj1;

    /** Object value 2. */
    private Object obj2;

    /** Objects. */
    private Object[] objs;

    /** Data to be collected. */
    private ArrayList<Object> data;

    /**
     * Constructor.
     *
     * @param grp Thread group.
     * @param thread Thread.
     */
    public TraceThreadData(TraceThreadGroup grp, Thread thread) {
        this.thread = thread;
        this.grp = grp;
    }

    /**
     * @return Group.
     */
    public TraceThreadGroup group() {
        return grp;
    }

    /**
     * @return Thread.
     */
    public Thread thread() {
        return thread;
    }

    /**
     * Get int value.
     *
     * @param idx Index.
     * @return Value.
     */
    public int intValue(int idx) {
        switch (idx) {
            case 0:
                return int0;

            case 1:
                return int1;

            case 2:
                return int2;

            default:
                return ints == null || idx >= ints.length ? 0 : ints[idx];
        }
    }

    /**
     * Set int value.
     *
     * @param idx Index.
     * @param val Value.
     */
    public void intValue(int idx, int val) {
        switch (idx) {
            case 0:
                int0 = val;

                break;

            case 1:
                int1 = val;

                break;

            case 2:
                int2 = val;

                break;

            default: {
                if (ints == null || idx >= ints.length) {
                    int[] ints0 = new int[idx + 1];

                    if (ints != null)
                        System.arraycopy(ints, 0, ints0, 0, ints.length);

                    ints = ints0;
                }

                ints[idx] = val;
            }
        }
    }

    /**
     * Get long value.
     *
     * @param idx Index.
     * @return Value.
     */
    public long longValue(int idx) {
        switch (idx) {
            case 0:
                return long0;

            case 1:
                return long1;

            case 2:
                return long2;

            default:
                return longs == null || idx >= longs.length ? 0 : longs[idx];
        }
    }

    /**
     * Set long value.
     *
     * @param idx Index.
     * @param val Value.
     */
    public void longValue(int idx, long val) {
        switch (idx) {
            case 0:
                long0 = val;

                break;

            case 1:
                long1 = val;

                break;

            case 2:
                long2 = val;

                break;

            default: {
                if (longs == null || idx >= longs.length) {
                    long[] longs0 = new long[idx + 1];

                    if (longs != null)
                        System.arraycopy(longs, 0, longs0, 0, longs.length);

                    longs = longs0;
                }

                longs[idx] = val;
            }
        }
    }

    /**
     * Get object value.
     *
     * @param idx Index.
     * @return Value.
     */
    public Object objectValue(int idx) {
        switch (idx) {
            case 0:
                return obj0;

            case 1:
                return obj1;

            case 2:
                return obj2;

            default:
                return objs == null || idx >= objs.length ? 0 : objs[idx];
        }
    }

    /**
     * Set object value.
     *
     * @param idx Index.
     * @param val Value.
     */
    public void objectValue(int idx, Object val) {
        switch (idx) {
            case 0:
                obj0 = val;

                break;

            case 1:
                obj1 = val;

                break;

            case 2:
                obj2 = val;

                break;

            default: {
                if (objs == null || idx >= objs.length) {
                    Object[] objs0 = new Object[idx + 1];

                    if (objs != null)
                        System.arraycopy(objs, 0, objs0, 0, objs.length);

                    objs = objs0;
                }

                objs[idx] = val;
            }
        }
    }

    /**
     * Clear current state.
     */
    void clearState() {
        int0 = int1 = int2 = 0;
        long0 = long1 = long2 = 0;
        obj0 = obj1 = obj2 = null;

        ints = null;
        longs = null;
        objs = null;
    }

    /**
     * Push data entry.
     *
     * @param entry Entry.
     */
    public void pushData(Object entry) {
        synchronized (mux) {

            if (data == null)
                data = new ArrayList<>();

            data.add(entry);
        }
    }

    /**
     * @return Data.
     */
    ArrayList<Object> data() {
        synchronized (mux) {
            return new ArrayList<>(data);
        }
    }

    /**
     * Reset data.
     */
    void reset() {
        synchronized (mux) {
            data = null;
        }
    }
}
