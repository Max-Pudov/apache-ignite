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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.*;
import java.util.*;
import java.util.stream.*;

/**
 * TODO: add description.
 */
public class DenseLocalOffHeapVector extends AbstractVector {
    /** */
    private void makeOffheapStorage(int size){
        setStorage(new VectorOffheapStorage(size));
    }

    /**
     * @param args
     */
    public DenseLocalOffHeapVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size"))
            makeOffheapStorage((int) args.get("size"));
        else if (args.containsKey("arr") && args.containsKey("shallowCopy")) {
            double[] arr = (double[])args.get("arr");

            makeOffheapStorage(arr.length);

            assign(arr);
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     *
     * @param arr Array to copy to offheap storage.
     */
    public DenseLocalOffHeapVector(double[] arr){
        makeOffheapStorage(arr.length);

        assign(arr);
    }

    /**
     *
     * @param size
     */
    public DenseLocalOffHeapVector(int size){
        makeOffheapStorage(size);
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        DenseLocalOffHeapVector cp = new DenseLocalOffHeapVector(size());

        IntStream.range(0, size()).parallel().forEach(idx -> cp.set(idx, get(idx)));

        return cp;
    }

    /** {@inheritDoc */
    @Override public Vector times(double x) {
        if (x == 0.0)
            return like(size()).assign(0);
        else
            return super.times(x);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new DenseLocalOffHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return null; // TODO
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        getStorage().destroy();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o != null && getClass().equals(o.getClass()) && (getStorage().equals(((Vector)o).getStorage()));
    }
}
