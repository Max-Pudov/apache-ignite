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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.Vector;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
class VectorImplementationsFixtures {
    /** */
    private static final List<Supplier<Iterable<Vector>>> suppliers = Arrays.asList(
        new Supplier<Iterable<Vector>>() {
            /** {@inheritDoc} */
            @Override public Iterable<Vector> get() {
                return new DenseLocalOnHeapVectorFixture();
            }
        },
        new Supplier<Iterable<Vector>>() {
            /** {@inheritDoc} */
            @Override public Iterable<Vector> get() {
                return new DenseLocalOffHeapVectorFixture();
            }
        },
        new Supplier<Iterable<Vector>>() {
            /** {@inheritDoc} */
            @Override public Iterable<Vector> get() {
                return new SparseLocalOnHeapVectorFixture();
            }
        } // todo add Supplier for RandomVectorFixture after testing and bugfixes are completed
    );

    /** */
    void consumeSampleVectors(Consumer<Integer> paramsConsumer, BiConsumer<Vector, String> consumer) {
        for (Supplier<Iterable<Vector>> fixtureSupplier : VectorImplementationsFixtures.suppliers) {
            final Iterable<Vector> fixture = fixtureSupplier.get();

            for (Vector v : fixture) {
                if (paramsConsumer != null)
                    paramsConsumer.accept(v.size());

                consumer.accept(v, fixture.toString());
            }
        }
    }

    /** */
    void selfTest() {
        new VectorSizesCpIterator("VectorSizesCpIterator test", DenseLocalOnHeapVector::new, null).selfTest();

        new VectorSizesIterator("VectorSizesIterator test", DenseLocalOffHeapVector::new, null).selfTest();
    }

    /** */
    private static class DenseLocalOnHeapVectorFixture implements Iterable<Vector> {
        /** */ private final Supplier<VectorSizesCpIterator> iter;

        /** */ private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        DenseLocalOnHeapVectorFixture() {
            iter = () -> new VectorSizesCpIterator("DenseLocalOnHeapVector", DenseLocalOnHeapVector::new, ctxDescrHolder::set);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Vector> iterator() {
            return iter.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return ctxDescrHolder.get();
        }
    }

    /** */
    private static class DenseLocalOffHeapVectorFixture extends VectorSizesFixture {
        /** */
        DenseLocalOffHeapVectorFixture() {
            super("DenseLocalOffHeapVector", DenseLocalOffHeapVector::new);
        }
    }

    /** */
    private static class SparseLocalOnHeapVectorFixture implements Iterable<Vector> {
        /** */ private final Supplier<VectorSizesModeIterator> iter;

        /** */ private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        SparseLocalOnHeapVectorFixture() {
            iter = () -> new VectorSizesModeIterator("SparseLocalOnHeapVector", SparseLocalOnHeapVector::new, ctxDescrHolder::set);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Vector> iterator() {
            return iter.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return ctxDescrHolder.get();
        }
    }

    /** */
    private static class RandomVectorFixture extends VectorSizesFixture {
        /** */
        RandomVectorFixture() {
            super("RandomVector", RandomVector::new);
        }
    }

    /** */
    private static abstract class VectorSizesFixture implements Iterable<Vector> {
        /** */ private final Supplier<VectorSizesIterator> iter;

        /** */ private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        VectorSizesFixture(String vectorKind, Function<Integer, Vector> ctor) {
            iter = () -> new VectorSizesIterator(vectorKind, ctor, ctxDescrHolder::set);
        }

        /** {@inheritDoc} */
        @Override public Iterator<Vector> iterator() {
            return iter.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return ctxDescrHolder.get();
        }
    }

    /** */
    private static class VectorSizesModeIterator extends VectorSizesIterator {
        /** */ private static final Integer modes[] = new Integer[] {0, 1, null};

        /** */ private int modeIdx = 0;

        /** */ private final BiFunction<Integer, Integer, Vector> ctor;

        /** */
        VectorSizesModeIterator(String vectorKind, BiFunction<Integer, Integer, Vector> ctor,
            Consumer<String> ctxDescrConsumer) {
            super(vectorKind, null, ctxDescrConsumer);

            this.ctor = ctor;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return super.hasNext() && hasNextMode(modeIdx);
        }

        /** {@inheritDoc} */
        @Override void nextIdx() {
            assert modes[modeIdx] != null
                : "Index(es) out of bound at " + VectorSizesModeIterator.this;

            if (hasNextMode(modeIdx + 1)) {
                modeIdx++;

                return;
            }

            modeIdx = 0;

            super.nextIdx();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return "{" + super.toString() +
                ", mode=" + modes[modeIdx] +
                '}';
        }

        /** {@inheritDoc} */
        @Override Function<Integer, Vector> ctor() {
            return (size) -> ctor.apply(size, modes[modeIdx]);
        }

        /** */
        void selfTest() {
            final Set<Integer> modeIdxs = new HashSet<>();

            int cnt = 0;

            while (hasNext()) {
                assertNotNull("Expect not null vector at " + this, next());

                if (modes[modeIdx] != null)
                    modeIdxs.add(modeIdx);

                cnt++;
            }

            assertEquals("Mode tested", modeIdxs.size(), modes.length - 1);

            assertEquals("Combinations tested mismatch.",
                8 * 3 * (modes.length - 1), cnt);
        }

        /** */
        private boolean hasNextMode(int idx) {
            return modes[idx] != null;
        }
    }

    /** */
    private static class VectorSizesCpIterator extends VectorSizesIterator {
        /** */ private static final Boolean shallowCps[] = new Boolean[] {false, true, null};

        /** */ private int shallowCpIdx = 0;

        /** */ private final BiFunction<double[], Boolean, Vector> ctor;

        /** */
        VectorSizesCpIterator(String vectorKind, BiFunction<double[], Boolean, Vector> ctor,
            Consumer<String> ctxDescrConsumer) {
            super(vectorKind, null, ctxDescrConsumer);

            this.ctor = ctor;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return super.hasNext() && hasNextShallowCp(shallowCpIdx);
        }

        /** {@inheritDoc} */
        @Override void nextIdx() {
            assert shallowCps[shallowCpIdx] != null
                : "Index(es) out of bound at " + VectorSizesCpIterator.this;

            if (hasNextShallowCp(shallowCpIdx + 1)) {
                shallowCpIdx++;

                return;
            }

            shallowCpIdx = 0;

            super.nextIdx();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return "{" + super.toString() +
                ", shallowCopy=" + shallowCps[shallowCpIdx] +
                '}';
        }

        /** {@inheritDoc} */
        @Override Function<Integer, Vector> ctor() {
            return (size) -> ctor.apply(new double[size], shallowCps[shallowCpIdx]);
        }

        /** */
        void selfTest() {
            final Set<Integer> shallowCpIdxs = new HashSet<>();

            int cnt = 0;

            while (hasNext()) {
                assertNotNull("Expect not null vector at " + this, next());

                if (shallowCps[shallowCpIdx] != null)
                    shallowCpIdxs.add(shallowCpIdx);

                cnt++;
            }

            assertEquals("ShallowCp tested", shallowCpIdxs.size(), shallowCps.length - 1);

            assertEquals("Combinations tested mismatch.",
                8 * 3 * (shallowCps.length - 1), cnt);
        }

        /** */
        private boolean hasNextShallowCp(int idx) {
            return shallowCps[idx] != null;
        }
    }

    /** */
    private static class VectorSizesIterator implements Iterator<Vector> {
        /** */ private static final Integer sizes[] = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, null};

        /** */ private static final Integer deltas[] = new Integer[] {-1, 0, 1, null};

        /** */ private final String vectorKind;

        /** */ private final Function<Integer, Vector> ctor;

        /** */ private final Consumer<String> ctxDescrConsumer;

        /** */ private int sizeIdx = 0;

        /** */ private int deltaIdx = 0;

        /** */
        VectorSizesIterator(String vectorKind, Function<Integer, Vector> ctor, Consumer<String> ctxDescrConsumer) {
            this.vectorKind = vectorKind;

            this.ctor = ctor;

            this.ctxDescrConsumer = ctxDescrConsumer;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return hasNextSize(sizeIdx) && hasNextDelta(deltaIdx);
        }

        /** {@inheritDoc} */
        @Override public Vector next() {
            if (!hasNext())
                throw new NoSuchElementException(VectorSizesIterator.this.toString());

            if (ctxDescrConsumer != null)
                ctxDescrConsumer.accept(toString());

            Vector res = ctor().apply(sizes[sizeIdx] + deltas[deltaIdx]);

            nextIdx();

            return res;
        }

        /** IMPL NOTE override in subclasses if needed */
        void nextIdx() {
            assert sizes[sizeIdx] != null && deltas[deltaIdx] != null
                : "Index(es) out of bound at " + VectorSizesIterator.this;

            if (hasNextDelta(deltaIdx + 1)) {
                deltaIdx++;

                return;
            }

            deltaIdx = 0;

            sizeIdx++;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return vectorKind + "{" + "size=" + sizes[sizeIdx] +
                ", size delta=" + deltas[deltaIdx] +
                '}';
        }

        /** IMPL NOTE override in subclasses if needed */
        Function<Integer, Vector> ctor() { return ctor; }

        /** */
        void selfTest() {
            final Set<Integer> sizeIdxs = new HashSet<>(), deltaIdxs = new HashSet<>();

            int cnt = 0;

            while (hasNext()) {
                assertNotNull("Expect not null vector at " + this, next());

                if (sizes[sizeIdx] != null)
                    sizeIdxs.add(sizeIdx);

                if (deltas[deltaIdx] != null)
                    deltaIdxs.add(deltaIdx);

                cnt++;
            }

            assertEquals("Sizes tested mismatch.", sizeIdxs.size(), sizes.length - 1);

            assertEquals("Deltas tested", deltaIdxs.size(), deltas.length - 1);

            assertEquals("Combinations tested mismatch.",
                (sizes.length - 1) * (deltas.length - 1), cnt);
        }

        /** */
        private boolean hasNextSize(int idx) {
            return sizes[idx] != null;
        }

        /** */
        private boolean hasNextDelta(int idx) {
            return deltas[idx] != null;
        }
    }
}
