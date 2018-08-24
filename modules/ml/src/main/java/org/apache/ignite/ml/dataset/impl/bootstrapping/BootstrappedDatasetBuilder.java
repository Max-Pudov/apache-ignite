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

package org.apache.ignite.ml.dataset.impl.bootstrapping;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Builder for bootstrapped dataset. Bootstrapped dataset consist of several subsamples created in according to random
 * sampling with replacements selection of vectors from original dataset. This realization uses
 * {@link BootstrappedVector} containing each vector from original sample with counters of repetitions
 * for each subsample. As heuristic this implementation uses Poisson Distribution for generating counter values.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class BootstrappedDatasetBuilder<K,V> implements PartitionDataBuilder<K,V, EmptyContext, BootstrappedDatasetPartition> {
    /** Feature extractor. */
    private final IgniteBiFunction<K, V, Vector> featureExtractor;

    /** Label extractor. */
    private final IgniteBiFunction<K, V, Double> lbExtractor;

    /** Samples count. */
    private final int samplesCount;

    /** Subsample size. */
    private final double subsampleSize;

    /** Seed. */
    private long seed;

    /**
     * Creates an instance of BootstrappedDatasetBuilder.
     *
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param samplesCount Samples count.
     * @param subsampleSize Subsample size.
     */
    public BootstrappedDatasetBuilder(IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor,
        int samplesCount, double subsampleSize) {

        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.samplesCount = samplesCount;
        this.subsampleSize = subsampleSize;
    }

    public BootstrappedDatasetBuilder<K,V> withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    /** {@inheritDoc} */
    @Override public BootstrappedDatasetPartition build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize,
        EmptyContext ctx) {

        BootstrappedVector[] dataset = (BootstrappedVector[]) Array.newInstance(BootstrappedVector.class, Math.toIntExact(upstreamDataSize));
        AtomicInteger ptr = new AtomicInteger();
        PoissonDistribution poissonDistribution = new PoissonDistribution(subsampleSize);
        poissonDistribution.reseedRandomGenerator(seed);
        upstreamData.forEachRemaining(entry -> {
            Vector features = featureExtractor.apply(entry.getKey(), entry.getValue());
            Double label = lbExtractor.apply(entry.getKey(), entry.getValue());
            int[] repetitionCounters = new int[samplesCount];
            Arrays.setAll(repetitionCounters, i -> poissonDistribution.sample());
            dataset[ptr.getAndIncrement()] = new BootstrappedVector(features, label, repetitionCounters);
        });

        return new BootstrappedDatasetPartition(dataset);
    }
}
