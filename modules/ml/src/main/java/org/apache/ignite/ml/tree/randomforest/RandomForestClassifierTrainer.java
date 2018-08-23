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

package org.apache.ignite.ml.tree.randomforest;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.FeatureHistogram;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bagging.BaggedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bagging.BaggedVector;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.randomforest.data.impurity.GiniHistogram;

public class RandomForestClassifierTrainer extends RandomForestTrainer<FeatureHistogram<BaggedVector>, GiniHistogram, RandomForestClassifierTrainer> {
    private Map<Double, Integer> lblMapping = new HashMap<>();

    public RandomForestClassifierTrainer(List<FeatureMeta> meta) {
        super(meta);
    }

    @Override protected RandomForestClassifierTrainer instance() {
        return this;
    }

    @Override protected void init(Dataset<EmptyContext, BaggedDatasetPartition> dataset) {
        Set<Double> uniqLabels = dataset.compute(
            x -> {
                Set<Double> labels = new HashSet<>();
                for (int i = 0; i < x.getRowsCount(); i++)
                    labels.add(x.getRow(i).getLabel());
                return labels;
            },
            (l, r) -> {
                if (l == null)
                    return r;
                if (r == null)
                    return l;
                Set<Double> lbls = new HashSet<>();
                lbls.addAll(l);
                lbls.addAll(r);
                return lbls;
            }
        );

        int i = 0;
        for (Double label : uniqLabels)
            lblMapping.put(label, i++);

        super.init(dataset);
    }

    @Override protected ModelsComposition buildComposition(List<RandomForestTrainer.TreeRoot> models) {
        return new ModelsComposition(models, new OnMajorityPredictionsAggregator());
    }

    @Override protected GiniHistogram createImpurityComputer(int sampleId, BucketMeta meta) {
        return new GiniHistogram(sampleId, lblMapping, meta);
    }

    @Override protected void addElementToLeafStat(FeatureHistogram<BaggedVector> leafStatAggr, BaggedVector vec, int sampleId) {
        leafStatAggr.addElement(vec);
    }

    @Override protected FeatureHistogram<BaggedVector> mergeLeafStats(FeatureHistogram<BaggedVector> leafStatAggr1,
        FeatureHistogram<BaggedVector> leafStatAggr2) {

        leafStatAggr1.addHist(leafStatAggr2);
        return leafStatAggr1;
    }

    @Override protected FeatureHistogram<BaggedVector> createLeafStatsAggregator(int sampleId) {
        return new FeatureHistogram<>(
            x -> lblMapping.get(x.getLabel()),
            x -> (double)x.getRepetitionsCounters()[sampleId]
        );
    }

    @Override protected double computeLeafValue(FeatureHistogram<BaggedVector> stat) {
        Integer bucketId = stat.buckets().stream()
            .max(Comparator.comparing(b -> stat.get(b).orElse(0.0)))
            .get();

        return lblMapping.entrySet().stream()
            .filter(x -> x.getValue().equals(bucketId))
            .findFirst()
            .get().getKey();
    }
}
