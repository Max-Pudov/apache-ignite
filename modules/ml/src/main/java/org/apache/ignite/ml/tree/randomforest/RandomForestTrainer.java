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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedVector;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies;
import org.apache.ignite.ml.tree.randomforest.data.NodeId;
import org.apache.ignite.ml.tree.randomforest.data.NodeSplit;
import org.apache.ignite.ml.tree.randomforest.data.TreeNode;
import org.apache.ignite.ml.tree.randomforest.data.impurity.ImpurityComputer;

public abstract class RandomForestTrainer<LeafStatsAggregator, S extends ImpurityComputer<BootstrappedVector, S>, T extends RandomForestTrainer<LeafStatsAggregator, S, T>>
    extends DatasetTrainer<ModelsComposition, Double> {

    public static final double BUCKET_SIZE_FACTOR = (1 / 10.0);
    private int countOfTrees;
    private double subsampleSize;
    private int maxDepth;
    private double minImpurityDelta;
    private List<FeatureMeta> meta;
    private int featuresPerTree;
    private Random random;
    private long seed;
    private IgniteFunction<Queue<TreeNode>, List<TreeNode>> nodesToLearnSelectionStrgy;

    public RandomForestTrainer(List<FeatureMeta> meta) {
        this.meta = meta;
        this.countOfTrees = 1;
        this.subsampleSize = 1.0;
        this.maxDepth = 5;
        this.minImpurityDelta = 0.0;
        this.featuresPerTree = FeaturesCountSelectionStrategies.ALL.apply(meta);
        this.random = new Random(System.currentTimeMillis());
        this.nodesToLearnSelectionStrgy = this::defaultNodesToLearnSelectionStrgy;
    }

    @Override public <K, V> ModelsComposition fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        List<TreeRoot> models = null;
        try (Dataset<EmptyContext, BootstrappedDatasetPartition> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new BootstrappedDatasetBuilder<>(featureExtractor, lbExtractor, countOfTrees, subsampleSize))) {

            init(dataset);
            models = fit(dataset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        assert models != null;
        return buildComposition(models);
    }

    protected abstract T instance();

    public T withCountOfTrees(int countOfTrees) {
        this.countOfTrees = countOfTrees;
        return instance();
    }

    public T withSubsampleSize(double subsampleSize) {
        this.subsampleSize = subsampleSize;
        return instance();
    }

    public T withMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return instance();
    }

    public T withMinImpurityDelta(double minImpurityDelta) {
        this.minImpurityDelta = minImpurityDelta;
        return instance();
    }

    public T withFeaturesCountSelectionStrgy(IgniteFunction<List<FeatureMeta>, Integer> strgy) {
        this.featuresPerTree = strgy.apply(meta);
        return instance();
    }

    public T withNodesToLearnSelectionStrgy(IgniteFunction<Queue<TreeNode>, List<TreeNode>> strgy) {
        this.nodesToLearnSelectionStrgy = strgy;
        return instance();
    }

    public T withSeed(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
        return instance();
    }

    protected void init(Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {
    }

    private List<TreeRoot> fit(Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {
        Queue<TreeNode> treesQueue = createRootsQueue();
        ArrayList<TreeRoot> roots = initTrees(treesQueue);
        Map<Integer, BucketMeta> histMeta = computeHistogramMeta(meta, dataset);

        while (!treesQueue.isEmpty()) {
            Map<NodeId, TreeNode> nodesToLearn = getNodesToLearn(treesQueue);
            Map<NodeId, NodeStatistics> nodesStatistics = dataset.compute(
                x -> aggregateStatistics(x, roots, histMeta, nodesToLearn),
                this::reduce
            );

            if (nodesToLearn.size() != nodesStatistics.size())
                throw new IllegalStateException();

            for (IgniteBiTuple<Integer, Long> nodeKey : nodesStatistics.keySet()) {
                NodeStatistics statistics = nodesStatistics.get(nodeKey);
                TreeNode cornerNode = nodesToLearn.get(statistics.nodeId);
                Optional<NodeSplit> bestSplit = statistics.findBestSplit();

                if (needSplit(cornerNode, bestSplit)) {
                    List<TreeNode> children = bestSplit.get().split(cornerNode);
                    treesQueue.addAll(children);
                }
                else {
                    if (bestSplit.isPresent())
                        bestSplit.get().createLeaf(cornerNode);
                    else {
                        cornerNode.setImpurity(Double.NEGATIVE_INFINITY);
                        cornerNode.toLeaf(0.0);
                    }
                }
            }
        }

        computeLeafValues(roots, dataset);
        return roots;
    }

    protected void computeLeafValues(ArrayList<TreeRoot> roots,
        Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {

        Map<NodeId, TreeNode> leafs = roots.stream().flatMap(r -> r.getLeafs().stream())
            .collect(Collectors.toMap(TreeNode::getId, n -> n));

        Map<NodeId, LeafStatsAggregator> stats = dataset.compute(
            data -> {
                Map<NodeId, LeafStatsAggregator> res = new HashMap<>();
                for (int sampleId = 0; sampleId < roots.size(); sampleId++) {
                    final int sampleIdConst = sampleId;

                    data.foreach(vec -> {
                        NodeId leafId = roots.get(sampleIdConst).getNode().predictNextNodeKey(vec.getFeatures());
                        if (!leafs.containsKey(leafId))
                            throw new IllegalStateException();

                        if (!res.containsKey(leafId))
                            res.put(leafId, createLeafStatsAggregator(sampleIdConst));

                        addElementToLeafStat(res.get(leafId), vec, sampleIdConst);
                    });
                }

                return res;
            },
            (left, right) -> {
                if (left == null)
                    return right;
                if (right == null)
                    return left;

                Set<NodeId> keys = new HashSet<>(left.keySet());
                keys.addAll(right.keySet());
                for (NodeId key : keys) {
                    if (!left.containsKey(key))
                        left.put(key, right.get(key));
                    else if (right.containsKey(key))
                        left.put(key, mergeLeafStats(left.get(key), right.get(key)));
                }

                return left;
            });

        leafs.forEach((id, leaf) -> {
            LeafStatsAggregator stat = stats.get(id);
            double leafVal = computeLeafValue(stat);
            leaf.setValue(leafVal);
        });
    }

    protected abstract void addElementToLeafStat(LeafStatsAggregator leafStatAggr, BootstrappedVector vec, int sampleId);

    protected abstract LeafStatsAggregator mergeLeafStats(
        LeafStatsAggregator leafStatAggr1, LeafStatsAggregator leafStatAggr2);

    protected abstract LeafStatsAggregator createLeafStatsAggregator(int sampleId);

    protected abstract double computeLeafValue(LeafStatsAggregator stat);


    protected ArrayList<TreeRoot> initTrees(Queue<TreeNode> treesQueue) {
        ArrayList<TreeRoot> roots = new ArrayList<>();

        for (TreeNode node : treesQueue) {
            Set<Integer> featuresSubspace = createFeaturesSubspace();
            roots.add(new TreeRoot(node, featuresSubspace));
        }

        return roots;
    }

    Set<Integer> createFeaturesSubspace() {
        if (featuresPerTree > meta.size())
            throw new RuntimeException("Count of features per tree must be less or equal feature space size");

        Set<Integer> subspace = new HashSet<>();
        int count = 0;
        while (count < featuresPerTree) {
            int featureId = random.nextInt(meta.size());
            if (!subspace.contains(featureId)) {
                subspace.add(featureId);
                count++;
            }
        }

        return subspace;
    }

    static class NormalDistributionStats {
        private final double min;
        private final double max;
        private final double sumOfSquares;
        private final double sumOfValues;
        private final long n;

        public NormalDistributionStats(double min, double max, double sumOfSquares, double sumOfValues, long n) {
            this.min = min;
            this.max = max;
            this.sumOfSquares = sumOfSquares;
            this.sumOfValues = sumOfValues;
            this.n = n;
        }

        public NormalDistributionStats plus(NormalDistributionStats stats) {
            return new NormalDistributionStats(
                Math.min(this.min, stats.min),
                Math.max(this.max, stats.max),
                this.sumOfSquares + stats.sumOfSquares,
                this.sumOfValues + stats.sumOfValues,
                this.n + stats.n
            );
        }

        public double mean() {
            return sumOfValues / n;
        }

        public double variance() {
            double mean = mean();
            return (sumOfSquares / n) - mean * mean;
        }

        public double std() {
            return Math.sqrt(variance());
        }

        public double min() {
            return min;
        }

        public double max() {
            return max;
        }
    }

    private Map<Integer, BucketMeta> computeHistogramMeta(List<FeatureMeta> meta,
        Dataset<EmptyContext, BootstrappedDatasetPartition> dataset) {

        List<NormalDistributionStats> stats = dataset.compute(
            x -> computeStatsOnPartition(x, meta),
            (l, r) -> reduceStats(l, r, meta)
        );

        Map<Integer, BucketMeta> bucketsMeta = new HashMap<>();
        for (int i = 0; i < stats.size(); i++) {
            BucketMeta bucketMeta = new BucketMeta(meta.get(i));
            if (!bucketMeta.getFeatureMeta().isCategoricalFeature()) {
                NormalDistributionStats stat = stats.get(i);
                bucketMeta.setMinValue(stat.min());
                bucketMeta.setBucketSize(stat.std() * BUCKET_SIZE_FACTOR);
            }
            bucketsMeta.put(i, bucketMeta);
        }
        return bucketsMeta;
    }

    List<NormalDistributionStats> computeStatsOnPartition(BootstrappedDatasetPartition part,
        List<FeatureMeta> meta) {

        double[] sumOfValues = new double[meta.size()];
        double[] sumOfSquares = new double[sumOfValues.length];
        double[] min = new double[sumOfValues.length];
        double[] max = new double[sumOfValues.length];
        Arrays.fill(min, Double.POSITIVE_INFINITY);
        Arrays.fill(max, Double.NEGATIVE_INFINITY);

        for (int i = 0; i < part.getRowsCount(); i++) {
            Vector vec = part.getRow(i).getFeatures();
            for (int featureId = 0; featureId < vec.size(); featureId++) {
                if (!meta.get(featureId).isCategoricalFeature()) {
                    double featureValue = vec.get(featureId);
                    sumOfValues[featureId] += featureValue;
                    sumOfSquares[featureId] += Math.pow(featureValue, 2);
                    min[featureId] = Math.min(min[featureId], featureValue);
                    max[featureId] = Math.max(max[featureId], featureValue);
                }
            }
        }

        ArrayList<NormalDistributionStats> res = new ArrayList<>();
        for (int featureId = 0; featureId < sumOfSquares.length; featureId++) {
            res.add(new NormalDistributionStats(
                min[featureId], max[featureId],
                sumOfSquares[featureId], sumOfValues[featureId],
                part.getRowsCount())
            );
        }
        return res;
    }

    List<NormalDistributionStats> reduceStats(List<NormalDistributionStats> left,
        List<NormalDistributionStats> right,
        List<FeatureMeta> meta) {

        if (left == null)
            return right;
        if (right == null)
            return left;

        assert meta.size() == left.size() && meta.size() == right.size();
        List<NormalDistributionStats> result = new ArrayList<>();
        for (int featureId = 0; featureId < meta.size(); featureId++) {
            NormalDistributionStats leftStat = left.get(featureId);
            NormalDistributionStats rightStat = right.get(featureId);
            result.add(leftStat.plus(rightStat));
        }
        return result;
    }

    private Queue<TreeNode> createRootsQueue() {
        Queue<TreeNode> roots = new LinkedList<>();
        for (int i = 0; i < countOfTrees; i++)
            roots.add(new TreeNode(1, i));
        return roots;
    }

    private Map<NodeId, TreeNode> getNodesToLearn(Queue<TreeNode> queue) {
        return nodesToLearnSelectionStrgy.apply(queue).stream()
            .collect(Collectors.toMap(TreeNode::getId, node -> node));
    }

    private List<TreeNode> defaultNodesToLearnSelectionStrgy(Queue<TreeNode> queue) {
        List<TreeNode> result = new ArrayList<>(queue);
        queue.clear();
        return result;
    }

    boolean needSplit(TreeNode parentNode, Optional<NodeSplit> split) {
        return split.isPresent() && parentNode.getImpurity() - split.get().getImpurity() > minImpurityDelta &&
            parentNode.getDepth() < (maxDepth + 1);
    }

    protected abstract ModelsComposition buildComposition(List<TreeRoot> models);

    Map<NodeId, NodeStatistics> aggregateStatistics(
        BootstrappedDatasetPartition dataset, ArrayList<TreeRoot> roots,
        Map<Integer, BucketMeta> histMeta,
        Map<NodeId, TreeNode> part) {

        Map<NodeId, NodeStatistics> res = part.keySet().stream()
            .collect(Collectors.toMap(n -> n, n -> new NodeStatistics(n, new HashMap<>())));

        dataset.foreach(vector -> {
            for (int sampleId = 0; sampleId < vector.getRepetitionsCounters().length; sampleId++) {
                if (vector.getRepetitionsCounters()[sampleId] == 0)
                    continue;

                TreeRoot root = roots.get(sampleId);
                NodeId key = root.node.predictNextNodeKey(vector.getFeatures());
                if (!part.containsKey(key))
                    continue;

                NodeStatistics statistics = res.get(key);
                for (Integer featureId : root.getUsedFeatures()) {
                    BucketMeta meta = histMeta.get(featureId);
                    if (!statistics.perFeatureStatistics.containsKey(featureId))
                        statistics.perFeatureStatistics.put(featureId, createImpurityComputer(sampleId, meta));
                    S impurityComputer = statistics.perFeatureStatistics.get(featureId);
                    impurityComputer.addElement(vector);
                }
            }
        });
        return res;
    }

    protected abstract S createImpurityComputer(int sampleId, BucketMeta meta);

    //TODO: need test
    Map<NodeId, NodeStatistics> reduce(Map<NodeId, NodeStatistics> left,
        Map<NodeId, NodeStatistics> right) {

        if (left == null)
            return right;
        if (right == null)
            return left;

        Map<NodeId, NodeStatistics> result = new HashMap<>(left);
        for (NodeId key : right.keySet()) {
            NodeStatistics rightVal = right.get(key);
            if (!result.containsKey(key))
                result.put(key, rightVal);
            else
                result.get(key).addOtherStatistics(rightVal.perFeatureStatistics);
        }

        return result;
    }

    class NodeStatistics {
        private final NodeId nodeId;
        private final Map<Integer, S> perFeatureStatistics; //not all features

        public NodeStatistics(NodeId nodeId, Map<Integer, S> perFeatureStatistics) {
            this.nodeId = nodeId;
            this.perFeatureStatistics = perFeatureStatistics;
        }

        public void addOtherStatistics(Map<Integer, S> other) {
            assert other.keySet().containsAll(perFeatureStatistics.keySet());

            for (Integer featureId : other.keySet()) {
                if (!perFeatureStatistics.containsKey(featureId))
                    perFeatureStatistics.put(featureId, other.get(featureId));
                else
                    perFeatureStatistics.get(featureId).addHist(other.get(featureId));
            }
        }

        public Optional<NodeSplit> findBestSplit() {
            return perFeatureStatistics.values().stream()
                .flatMap(x -> x.findBestSplit().map(Stream::of).orElse(Stream.empty()))
                .min(Comparator.comparingDouble(NodeSplit::getImpurity));
        }
    }

    public static class TreeRoot implements Model<Vector, Double> {
        private TreeNode node;
        private Set<Integer> usedFeatures;

        public TreeRoot(TreeNode root, Set<Integer> usedFeatures) {
            this.node = root;
            this.usedFeatures = usedFeatures;
        }

        @Override public Double apply(Vector vector) {
            return node.apply(vector);
        }

        public Set<Integer> getUsedFeatures() {
            return usedFeatures;
        }

        public TreeNode getNode() {
            return node;
        }

        public List<TreeNode> getLeafs() {
            List<TreeNode> res = new ArrayList<>();
            getLeafs(node, res);
            return res;
        }

        private void getLeafs(TreeNode root, List<TreeNode> res) {
            if (root.getType() == TreeNode.Type.LEAF)
                res.add(root);
            else {
                getLeafs(root.getLeft(), res);
                getLeafs(root.getRight(), res);
            }
        }
    }
}
