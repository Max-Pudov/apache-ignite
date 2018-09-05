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

package org.apache.ignite.ml.composition.boosting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckStrategy;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckStrategyFactory;
import org.apache.ignite.ml.composition.boosting.convergence.mean.MeanAbsValueCheckConvergenceStgyFactory;
import org.apache.ignite.ml.composition.boosting.learningrate.LearningRateOptimizer;
import org.apache.ignite.ml.composition.boosting.learningrate.LearningRateOptimizerFactory;
import org.apache.ignite.ml.composition.boosting.learningrate.stub.LearningRateOptimizerStubFactory;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceChecker;
import org.apache.ignite.ml.composition.boosting.convergence.ConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.boosting.convergence.mean.MeanAbsValueConvergenceCheckerFactory;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.NotNull;

/**
 * Learning strategy for gradient boosting.
 */
public class GDBLearningStrategy {
    /** Learning environment. */
    protected LearningEnvironment environment;

    /** Count of iterations. */
    protected int cntOfIterations;

    /** Loss of gradient. */
    protected IgniteTriFunction<Long, Double, Double, Double> lossGradient;

    /** External label to internal mapping. */
    protected IgniteFunction<Double, Double> externalLbToInternalMapping;

    /** Internal label to external mapping. */
    protected IgniteFunction<Double, Double> internalLbToExternalMapping;

    /** Base model trainer builder. */
    protected IgniteSupplier<DatasetTrainer<? extends Model<Vector, Double>, Double>> baseMdlTrainerBuilder;

    /** Mean label value. */
    protected double meanLabelValue;

    /** Sample size. */
    protected long sampleSize;

    /** Composition weights. */
    protected double[] compositionWeights;

    /** Check convergence strategy factory. */
    protected ConvergenceCheckerFactory checkConvergenceStgyFactory = new MeanAbsValueConvergenceCheckerFactory(0.001);

    /** Default gradient step size. */
    private double defaultGradStepSize;

    /** Learning rate optimizer factory. */
    protected LearningRateOptimizerFactory learningRateOptimizerFactory = new LearningRateOptimizerStubFactory(0.1);

    /**
     * Implementation of gradient boosting iterations. At each step of iterations this algorithm build a regression
     * model based on gradient of loss-function for current models composition.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @return list of learned models.
     */
    public <K, V> List<Model<Vector, Double>> learnModels(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        return update(null, datasetBuilder, featureExtractor, lbExtractor);
    }

    /**
     * Gets state of model in arguments, compare it with training parameters of trainer and if they are fit then
     * trainer updates model in according to new data and return new model. In other case trains new model.
     *
     * @param mdlToUpdate Learned model.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated models list.
     */
    public <K,V> List<Model<Vector, Double>> update(GDBTrainer.GDBModel mdlToUpdate,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, Double> lbExtractor) {

        List<Model<Vector, Double>> models = initLearningState(mdlToUpdate);

        ConvergenceChecker<K, V> convCheck = checkConvergenceStgyFactory.create(sampleSize,
            externalLbToInternalMapping, lossGradient, datasetBuilder, featureExtractor, lbExtractor);
        LearningRateOptimizer<K,V> rateOptimizer = learningRateOptimizerFactory.create(sampleSize,
            externalLbToInternalMapping, internalLbToExternalMapping, lossGradient, featureExtractor, lbExtractor);

        DatasetTrainer<? extends Model<Vector, Double>, Double> trainer = baseMdlTrainerBuilder.get();
        for (int i = 0; i < cntOfIterations; i++) {
            double[] weights = Arrays.copyOf(compositionWeights, models.size());

            WeightedPredictionsAggregator aggregator = new WeightedPredictionsAggregator(weights, meanLabelValue);
            ModelsComposition currComposition = new ModelsComposition(models, aggregator);
            if (convCheck.isConverged(datasetBuilder, currComposition))
                break;

            IgniteBiFunction<K, V, Double> lbExtractorWrap = (k, v) -> {
                Double realAnswer = externalLbToInternalMapping.apply(lbExtractor.apply(k, v));
                Double mdlAnswer = currComposition.apply(featureExtractor.apply(k, v));
                return -lossGradient.apply(sampleSize, realAnswer, mdlAnswer);
            };

            long startTs = System.currentTimeMillis();

            Model<Vector, Double> newMdl = trainer.fit(datasetBuilder, featureExtractor, lbExtractorWrap);
            compositionWeights[i] = rateOptimizer.learnRate(datasetBuilder, currComposition, newMdl);
            models.add(newMdl);
            double learningTime = (double)(System.currentTimeMillis() - startTs) / 1000.0;
            environment.logger(getClass()).log(MLLogger.VerboseLevel.LOW, "One model training time was %.2fs", learningTime);
        }

        return models;
    }

    /**
     * Restores state of already learned model if can and sets learning parameters according to this state.
     *
     * @param mdlToUpdate Model to update.
     * @return list of already learned models.
     */
    @NotNull protected List<Model<Vector, Double>> initLearningState(GDBTrainer.GDBModel mdlToUpdate) {
        List<Model<Vector, Double>> models = new ArrayList<>();
        if(mdlToUpdate != null) {
            models.addAll(mdlToUpdate.getModels());
            WeightedPredictionsAggregator aggregator = (WeightedPredictionsAggregator) mdlToUpdate.getPredictionsAggregator();
            meanLabelValue = aggregator.getBias();
            compositionWeights = new double[models.size() + cntOfIterations];
            for(int i = 0; i < models.size(); i++)
                compositionWeights[i] = aggregator.getWeights()[i];
        } else {
            compositionWeights = new double[cntOfIterations];
        }

        Arrays.fill(compositionWeights, models.size(), compositionWeights.length, defaultGradStepSize);
        return models;
    }

    /**
     * Sets learning environment.
     *
     * @param environment Learning Environment.
     */
    public GDBLearningStrategy withEnvironment(LearningEnvironment environment) {
        this.environment = environment;
        return this;
    }

    /**
     * Sets count of iterations.
     *
     * @param cntOfIterations Count of iterations.
     */
    public GDBLearningStrategy withCntOfIterations(int cntOfIterations) {
        this.cntOfIterations = cntOfIterations;
        return this;
    }

    /**
     * Sets gradient of loss function.
     *
     * @param lossGradient Loss gradient.
     */
    public GDBLearningStrategy withLossGradient(IgniteTriFunction<Long, Double, Double, Double> lossGradient) {
        this.lossGradient = lossGradient;
        return this;
    }

    /**
     * Sets external to internal label representation mapping.
     *
     * @param externalLbToInternal External label to internal.
     */
    public GDBLearningStrategy withExternalLabelToInternal(IgniteFunction<Double, Double> externalLbToInternal) {
        this.externalLbToInternalMapping = externalLbToInternal;
        return this;
    }

    /**
     * Sets internal to external label representation mapping.
     *
     * @param mapping External label to internal.
     */
    public GDBLearningStrategy withInternalLabelToExternal(IgniteFunction<Double, Double> mapping) {
        this.internalLbToExternalMapping = mapping;
        return this;
    }

    /**
     * Sets base model builder.
     *
     * @param buildBaseMdlTrainer Build base model trainer.
     */
    public GDBLearningStrategy withBaseModelTrainerBuilder(
        IgniteSupplier<DatasetTrainer<? extends Model<Vector, Double>, Double>> buildBaseMdlTrainer) {
        this.baseMdlTrainerBuilder = buildBaseMdlTrainer;
        return this;
    }

    /**
     * Sets mean label value.
     *
     * @param meanLabelValue Mean label value.
     */
    public GDBLearningStrategy withMeanLabelValue(double meanLabelValue) {
        this.meanLabelValue = meanLabelValue;
        return this;
    }

    /**
     * Sets sample size.
     *
     * @param sampleSize Sample size.
     */
    public GDBLearningStrategy withSampleSize(long sampleSize) {
        this.sampleSize = sampleSize;
        return this;
    }

    /**
     * Sets composition weights vector.
     *
     * @param compositionWeights Composition weights.
     */
    public GDBLearningStrategy withCompositionWeights(double[] compositionWeights) {
        this.compositionWeights = compositionWeights;
        return this;
    }

    /**
     * Sets CheckConvergenceStgyFactory.
     *
     * @param factory Factory.
     */
    public GDBLearningStrategy withCheckConvergenceStgyFactory(ConvergenceCheckerFactory factory) {
        this.checkConvergenceStgyFactory = factory;
        return this;
    }

    /**
     * Sets default gradient step size.
     *
     * @param defaultGradStepSize Default gradient step size.
     */
    public GDBLearningStrategy withDefaultGradStepSize(double defaultGradStepSize) {
        this.defaultGradStepSize = defaultGradStepSize;
        return this;
    }

    /** */
    public double[] getCompositionWeights() {
        return compositionWeights;
    }

    /** */
    public double getMeanValue() {
        return meanLabelValue;
    }

    /**
     * Sets LearningRateOptimizerFactory.
     *
     * @param factory Factory.
     */
    public GDBLearningStrategy withLearningRateOptimizerFactory(LearningRateOptimizerFactory factory) {
        this.learningRateOptimizerFactory = factory;
        return this;
    }
}
