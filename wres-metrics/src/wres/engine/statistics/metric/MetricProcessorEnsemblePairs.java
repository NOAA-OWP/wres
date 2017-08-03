package wres.engine.statistics.metric;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.EnsemblePairs;
import wres.datamodel.metric.MetricConstants.MetricInputGroup;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputForProjectByThreshold;
import wres.datamodel.metric.MetricOutputMapByMetric;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ProbabilityThreshold;
import wres.datamodel.metric.QuantileThreshold;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Slicer;
import wres.datamodel.metric.Threshold;
import wres.datamodel.metric.VectorOutput;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * {@link EnsemblePairs} and configured transformations of {@link EnsemblePairs}. For example, metrics that consume
 * {@link SingleValuedPairs} may be processed after transforming the {@link EnsemblePairs} with an appropriate mapping
 * function, such as an ensemble mean.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

final class MetricProcessorEnsemblePairs extends MetricProcessor
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DiscreteProbabilityPairs} and produce
     * {@link VectorOutput}.
     */

    private final MetricCollection<DiscreteProbabilityPairs, VectorOutput> discreteProbabilityVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     */

    private final MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput> discreteProbabilityMultiVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce {@link VectorOutput}.
     */

    private final MetricCollection<EnsemblePairs, VectorOutput> ensembleVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce
     * {@link MultiVectorOutput}.
     */

    private final MetricCollection<EnsemblePairs, MultiVectorOutput> ensembleMultiVector;

    /**
     * Default function that maps between ensemble pairs and single-valued pairs.
     */

    private final Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> toSingleValues;

    /**
     * Function to map between ensemble pairs and discrete probabilities.
     */

    private final BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> toDiscreteProbabilities;

    @Override
    public MetricOutputForProjectByThreshold apply(MetricInput<?> t)
    {
        if(!(t instanceof EnsemblePairs))
        {
            throw new MetricCalculationException("Expected ensemble pairs for metric processing.");
        }

        //Slicer
        Slicer slicer = dataFactory.getSlicer();

        //Metric futures
        MetricFutures futures = new MetricFutures();

        //Process the metrics that consume ensemble pairs
        if(hasMetrics(MetricInputGroup.ENSEMBLE))
        {
            processEnsemblePairs((EnsemblePairs)t, futures);
        }
        //Process the metrics that consume single-valued pairs
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED))
        {
            //Derive the single-valued pairs from the ensemble pairs using the configured mapper
            SingleValuedPairs input = slicer.transformPairs((EnsemblePairs)t, toSingleValues);
            processSingleValuedPairs(input, futures);
        }
        //Process the metrics that consume discrete probability pairs
        if(hasMetrics(MetricInputGroup.DISCRETE_PROBABILITY))
        {
            processDiscreteProbabilityPairs((EnsemblePairs)t, futures);
        }

        //Process and return the result        
        return futures.getMetricOutput();
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     */

    MetricProcessorEnsemblePairs(DataFactory dataFactory, ProjectConfig config)
    {
        super(dataFactory, config);
        //Validate the configuration
        if(hasMetrics(MetricInputGroup.DICHOTOMOUS))
        {
            throw new IllegalArgumentException("Cannot configure dichotomous metrics for ensemble inputs: correct the configuration '"
                + config.getLabel() + "'.");
        }
        if(hasMetrics(MetricInputGroup.MULTICATEGORY))
        {
            throw new IllegalArgumentException("Cannot configure multicategory metrics for ensemble inputs: correct the configuration '"
                + config.getLabel() + "'.");
        }
        //Construct the metrics
        if(hasMetrics(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR))
        {
            discreteProbabilityVector =
                                      metricFactory.ofDiscreteProbabilityVectorCollection(getSelectedMetrics(metrics,
                                                                                                             MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                                             MetricOutputGroup.VECTOR));
        }
        else
        {
            discreteProbabilityVector = null;
        }
        if(hasMetrics(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR))
        {
            discreteProbabilityMultiVector =
                                           metricFactory.ofDiscreteProbabilityMultiVectorCollection(getSelectedMetrics(metrics,
                                                                                                                       MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                                                       MetricOutputGroup.MULTIVECTOR));
        }
        else
        {
            discreteProbabilityMultiVector = null;
        }

        //TODO: implement the ensemble metrics
        ensembleVector = null;
        ensembleMultiVector = null;

//        if(hasMetrics(MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR))
//        {
//            ensembleVector =
//                              metricFactory.ofEnsembleVectorCollection(getSelectedMetrics(metrics,
//                                                                                             MetricInputGroup.ENSEMBLE,
//                                                                                             MetricOutputGroup.VECTOR));
//        }
//        else
//        {
//            ensembleVector = null;
//        }
//        if(hasMetrics(MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR))
//        {
//            ensembleMultiVector =
//                                metricFactory.ofEnsembleMultiVectorCollection(getSelectedMetrics(metrics,
//                                                                                                 MetricInputGroup.ENSEMBLE,
//                                                                                                 MetricOutputGroup.MULTIVECTOR));
//        }
//        else
//        {
//            ensembleMultiVector = null;
//        }                   

        //Construct the default mapper from ensembles to single-values: this is not currently configurable
        toSingleValues = in -> dataFactory.pairOf(in.getItemOne(),
                                                  Arrays.stream(in.getItemTwo()).average().getAsDouble());

        //Construct the default mapper from ensembles to probabilities: this is not currently configurable
        toDiscreteProbabilities = dataFactory.getSlicer()::transformPair;
    }

    /**
     * Processes a set of metric futures that consume {@link EnsemblePairs}, which are mapped from the input pairs,
     * {@link EnsemblePairs}, using a configured mapping function.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     */

    void processEnsemblePairs(EnsemblePairs input, MetricFutures futures)
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.ENSEMBLE))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.ENSEMBLE);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processEnsembleThreshold(threshold,
                                                                     sorted,
                                                                     input,
                                                                     ensembleVector,
                                                                     futures.vector));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new UnsupportedOperationException(unsupportedException);
            }
        }
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.ENSEMBLE))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.ENSEMBLE);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processEnsembleThreshold(threshold,
                                                                     sorted,
                                                                     input,
                                                                     ensembleMultiVector,
                                                                     futures.multivector));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new UnsupportedOperationException(unsupportedException);
            }
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DiscreteProbabilityPairs}, which are mapped from the input
     * pairs, {@link EnsemblePairs}, using a configured mapping function.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     */

    void processDiscreteProbabilityPairs(EnsemblePairs input, MetricFutures futures)
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.DISCRETE_PROBABILITY))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.DISCRETE_PROBABILITY);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processDiscreteProbabilityThreshold(threshold,
                                                                                sorted,
                                                                                input,
                                                                                discreteProbabilityVector,
                                                                                futures.vector));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new UnsupportedOperationException(unsupportedException);
            }
        }
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.DISCRETE_PROBABILITY))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.DISCRETE_PROBABILITY);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processDiscreteProbabilityThreshold(threshold,
                                                                                sorted,
                                                                                input,
                                                                                discreteProbabilityMultiVector,
                                                                                futures.multivector));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new UnsupportedOperationException(unsupportedException);
            }
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link EnsemblePairs} at a specific
     * {@link Threshold} and appends it to the input map of futures.
     * 
     * @param threshold the threshold
     * @param sorted a sorted set of values from which to determine {@link QuantileThreshold} where the input
     *            {@link Threshold} is a {@link ProbabilityThreshold}.
     * @param pairs the pairs
     * @param collection the metric collection
     * @param futures the collection of futures to which the new future will be added
     */

    private <T extends MetricOutput<?>> void processDiscreteProbabilityThreshold(Threshold threshold,
                                                                                 double[] sorted,
                                                                                 EnsemblePairs pairs,
                                                                                 MetricCollection<DiscreteProbabilityPairs, T> collection,
                                                                                 Map<Threshold, CompletableFuture<MetricOutputMapByMetric<T>>> futures)
    {
        Threshold useMe = threshold;
        //Quantile required: need to determine real-value from probability
        if(threshold instanceof ProbabilityThreshold)
        {
            useMe = dataFactory.getSlicer().getQuantileFromProbability((ProbabilityThreshold)useMe, sorted);
        }
        //Slice the pairs
        DiscreteProbabilityPairs transformed = dataFactory.getSlicer()
                                                          .transformPairs(pairs, threshold, toDiscreteProbabilities);
        futures.put(useMe, CompletableFuture.supplyAsync(() -> collection.apply(transformed)));
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link EnsemblePairs} at a specific
     * {@link Threshold} and appends it to the input map of futures.
     * 
     * @param threshold the threshold
     * @param sorted a sorted set of values from which to determine {@link QuantileThreshold} where the input
     *            {@link Threshold} is a {@link ProbabilityThreshold}.
     * @param pairs the pairs
     * @param collection the metric collection
     * @param futures the collection of futures to which the new future will be added
     */

    private <T extends MetricOutput<?>> void processEnsembleThreshold(Threshold threshold,
                                                                      double[] sorted,
                                                                      EnsemblePairs pairs,
                                                                      MetricCollection<EnsemblePairs, T> collection,
                                                                      Map<Threshold, CompletableFuture<MetricOutputMapByMetric<T>>> futures)
    {
        Threshold useMe = threshold;
        //Quantile required: need to determine real-value from probability
        if(threshold instanceof ProbabilityThreshold)
        {
            useMe = dataFactory.getSlicer().getQuantileFromProbability((ProbabilityThreshold)useMe, sorted);
        }
        //Slice the pairs
        EnsemblePairs subset = dataFactory.getSlicer().sliceByLeft(pairs, useMe);
        futures.put(useMe, CompletableFuture.supplyAsync(() -> collection.apply(subset)));
    }

    /**
     * Helper that returns a sorted set of values from the left side of the input pairs if any of the thresholds are
     * {@link ProbabilityThreshold}.
     * 
     * @param input the inputs pairs
     * @param thresholds the thresholds to test
     * @return a sorted array of values or null
     */

    private double[] getSortedLeftSide(EnsemblePairs input, List<Threshold> thresholds)
    {
        double[] sorted = null;
        if(hasProbabilityThreshold(thresholds))
        {
            sorted = dataFactory.getSlicer().getLeftSide(input);
            Arrays.sort(sorted);
        }
        return sorted;
    }

}
