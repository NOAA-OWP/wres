package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.EnsemblePairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetricConstants;
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
import wres.datamodel.metric.Threshold;
import wres.datamodel.metric.Threshold.Condition;
import wres.datamodel.metric.VectorOutput;

/**
 * <p>
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for all configured
 * {@link Threshold}. Typically, this will represent a single forecast lead time within a processing pipeline.
 * </p>
 * <p>
 * The current implementation adopts the following simplifying assumptions:
 * </p>
 * <ol>
 * <li>That a global set of {@link Threshold} is defined for all {@link Metric} within a {@link MetricCollection}. Using
 * metric-specific thresholds will require additional logic to break-up the {@link MetricCollection}.</li>
 * <li>If the {@link Threshold} are {@link ProbabilityThreshold}, the corresponding {@link QuantileThreshold} are
 * derived from the observations associated with the {@link MetricInput} to {@link #apply(MetricInput)}. When other
 * datasets are required to derive the {@link QuantileThreshold} (e.g. all historical observations), they will need to
 * be associated with the {@link MetricInput}.</li>
 * </ol>
 * <p>
 * Upon construction, the project configuration is validated to ensure that appropriate {@link Metric} are configured
 * for the type of {@link MetricInput} consumed. These metrics are stored in a series of {@link MetricCollection} that
 * consume a given {@link MetricInput} and produce a given {@link MetricOutput}. If the type of {@link MetricInput}
 * consumed by any given {@link MetricCollection} differs from the {@link MetricInput} from which the
 * {@link MetricProcessor} is constructed, a transformation must be applied. For example, {@link Metric} that consume
 * {@link SingleValuedPairs} may be computed for {@link EnsemblePairs} if an appropriate transformation is configured.
 * Subclasses must define and apply any transformation required.
 * </p>
 * Upon calling {@link #apply(Object)} with a concrete {@link MetricInput}, the configured {@link Metric} are computed
 * asynchronously for each {@link Threshold}. These asynchronous tasks are stored in a {@link MetricFutures} whose
 * method, {@link MetricFutures#getMetricOutput()} returns the full suite of results in a
 * {@link MetricOutputForProjectByThreshold}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

abstract class MetricProcessor implements Function<MetricInput<?>, MetricOutputForProjectByThreshold>
{

    /**
     * Instance of a {@link MetricFactory}.
     */

    final MetricFactory metricFactory;

    /**
     * Instance of a {@link DataFactory}.
     */

    final DataFactory dataFactory;

    /**
     * List of thresholds associated with each metric.
     */

    final EnumMap<MetricConstants, List<Threshold>> localThresholds;

    /**
     * List of thresholds that apply to all metrics within a {@link MetricInputGroup}.
     */

    final EnumMap<MetricInputGroup, List<Threshold>> globalThresholds;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     */

    final MetricCollection<SingleValuedPairs, ScalarOutput> singleValuedScalar;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link VectorOutput}.
     */

    final MetricCollection<SingleValuedPairs, VectorOutput> singleValuedVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorOutput}.
     */

    final MetricCollection<SingleValuedPairs, MultiVectorOutput> singleValuedMultiVector;

    /**
     * The list of metrics associated with the verification project.
     */

    final List<MetricConstants> metrics;

    /**
     * Returns an instance of a metric processor based on {@link ProjectConfig}
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     */

    public static MetricProcessor of(DataFactory dataFactory, ProjectConfig config)
    {
        switch(getInputType(config))
        {
            case SINGLE_VALUED:
                return new MetricProcessorSingleValuedPairs(dataFactory, config);
            case ENSEMBLE:
                return new MetricProcessorEnsemblePairs(dataFactory, config);
            default:
                throw new UnsupportedOperationException("Unsupported input type in the project configuration '" + config
                    + "'");
        }
    }

    /**
     * Returns true if metrics are available for the input {@link MetricInputGroup} and {@link MetricOutputGroup}, false
     * otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if metrics are available for the input {@link MetricInputGroup} and {@link MetricOutputGroup}, false
     *         otherwise
     */

    public boolean hasMetrics(MetricInputGroup inGroup, MetricOutputGroup outGroup)
    {
        return getSelectedMetrics(metrics, inGroup, outGroup).length > 0;
    }
    
    /**
     * Returns true if metrics are available for the input {@link MetricInputGroup}, false
     * otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @return true if metrics are available for the input {@link MetricInputGroup} false
     *         otherwise
     */

    public boolean hasMetrics(MetricInputGroup inGroup)
    {
        return metrics.stream().anyMatch(a -> a.isInGroup(inGroup));

    }    
    
    /**
     * Returns true if metrics are available for the input {@link MetricOutputGroup}, false
     * otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if metrics are available for the input {@link MetricOutputGroup} false
     *         otherwise
     */

    public boolean hasMetrics(MetricOutputGroup outGroup)
    {
        return metrics.stream().anyMatch(a -> a.isInGroup(outGroup));
    }        

    /**
     * Constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     */

    MetricProcessor(final DataFactory dataFactory, final ProjectConfig config)
    {
        Objects.requireNonNull(config,
                               "Specify a non-null project configuration from which to construct the metric processor.");
        Objects.requireNonNull(dataFactory,
                               "Specify a non-null data factory from which to construct the metric processor.");
        this.dataFactory = dataFactory;
        metrics = getMetricsFromConfig(config);
        metricFactory = MetricFactory.getInstance(dataFactory);
        //Construct the metrics that are common to more than one type of input pairs
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR))
        {
            singleValuedScalar =
                               metricFactory.ofSingleValuedScalarCollection(getSelectedMetrics(metrics,
                                                                                               MetricInputGroup.SINGLE_VALUED,
                                                                                               MetricOutputGroup.SCALAR));
        }
        else
        {
            singleValuedScalar = null;
        }
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR))
        {
            singleValuedVector =
                               metricFactory.ofSingleValuedVectorCollection(getSelectedMetrics(metrics,
                                                                                               MetricInputGroup.SINGLE_VALUED,
                                                                                               MetricOutputGroup.VECTOR));
        }
        else
        {
            singleValuedVector = null;
        }
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR))
        {
            singleValuedMultiVector =
                                    metricFactory.ofSingleValuedMultiVectorCollection(getSelectedMetrics(metrics,
                                                                                                         MetricInputGroup.SINGLE_VALUED,
                                                                                                         MetricOutputGroup.MULTIVECTOR));
        }
        else
        {
            singleValuedMultiVector = null;
        }
        //Obtain the thresholds for each metric and store them
        localThresholds = new EnumMap<>(MetricConstants.class);
        globalThresholds = new EnumMap<>(MetricInputGroup.class);
        setThresholds(dataFactory, config);
    }

    /**
     * Returns true if the input list of thresholds contains one or more probability thresholds, false otherwise.
     * 
     * @return true if the input list contains one or more probability thresholds, false otherwise
     */

    boolean hasProbabilityThreshold(List<Threshold> check)
    {
        for(Threshold next: check)
        {
            if(next instanceof ProbabilityThreshold)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Processes a set of metric futures for {@link SingleValuedPairs}.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     */

    void processSingleValuedPairs(SingleValuedPairs input, MetricFutures futures)
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.SINGLE_VALUED))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.SINGLE_VALUED);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processSingleValuedThreshold(threshold,
                                                                         sorted,
                                                                         input,
                                                                         singleValuedScalar,
                                                                         futures.scalar));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new UnsupportedOperationException(unsupportedException);
            }
        }
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.SINGLE_VALUED))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.SINGLE_VALUED);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processSingleValuedThreshold(threshold,
                                                                         sorted,
                                                                         input,
                                                                         singleValuedVector,
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
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.SINGLE_VALUED))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.SINGLE_VALUED);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processSingleValuedThreshold(threshold,
                                                                         sorted,
                                                                         input,
                                                                         singleValuedMultiVector,
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
     * Returns true if global thresholds are available for a particular {@link MetricInputGroup}, false otherwise.
     * 
     * @return true if global thresholds are available for a {@link MetricInputGroup}, false otherwise
     */

    boolean hasGlobalThresholds(MetricInputGroup group)
    {
        return globalThresholds.containsKey(group);
    }

    /**
     * Store of metric futures for each output type. Use {@ link #getMetricOutput()} to obtain the processed
     * {@link MetricOutputForProjectByThreshold}, which blocks until all results are available.
     */

    class MetricFutures
    {
        final Map<Threshold, CompletableFuture<MetricOutputMapByMetric<ScalarOutput>>> scalar = new HashMap<>();
        final Map<Threshold, CompletableFuture<MetricOutputMapByMetric<VectorOutput>>> vector = new HashMap<>();
        final Map<Threshold, CompletableFuture<MetricOutputMapByMetric<MultiVectorOutput>>> multivector =
                                                                                                        new HashMap<>();
        final Map<Threshold, CompletableFuture<MetricOutputMapByMetric<MatrixOutput>>> matrix = new HashMap<>();

        /**
         * Returns the results associated with the futures. This method is blocking.
         * 
         * @return the metric results
         */

        MetricOutputForProjectByThreshold getMetricOutput()
        {
            MetricOutputForProjectByThreshold.Builder builder = dataFactory.ofMetricOutputForProjectByThreshold();
            if(!scalar.isEmpty())
            {
                scalar.forEach((threshold, future) -> builder.addScalarOutput(threshold, future.join()));
            }
            if(!vector.isEmpty())
            {
                vector.forEach((threshold, future) -> builder.addVectorOutput(threshold, future.join()));
            }
            if(!multivector.isEmpty())
            {
                multivector.forEach((threshold, future) -> builder.addMultiVectorOutput(threshold, future.join()));
            }
            if(!matrix.isEmpty())
            {
                matrix.forEach((threshold, future) -> builder.addMatrixOutput(threshold, future.join()));
            }
            return builder.build();
        }
    }

    /**
     * Returns a set of {@link MetricConstants} for a specified {@link MetricInputGroup} and {@link MetricOutputGroup}.
     * 
     * @param input the input constants
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return a set of {@link MetricConstants} for a specified {@link MetricInputGroup} and {@link MetricOutputGroup}
     *         or an empty array
     */

    MetricConstants[] getSelectedMetrics(List<MetricConstants> input,
                                         MetricInputGroup inGroup,
                                         MetricOutputGroup outGroup)
    {
        Objects.requireNonNull(input, "Specify a non-null array of metric identifiers from which to select metrics.");
        //Find the matching metrics 
        Set<MetricConstants> metrics = MetricConstants.getMetrics(inGroup, outGroup);
        metrics.removeIf(a -> !input.contains(a));
        return metrics.toArray(new MetricConstants[metrics.size()]);
    }

    /**
     * Returns a set of {@link MetricConstants} from a {@link ProjectConfig}.
     * 
     * @param config the project configuration
     * @return a set of {@link MetricConstants}
     */

    List<MetricConstants> getMetricsFromConfig(ProjectConfig config)
    {
        Objects.requireNonNull(config, "Specify a non-null project from which to generate metrics.");
        //Obtain the list of metrics
        List<MetricConfigName> metricsConfig = config.getOutputs()
                                                     .getMetric()
                                                     .stream()
                                                     .map(MetricConfig::getValue)
                                                     .collect(Collectors.toList());
        List<MetricConstants> metrics = new ArrayList<>();
        metricsConfig.forEach(a -> metrics.add(fromMetricConfigName(a)));
        return metrics;
    }

    /**
     * Helper that returns a sorted set of values from the left side of the input pairs if any of the thresholds are
     * {@link ProbabilityThreshold}.
     * 
     * @param input the inputs pairs
     * @param thresholds the thresholds to test
     * @return a sorted array of values or null
     */

    double[] getSortedLeftSide(SingleValuedPairs input, List<Threshold> thresholds)
    {
        double[] sorted = null;
        if(hasProbabilityThreshold(thresholds))
        {
            sorted = dataFactory.getSlicer().getLeftSide(input);
            Arrays.sort(sorted);
        }
        return sorted;
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link SingleValuedPairs} at a specific
     * {@link Threshold} and appends it to the input map of futures.
     * 
     * @param threshold the threshold
     * @param sorted a sorted set of values from which to determine {@link QuantileThreshold} where the input
     *            {@link Threshold} is a {@link ProbabilityThreshold}.
     * @param pairs the pairs
     * @param collection the metric collection
     * @param futures the collection of futures to which the new future will be added
     */

    private <T extends MetricOutput<?>> void processSingleValuedThreshold(Threshold threshold,
                                                                          double[] sorted,
                                                                          SingleValuedPairs pairs,
                                                                          MetricCollection<SingleValuedPairs, T> collection,
                                                                          Map<Threshold, CompletableFuture<MetricOutputMapByMetric<T>>> futures)
    {
        Threshold useMe = threshold;
        //Quantile required: need to determine real-value from probability
        if(threshold instanceof ProbabilityThreshold)
        {
            useMe = dataFactory.getSlicer().getQuantileFromProbability((ProbabilityThreshold)useMe, sorted);
        }
        //Slice the pairs
        SingleValuedPairs subset = dataFactory.getSlicer().sliceByLeft(pairs, useMe);
        futures.put(useMe, CompletableFuture.supplyAsync(() -> collection.apply(subset)));
    }

    /**
     * Sets the thresholds for each metric in the configuration, including any thresholds that apply globally (to all
     * metrics).
     * 
     * @param dataFactory a data factory
     * @param config the project configuration
     */

    private void setThresholds(DataFactory dataFactory, ProjectConfig config)
    {
        EnumMap<MetricConstants, List<Threshold>> localThresholds = new EnumMap<>(MetricConstants.class);
        config.getOutputs().getMetric().forEach(metric -> {
            //Obtain conditions here
            List<Threshold> thresholds = new ArrayList<>();
            //TODO: implement this when threshold conditions are available in the configuration
            if(!thresholds.isEmpty())
            {
                localThresholds.put(fromMetricConfigName(metric.getValue()), thresholds);
            }
        });
        if(!localThresholds.isEmpty())
        {
            this.localThresholds.putAll(localThresholds);
        }
        //TODO: implement this when threshold conditions are available in the configuration
        List<Threshold> globalThresholds = new ArrayList<>();
        //Set a single global threshold representing all data until thresholds are available
        globalThresholds.add(dataFactory.getThreshold(Double.NEGATIVE_INFINITY, Condition.GREATER));
        //Only set the global thresholds if no local ones are available
        //TODO: determine which metric groups have global thresholds: currently uses same thresholds for all
        if(localThresholds.isEmpty())
        {
            for(MetricInputGroup group: MetricInputGroup.values())
            {
                this.globalThresholds.put(group, globalThresholds);
            }
        }
    }

    /**
     * Returns the metric data input type from the {@link ProjectConfig}.
     * 
     * @param config the {@link ProjectConfig}
     * @return the {@link MetricInputGroup} based on the {@link ProjectConfig}
     */

    private static MetricInputGroup getInputType(ProjectConfig config)
    {
        Objects.requireNonNull(config, "Specify a non-null project from which to generate metrics.");
        DatasourceType type = config.getInputs().getRight().getType();
        switch(type)
        {
            case ENSEMBLE_FORECASTS:
                return MetricInputGroup.ENSEMBLE;
            case SIMPLE_FORECASTS:
            case ASSIMILATIONS:
                return MetricInputGroup.SINGLE_VALUED;
            default:
                throw new UnsupportedOperationException("Unable to interpret the input type '" + type
                    + "' when attempting to process the metrics ");
        }
    }

    /**
     * Maps between metric identifiers in {@link MetricConfigName} and those in {@link MetricConstants}.
     * 
     * @param translate the input {@link MetricConfigName}
     * @return the corresponding {@link MetricConstants}.
     */

    private static MetricConstants fromMetricConfigName(MetricConfigName translate)
    {
        Objects.requireNonNull(translate,
                               "One or more metric identifiers in the project configuration could not be mapped "
                                   + "to a supported metric identifier.");
        switch(translate)
        {
            case BIAS_FRACTION:
                return MetricConstants.BIAS_FRACTION;
            case BRIER_SCORE:
                return MetricConstants.BRIER_SCORE;
            case BRIER_SKILL_SCORE:
                return MetricConstants.BRIER_SKILL_SCORE;
            case COEFFICIENT_OF_DETERMINATION:
                return MetricConstants.COEFFICIENT_OF_DETERMINATION;
            case CONTINGENCY_TABLE:
                return MetricConstants.CONTINGENCY_TABLE;
            case CORRELATION_PEARSONS:
                return MetricConstants.CORRELATION_PEARSONS;
            case CRITICAL_SUCCESS_INDEX:
                return MetricConstants.CRITICAL_SUCCESS_INDEX;
            case EQUITABLE_THREAT_SCORE:
                return MetricConstants.EQUITABLE_THREAT_SCORE;
            case MEAN_ABSOLUTE_ERROR:
                return MetricConstants.MEAN_ABSOLUTE_ERROR;
            case MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return MetricConstants.MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE;
            case MEAN_ERROR:
                return MetricConstants.MEAN_ERROR;
            case MEAN_SQUARE_ERROR:
                return MetricConstants.MEAN_SQUARE_ERROR;
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
            case PEIRCE_SKILL_SCORE:
                return MetricConstants.PEIRCE_SKILL_SCORE;
            case PROBABILITY_OF_DETECTION:
                return MetricConstants.PROBABILITY_OF_DETECTION;
            case QUANTILE_QUANTILE_DIAGRAM:
                return MetricConstants.QUANTILE_QUANTILE_DIAGRAM;
            case PROBABILITY_OF_FALSE_DETECTION:
                return MetricConstants.PROBABILITY_OF_FALSE_DETECTION;
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                return MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM;
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE;
            case RELIABILITY_DIAGRAM:
                return MetricConstants.RELIABILITY_DIAGRAM;
            case ROOT_MEAN_SQUARE_ERROR:
                return MetricConstants.ROOT_MEAN_SQUARE_ERROR;
            default:
                throw new IllegalArgumentException("Unrecognized metric identifier in project configuration '"
                    + translate + "'.");
        }
    }

}
