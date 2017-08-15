package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.ThresholdOperator;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.EnsemblePairs;
import wres.datamodel.metric.MapBiKey;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricInputGroup;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.metric.MetricOutputMapByMetric;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ProbabilityThreshold;
import wres.datamodel.metric.QuantileThreshold;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Threshold;
import wres.datamodel.metric.Threshold.Operator;
import wres.datamodel.metric.VectorOutput;

/**
 * <p>
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for all configured
 * {@link Threshold}. Typically, this will represent a single forecast lead time within a processing pipeline. The
 * {@link MetricCollection} are computed by calling {@link #apply(Object)}.
 * </p>
 * <p>
 * The current implementation adopts the following simplifying assumptions:
 * </p>
 * <ol>
 * <li>That a global set of {@link Threshold} is defined for all {@link Metric} within a {@link ProjectConfig} and hence
 * {@link MetricCollection}. Using metric-specific thresholds will require additional logic to disaggregate a
 * {@link MetricCollection} into {@link Metric} for which common thresholds are defined.</li>
 * <li>If the {@link Threshold} are {@link ProbabilityThreshold}, the corresponding {@link QuantileThreshold} are
 * derived from the observations associated with the {@link MetricInput} at runtime, i.e. upon calling
 * {@link #apply(Object)}. When other datasets are required to derive the {@link QuantileThreshold} (e.g. all historical
 * observations), they will need to be associated with the {@link MetricInput}.</li>
 * </ol>
 * <p>
 * Upon construction, the {@link ProjectConfig} is validated to ensure that appropriate {@link Metric} are configured
 * for the type of {@link MetricInput} consumed. These metrics are stored in a series of {@link MetricCollection} that
 * consume a given {@link MetricInput} and produce a given {@link MetricOutput}. If the type of {@link MetricInput}
 * consumed by any given {@link MetricCollection} differs from the {@link MetricInput} for which the
 * {@link MetricProcessor} is primed, a transformation must be applied. For example, {@link Metric} that consume
 * {@link SingleValuedPairs} may be computed for {@link EnsemblePairs} if an appropriate transformation is configured.
 * Subclasses must define and apply any transformation required. If inappropriate {@link MetricInput} are provided to
 * {@link #apply(Object)} for the {@link MetricCollection} configured, an unchecked {@link MetricCalculationException}
 * will be thrown. If metrics are configured incorrectly, a checked {@link MetricConfigurationException} will be thrown.
 * </p>
 * <p>
 * Upon calling {@link #apply(Object)} with a concrete {@link MetricInput}, the configured {@link Metric} are computed
 * asynchronously for each {@link Threshold}. These asynchronous tasks are stored in a {@link MetricFutures} whose
 * method, {@link MetricFutures#getMetricOutput()} returns the full suite of results in a
 * {@link MetricOutputForProjectByLeadThreshold}.
 * </p>
 * <p>
 * The {@link MetricOutput} are computed and stored by {@link MetricOutputGroup}. For {@link MetricOutput} that are not
 * consumed until the end of a processing pipeline, the results from sequential calls to {@link #apply(Object)} may be
 * cached and merged. This is achieved by constructing a {@link MetricProcessor} with a <code>vararg</code> of
 * {@link MetricOutputGroup} whose results will be cached across successive calls. The merged results are accessible
 * from the final call to {@link #apply(Object)} or by calling {@link #getStoredMetricOutput()}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class MetricProcessor implements Function<MetricInput<?>, MetricOutputForProjectByLeadThreshold>
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
     * An array of {@link MetricOutputGroup} that should be retained and merged across calls. May be null.
     */

    final MetricOutputGroup[] mergeList;

    /**
     * The metric futures from previous calls.
     */

    MetricFutures futures = null;

    /**
     * Default logger.
     */

    static final Logger LOGGER = LoggerFactory.getLogger(MetricProcessor.class);

    /**
     * Returns a {@link MetricOutputForProjectByLeadThreshold} for the last available results or null if
     * {@link #hasStoredMetricOutput()} returns false.
     * 
     * @return a {@link MetricOutputForProjectByLeadThreshold} or null
     */

    public MetricOutputForProjectByLeadThreshold getStoredMetricOutput()
    {
        return hasStoredMetricOutput() ? futures.getMetricOutput() : null;
    }

    /**
     * Returns true if a prior call led to the caching of metric outputs.
     * 
     * @return true if stored results are available, false otherwise
     */

    public boolean hasStoredMetricOutput()
    {
        return Objects.nonNull(futures) && futures.hasFutureOutputs();
    }

    /**
     * Returns true if one or more metric outputs will be cached across successive calls to {@link #apply(Object)},
     * false otherwise.
     * 
     * @return true if results will be cached, false otherwise
     */

    public boolean willStoreMetricOutput()
    {
        return mergeList.length > 0;
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
     * Returns true if metrics are available for the input {@link MetricInputGroup}, false otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @return true if metrics are available for the input {@link MetricInputGroup} false otherwise
     */

    public boolean hasMetrics(MetricInputGroup inGroup)
    {
        return metrics.stream().anyMatch(a -> a.isInGroup(inGroup));

    }

    /**
     * Returns true if metrics are available for the input {@link MetricOutputGroup}, false otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if metrics are available for the input {@link MetricOutputGroup} false otherwise
     */

    public boolean hasMetrics(MetricOutputGroup outGroup)
    {
        return metrics.stream().anyMatch(a -> a.isInGroup(outGroup));
    }

    /**
     * <p>
     * Returns true if metrics are available that require {@link Threshold}, in order to define a discrete event from a
     * continuous variable, false otherwise. The metrics that require {@link Threshold} belong to one of:
     * </p>
     * <ol>
     * <li>{@link MetricInputGroup#DISCRETE_PROBABILITY}</li>
     * <li>{@link MetricInputGroup#DICHOTOMOUS}</li>
     * <li>{@link MetricInputGroup#MULTICATEGORY}</li>
     * </ol>
     * 
     * @return true if metrics are available that require {@link Threshold}, false otherwise
     */

    public boolean hasThresholdMetrics()
    {
        return hasMetrics(MetricInputGroup.DISCRETE_PROBABILITY) || hasMetrics(MetricInputGroup.DICHOTOMOUS)
            || hasMetrics(MetricInputGroup.MULTICATEGORY);
    }

    /**
     * Returns the metric data input type from the {@link ProjectConfig}.
     * 
     * @param config the {@link ProjectConfig}
     * @return the {@link MetricInputGroup} based on the {@link ProjectConfig}
     * @throws MetricConfigurationException if the input type is not recognized
     */

    static MetricInputGroup getInputType(ProjectConfig config) throws MetricConfigurationException
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
                throw new MetricConfigurationException("Unable to interpret the input type '" + type
                    + "' when attempting to process the metrics ");
        }
    }

    /**
     * Constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(MetricInput)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    MetricProcessor(final DataFactory dataFactory,
                    final ProjectConfig config,
                    final ExecutorService executor,
                    final MetricOutputGroup... mergeList) throws MetricConfigurationException
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
            singleValuedScalar = metricFactory.ofSingleValuedScalarCollection(executor,
                                                                              getSelectedMetrics(metrics,
                                                                                                 MetricInputGroup.SINGLE_VALUED,
                                                                                                 MetricOutputGroup.SCALAR));
        }
        else
        {
            singleValuedScalar = null;
        }
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR))
        {
            singleValuedVector = metricFactory.ofSingleValuedVectorCollection(executor,
                                                                              getSelectedMetrics(metrics,
                                                                                                 MetricInputGroup.SINGLE_VALUED,
                                                                                                 MetricOutputGroup.VECTOR));
        }
        else
        {
            singleValuedVector = null;
        }
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR))
        {
            singleValuedMultiVector = metricFactory.ofSingleValuedMultiVectorCollection(executor,
                                                                                        getSelectedMetrics(metrics,
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
        this.mergeList = mergeList;
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
     * Merges the input {@link MetricFutures} with any existing {@link MetricFutures} defined for this processor.
     * 
     * @param mergeFuture the futures to merge
     * @throws MetricConfigurationException if the input futures contain unsupported output types
     */

    void mergeFutures(MetricFutures mergeFutures)
    {
        Objects.requireNonNull(mergeFutures, "Specify non-null futures for merging.");
        //Merge futures if cached outputs identified
        if(willStoreMetricOutput())
        {
            MetricFutures.Builder builder = new MetricFutures.Builder();
            if(Objects.nonNull(futures))
            {
                futures = builder.addDataFactory(dataFactory)
                                 .addFutures(futures, mergeList)
                                 .addFutures(mergeFutures, mergeList)
                                 .build();
            }
            else
            {
                futures = builder.addDataFactory(dataFactory).addFutures(mergeFutures, mergeList).build();
            }
        }
    }

    /**
     * Processes a set of metric futures for {@link SingleValuedPairs}.
     * 
     * @param leadTime the lead time
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    void processSingleValuedPairs(Integer leadTime, SingleValuedPairs input, MetricFutures.Builder futures)
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
                global.forEach(threshold -> processSingleValuedThreshold(leadTime,
                                                                         threshold,
                                                                         sorted,
                                                                         input,
                                                                         singleValuedScalar,
                                                                         futures.scalar));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException(unsupportedException);
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
                global.forEach(threshold -> processSingleValuedThreshold(leadTime,
                                                                         threshold,
                                                                         sorted,
                                                                         input,
                                                                         singleValuedVector,
                                                                         futures.vector));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException(unsupportedException);
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
                global.forEach(threshold -> processSingleValuedThreshold(leadTime,
                                                                         threshold,
                                                                         sorted,
                                                                         input,
                                                                         singleValuedMultiVector,
                                                                         futures.multivector));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException(unsupportedException);
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
     * {@link MetricOutputForProjectByLeadThreshold}.
     */

    static class MetricFutures
    {

        /**
         * Scalar results.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<ScalarOutput>>> scalar =
                                                                                                                        new ConcurrentHashMap<>();
        /**
         * Vector results.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<VectorOutput>>> vector =
                                                                                                                        new ConcurrentHashMap<>();
        /**
         * Multivector results.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<MultiVectorOutput>>> multivector =
                                                                                                                                  new ConcurrentHashMap<>();

        /**
         * Instance of a {@link DataFactory}
         */

        private final DataFactory dataFactory;

        /**
         * Returns the results associated with the futures.
         * 
         * @return the metric results
         */

        MetricOutputForProjectByLeadThreshold getMetricOutput()
        {
            MetricOutputForProjectByLeadThreshold.Builder builder = dataFactory.ofMetricOutputForProjectByThreshold();
            //Add outputs for current futures
            scalar.forEach(builder::addScalarOutput);
            vector.forEach(builder::addVectorOutput);
            multivector.forEach(builder::addMultiVectorOutput);
            return builder.build();
        }

        /**
         * Returns true if one or more future outputs is available, false otherwise.
         * 
         * @return true if one or more future outputs is available, false otherwise
         */

        boolean hasFutureOutputs()
        {
            return !(scalar.isEmpty() && vector.isEmpty() && multivector.isEmpty());
        }

        /**
         * A builder for the metric futures.
         */

        static class Builder
        {
            /**
             * Scalar results.
             */

            final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<ScalarOutput>>> scalar =
                                                                                                                    new ConcurrentHashMap<>();
            /**
             * Vector results.
             */

            final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<VectorOutput>>> vector =
                                                                                                                    new ConcurrentHashMap<>();
            /**
             * Multivector results.
             */

            final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<MultiVectorOutput>>> multivector =
                                                                                                                              new ConcurrentHashMap<>();
            /**
             * Instance of a {@link DataFactory}
             */

            private DataFactory dataFactory;

            /**
             * Adds a data factory.
             * 
             * @param dataFactory the data factory
             */

            Builder addDataFactory(DataFactory dataFactory)
            {
                this.dataFactory = dataFactory;
                return this;
            }

            /**
             * Adds the outputs from an existing {@link MetricFutures} for the outputs that are included in the merge
             * list.
             * 
             * @param futures the input futures
             * @param mergeList the merge list
             * @throws MetricConfigurationException
             */

            Builder addFutures(MetricFutures futures, MetricOutputGroup[] mergeList)
            {
                if(Objects.nonNull(mergeList))
                {
                    for(MetricOutputGroup nextGroup: mergeList)
                    {
                        switch(nextGroup)
                        {
                            case SCALAR:
                                scalar.putAll(futures.scalar);
                                break;
                            case VECTOR:
                                vector.putAll(futures.vector);
                                break;
                            case MULTIVECTOR:
                                multivector.putAll(futures.multivector);
                                break;
                            default:
                                LOGGER.error("Unsupported metric group '{}'.", nextGroup);
                        }
                    }
                }
                return this;
            }

            /**
             * Build the metric futures.
             * 
             * @return the metric futures
             */

            MetricFutures build()
            {
                return new MetricFutures(this);
            }

        }

        /**
         * Hidden constructor.
         * 
         * @param builder the builder
         */

        private MetricFutures(Builder builder)
        {
            Objects.requireNonNull(builder.dataFactory,
                                   "Specify a non-null data factory from which to construct the metric futures.");
            scalar.putAll(builder.scalar);
            vector.putAll(builder.vector);
            multivector.putAll(builder.multivector);
            dataFactory = builder.dataFactory;
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
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    List<MetricConstants> getMetricsFromConfig(ProjectConfig config) throws MetricConfigurationException
    {
        Objects.requireNonNull(config, "Specify a non-null project from which to generate metrics.");
        //Obtain the list of metrics
        List<MetricConfigName> metricsConfig = config.getOutputs()
                                                     .getMetric()
                                                     .stream()
                                                     .map(MetricConfig::getName)
                                                     .collect(Collectors.toList());
        List<MetricConstants> metrics = new ArrayList<>();
        for(MetricConfigName metric: metricsConfig)
        {
            metrics.add(fromMetricConfigName(metric));
        }
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
     * Builds a metric future for a {@link MetricCollection} that consumes {@link SingleValuedPairs} at a specific lead
     * time and {@link Threshold} and appends it to the input map of futures.
     * 
     * @param leadTime the lead time
     * @param threshold the threshold
     * @param sorted a sorted set of values from which to determine {@link QuantileThreshold} where the input
     *            {@link Threshold} is a {@link ProbabilityThreshold}.
     * @param pairs the pairs
     * @param collection the metric collection
     * @param futures the collection of futures to which the new future will be added
     * @return true if the future was added successfully
     */

    private <T extends MetricOutput<?>> boolean processSingleValuedThreshold(Integer leadTime,
                                                                             Threshold threshold,
                                                                             double[] sorted,
                                                                             SingleValuedPairs pairs,
                                                                             MetricCollection<SingleValuedPairs, T> collection,
                                                                             ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<T>>> futures)
    {
        Threshold useMe = threshold;
        //Quantile required: need to determine real-value from probability
        if(threshold instanceof ProbabilityThreshold)
        {
            useMe = dataFactory.getSlicer().getQuantileFromProbability((ProbabilityThreshold)useMe, sorted);
        }
        //Slice the pairs
        SingleValuedPairs subset = dataFactory.getSlicer().sliceByLeft(pairs, useMe);
        return Objects.isNull(futures.putIfAbsent(dataFactory.getMapKey(leadTime, useMe),
                                                  CompletableFuture.supplyAsync(() -> collection.apply(subset))));
    }

    /**
     * Sets the thresholds for each metric in the configuration, including any thresholds that apply globally (to all
     * metrics).
     * 
     * @param dataFactory a data factory
     * @param config the project configuration
     * @throws MetricConfigurationException if thresholds are configured incorrectly
     */

    private void setThresholds(DataFactory dataFactory, ProjectConfig config) throws MetricConfigurationException
    {
        //Throw an exception if no thresholds are configured alongside metrics that require thresholds
        Outputs outputs = config.getOutputs();
        if(hasThresholdMetrics() && Objects.isNull(outputs.getProbabilityThresholds())
            && Objects.isNull(outputs.getValueThresholds()))
        {
            throw new MetricConfigurationException("Thresholds are required by one or more of the configured "
                + "metrics.");
        }
        //Check for metric-local thresholds and throw an exception if they are defined, as they are currently not supported
        EnumMap<MetricConstants, List<Threshold>> localThresholds = new EnumMap<>(MetricConstants.class);
        for(MetricConfig metric: outputs.getMetric())
        {
            //Obtain metric-local thresholds here
            List<Threshold> thresholds = new ArrayList<>();
            MetricConstants name = fromMetricConfigName(metric.getName());
            if(Objects.nonNull(metric.getProbabilityThresholds()) || Objects.nonNull(metric.getValueThresholds()))
            {
                throw new MetricConfigurationException("Found metric-local thresholds for '" + name
                    + "', which are not " + "currently supported.");
            }
            //TODO: implement this when metric-local threshold conditions are available in the configuration
            if(!thresholds.isEmpty())
            {
                localThresholds.put(name, thresholds);
            }
        }
        ;
        if(!localThresholds.isEmpty())
        {
            this.localThresholds.putAll(localThresholds);
        }
        //Pool together all probability thresholds and real-valued thresholds in the context of the 
        //outputs configuration. 
        List<Threshold> globalThresholds = new ArrayList<>();
        //Add a threshold for "all data" by default
        globalThresholds.add(dataFactory.getThreshold(Double.NEGATIVE_INFINITY, Operator.GREATER));
        //Add probability thresholds
        if(!Objects.isNull(outputs.getProbabilityThresholds()))
        {
            Operator oper = fromThresholdOperator(outputs.getProbabilityThresholds().getOperator());
            String values = outputs.getProbabilityThresholds().getCommaSeparatedValues();
            globalThresholds.addAll(getThresholdsFromCommaSeparatedValues(values, oper, true));
        }
        //Add real-valued thresholds
        if(!Objects.isNull(outputs.getValueThresholds()))
        {
            Operator oper = fromThresholdOperator(outputs.getValueThresholds().getOperator());
            String values = outputs.getValueThresholds().getCommaSeparatedValues();
            globalThresholds.addAll(getThresholdsFromCommaSeparatedValues(values, oper, false));
        }

        //Only set the global thresholds if no local ones are available
        if(localThresholds.isEmpty())
        {
            for(MetricInputGroup group: MetricInputGroup.values())
            {
                this.globalThresholds.put(group, globalThresholds);
            }
        }
    }

    /**
     * Returns a list of {@link Threshold} from a comma-separated string. Specify the type of {@link Threshold}
     * required.
     * 
     * @param inputString the comma-separated input string
     * @param oper the operator
     * @param areProbs is true to generate {@link ProbabilityThreshold}, false for {@link Threshold}
     * @throws MetricConfigurationException if the thresholds are configured incorrectly
     */

    private List<Threshold> getThresholdsFromCommaSeparatedValues(String inputString,
                                                                  Operator oper,
                                                                  boolean areProbs) throws MetricConfigurationException
    {
        //Parse the double values
        List<Double> addMe =
                           Arrays.stream(inputString.split(",")).map(Double::parseDouble).collect(Collectors.toList());
        List<Threshold> returnMe = new ArrayList<>();
        //Between operator
        if(oper == Operator.BETWEEN)
        {
            if(addMe.size() < 2)
            {
                throw new MetricConfigurationException("At least two values are required to compose a "
                    + "threshold that operates between a lower and an upper bound.");
            }
            for(int i = 0; i < addMe.size() - 1; i++)
            {
                if(areProbs)
                {
                    returnMe.add(dataFactory.getProbabilityThreshold(addMe.get(i), addMe.get(i + 1), oper));
                }
                else
                {
                    returnMe.add(dataFactory.getThreshold(addMe.get(i), addMe.get(i + 1), oper));
                }
            }
        }
        //Other operators
        else
        {
            if(areProbs)
            {
                addMe.forEach(threshold -> returnMe.add(dataFactory.getProbabilityThreshold(threshold, oper)));
            }
            else
            {
                addMe.forEach(threshold -> returnMe.add(dataFactory.getThreshold(threshold, oper)));
            }
        }
        return returnMe;
    }

    /**
     * Maps between metric identifiers in {@link MetricConfigName} and those in {@link MetricConstants}.
     * 
     * @param translate the input {@link MetricConfigName}
     * @return the corresponding {@link MetricConstants}.
     * @throws MetricConfigurationException if the input name is unrecognized
     */

    private static MetricConstants fromMetricConfigName(MetricConfigName translate) throws MetricConfigurationException
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
                return MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE;
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
                throw new MetricConfigurationException("Unrecognized metric identifier in project configuration '"
                    + translate + "'.");
        }
    }

    /**
     * Maps between threshold operators in {@link ThresholdOperator} and those in {@link Operator}.
     * 
     * @param translate the input {@link ThresholdOperator}
     * @return the corresponding {@link Operator}.
     * @throws MetricConfigurationException if the operator name is unrecognized
     */

    private static Operator fromThresholdOperator(ThresholdOperator translate) throws MetricConfigurationException
    {
        Objects.requireNonNull(translate,
                               "One or more metric identifiers in the project configuration could not be mapped "
                                   + "to a supported metric identifier.");
        switch(translate)
        {
            case LESS_THAN:
                return Operator.LESS;
            case GREATER_THAN:
                return Operator.GREATER;
            case LESS_THAN_OR_EQUAL_TO:
                return Operator.LESS_EQUAL;
            case GREATER_THAN_OR_EQUAL_TO:
                return Operator.GREATER_EQUAL;
            default:
                throw new MetricConfigurationException("Unrecognized threshold operator in project configuration '"
                    + translate + "'.");
        }
    }

}
