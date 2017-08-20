package wres.engine.statistics.metric;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MapBiKey;
import wres.datamodel.MetricInput;
import wres.datamodel.MetricInputSliceException;
import wres.datamodel.MetricOutput;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByMetric;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.Threshold;
import wres.datamodel.VectorOutput;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricOutputForProjectByLeadThreshold.MetricOutputForProjectByLeadThresholdBuilder;

/**
 * A {@link MetricProcessor} that processes and stores metric by forecast lead time.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class MetricProcessorByLeadTime extends MetricProcessor<MetricOutputForProjectByLeadThreshold>
{

    /**
     * The metric futures from previous calls, indexed by lead time.
     */

    ConcurrentMap<Integer, MetricFuturesByLeadTime> futures = new ConcurrentSkipListMap<>();

    /**
     * Returns a {@link MetricOutputForProjectByLeadThreshold} for the last available results or null if
     * {@link #hasStoredMetricOutput()} returns false.
     * 
     * @return a {@link MetricOutputForProjectByLeadThreshold} or null
     */

    @Override
    public MetricOutputForProjectByLeadThreshold getStoredMetricOutput()
    {
        MetricOutputForProjectByLeadThreshold returnMe = null;
        if(hasStoredMetricOutput())
        {
            MetricFuturesByLeadTime.MetricFuturesByLeadTimeBuilder builder =
                                                                           new MetricFuturesByLeadTime.MetricFuturesByLeadTimeBuilder();
            builder.addDataFactory(dataFactory);
            for(MetricFuturesByLeadTime future: futures.values())
            {
                builder.addFutures(future, mergeList);
            }
            returnMe = builder.build().getMetricOutput();
        }
        return returnMe;
    }

    /**
     * Returns true if a prior call led to the caching of metric outputs.
     * 
     * @return true if stored results are available, false otherwise
     */
    @Override
    public boolean hasStoredMetricOutput()
    {
        return futures.values().stream().anyMatch(MetricFuturesByLeadTime::hasFutureOutputs);
    }

    /**
     * Adds the input {@link MetricFutures} to the internal store of existing {@link MetricFutures} defined for this
     * processor.
     * 
     * @param leadTime the lead time
     * @param mergeFuture the futures to add
     */

    void addToMergeMap(Integer leadTime, MetricFuturesByLeadTime mergeFutures)
    {
        Objects.requireNonNull(mergeFutures, "Specify non-null futures for merging.");
        //Merge futures if cached outputs identified
        if(willStoreMetricOutput())
        {
            futures.put(leadTime, mergeFutures);
        }
    }

    /**
     * Store of metric futures for each output type. Use {@ link #getMetricOutput()} to obtain the processed
     * {@link MetricOutputForProjectByLeadThreshold}.
     */

    static class MetricFuturesByLeadTime
    {

        /**
         * Instance of a {@link DataFactory}
         */

        private DataFactory dataFactory;

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
         * Returns the results associated with the futures.
         * 
         * @return the metric results
         */

        MetricOutputForProjectByLeadThreshold getMetricOutput()
        {
            MetricOutputForProjectByLeadThresholdBuilder builder =
                                                                 dataFactory.ofMetricOutputForProjectByLeadThreshold();
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

        static class MetricFuturesByLeadTimeBuilder
        {

            /**
             * Instance of a {@link DataFactory}
             */

            private DataFactory dataFactory;

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
             * Adds a data factory.
             * 
             * @param dataFactory the data factory
             */

            MetricFuturesByLeadTimeBuilder addDataFactory(DataFactory dataFactory)
            {
                this.dataFactory = dataFactory;
                return this;
            }

            /**
             * Adds a set of future {@link ScalarOutput} to the appropriate internal store.
             * 
             * @param key the key
             * @param value the future result
             * @return the builder
             */

            MetricFuturesByLeadTimeBuilder addScalarOutput(MapBiKey<Integer, Threshold> key,
                                                           Future<MetricOutputMapByMetric<ScalarOutput>> value)
            {
                scalar.put(key, value);
                return this;
            }

            /**
             * Adds a set of future {@link VectorOutput} to the appropriate internal store.
             * 
             * @param key the key
             * @param value the future result
             * @return the builder
             */

            MetricFuturesByLeadTimeBuilder addVectorOutput(MapBiKey<Integer, Threshold> key,
                                                           Future<MetricOutputMapByMetric<VectorOutput>> value)
            {
                vector.put(key, value);
                return this;
            }

            /**
             * Adds a set of future {@link MultiVectorOutput} to the appropriate internal store.
             * 
             * @param key the key
             * @param value the future result
             * @return the builder
             */

            MetricFuturesByLeadTimeBuilder addMultiVectorOutput(MapBiKey<Integer, Threshold> key,
                                                                Future<MetricOutputMapByMetric<MultiVectorOutput>> value)
            {
                multivector.put(key, value);
                return this;
            }

            /**
             * Build the metric futures.
             * 
             * @return the metric futures
             */

            MetricFuturesByLeadTime build()
            {
                return new MetricFuturesByLeadTime(this);
            }

            /**
             * Adds the outputs from an existing {@link MetricFutures} for the outputs that are included in the merge
             * list.
             * 
             * @param futures the input futures
             * @param mergeList the merge list
             * @throws MetricConfigurationException
             */

            private MetricFuturesByLeadTimeBuilder addFutures(MetricFuturesByLeadTime futures,
                                                              MetricOutputGroup[] mergeList)
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

        }

        /**
         * Hidden constructor.
         * 
         * @param builder the builder
         */

        private MetricFuturesByLeadTime(MetricFuturesByLeadTimeBuilder builder)
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
     * Processes a set of metric futures for {@link SingleValuedPairs}.
     * 
     * @param leadTime the lead time
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    void processSingleValuedPairs(Integer leadTime,
                                  SingleValuedPairs input,
                                  MetricFuturesByLeadTime.MetricFuturesByLeadTimeBuilder futures)
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
                double[] sorted = getSortedClimatology(input, global);
                global.forEach(threshold -> {
                    Threshold useMe = getThreshold(threshold, sorted);
                    try
                    {
                        futures.addScalarOutput(dataFactory.getMapKey(leadTime, useMe),
                                                processSingleValuedThreshold(useMe, input, singleValuedScalar));
                    }
                    catch(MetricInputSliceException e)
                    {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
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
                double[] sorted = getSortedClimatology(input, global);
                global.forEach(threshold -> {
                    Threshold useMe = getThreshold(threshold, sorted);
                    try
                    {
                        futures.addVectorOutput(dataFactory.getMapKey(leadTime, useMe),
                                                processSingleValuedThreshold(useMe, input, singleValuedVector));
                    }
                    catch(MetricInputSliceException e)
                    {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
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
                double[] sorted = getSortedClimatology(input, global);
                global.forEach(threshold -> {
                    Threshold useMe = getThreshold(threshold, sorted);
                    try
                    {
                        futures.addMultiVectorOutput(dataFactory.getMapKey(leadTime, useMe),
                                                     processSingleValuedThreshold(useMe,
                                                                                  input,
                                                                                  singleValuedMultiVector));
                    }
                    catch(MetricInputSliceException e)
                    {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
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
     * Constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(MetricInput)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    MetricProcessorByLeadTime(DataFactory dataFactory,
                              ProjectConfig config,
                              ExecutorService executor,
                              MetricOutputGroup[] mergeList) throws MetricConfigurationException
    {
        super(dataFactory, config, executor, mergeList);
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link SingleValuedPairs} at a specific lead
     * time and {@link Threshold}.
     * 
     * @param threshold the threshold
     * @param pairs the pairs
     * @param futures the collection of futures to which the new future will be added
     * @return the future result
     * @throws MetricInputSliceException if the threshold fails to slice any data
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>> processSingleValuedThreshold(Threshold threshold,
                                                                                                        SingleValuedPairs pairs,
                                                                                                        MetricCollection<SingleValuedPairs, T> collection) throws MetricInputSliceException
    {
        //Slice the pairs
        SingleValuedPairs subset = dataFactory.getSlicer().sliceByLeft(pairs, threshold);
        return CompletableFuture.supplyAsync(() -> collection.apply(subset));
    }

}
