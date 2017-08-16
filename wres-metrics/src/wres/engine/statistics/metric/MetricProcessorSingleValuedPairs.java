package wres.engine.statistics.metric;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MetricConstants.MetricInputGroup;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.metric.MetricOutputMapByMetric;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Threshold;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * {@link SingleValuedPairs} and configured transformations of {@link SingleValuedPairs}. For example, metrics that
 * consume {@link DichotomousPairs} may be processed after transforming the {@link SingleValuedPairs} with an
 * appropriate mapping function.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

final class MetricProcessorSingleValuedPairs extends MetricProcessor
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     */

    private final MetricCollection<DichotomousPairs, ScalarOutput> dichotomousScalar;

    @Override
    public MetricOutputForProjectByLeadThreshold apply(MetricInput<?> input)
    {
        if(!(input instanceof SingleValuedPairs))
        {
            throw new MetricCalculationException("Expected single-valued pairs for metric processing.");
        }
        Integer leadTime = input.getMetadata().getLeadTime();
        Objects.requireNonNull(leadTime, "Expected a non-null forecast lead time in the input metadata.");

        //Metric futures 
        MetricFutures.Builder futures = new MetricFutures.Builder().addDataFactory(dataFactory);

        //Process the metrics that consume single-valued pairs
        if(hasMetrics(MetricInputGroup.SINGLE_VALUED))
        {
            processSingleValuedPairs(leadTime, (SingleValuedPairs)input, futures);
        }
        if(hasMetrics(MetricInputGroup.DICHOTOMOUS))
        {
            processDichotomousPairs(leadTime, (SingleValuedPairs)input, futures);
        }

        // Log
        if(LOGGER.isInfoEnabled())
        {
            LOGGER.info("Completed processing of metrics for feature '{}' at lead time {}.",
                        input.getMetadata().getIdentifier().getGeospatialID(),
                        input.getMetadata().getLeadTime());
        }

        //Process and return the result       
        MetricFutures futureResults = futures.build();
        //Add for merge with existing futures, if required
        addToMergeMap(leadTime, futureResults);
        return futureResults.getMetricOutput();
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(MetricInput)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    MetricProcessorSingleValuedPairs(final DataFactory dataFactory,
                                     final ProjectConfig config,
                                     final ExecutorService executor,
                                     final MetricOutputGroup... mergeList) throws MetricConfigurationException
    {
        super(dataFactory, config, executor, mergeList);
        //Construct the metrics
        if(hasMetrics(MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR))
        {
            dichotomousScalar =
                              metricFactory.ofDichotomousScalarCollection(executor,
                                                                          getSelectedMetrics(metrics,
                                                                                             MetricInputGroup.DICHOTOMOUS,
                                                                                             MetricOutputGroup.SCALAR));
        }
        else
        {
            dichotomousScalar = null;
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function. Skips any thresholds for which
     * {@link Double#isFinite(double)} returns <code>false</code> on the threshold value(s).
     * 
     * @param leadTime the lead time
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairs(Integer leadTime, SingleValuedPairs input, MetricFutures.Builder futures)
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricInputGroup.DICHOTOMOUS))
            {
                List<Threshold> global = globalThresholds.get(MetricInputGroup.DICHOTOMOUS);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> {
                    //Only process discrete thresholds
                    if(threshold.isFinite())
                    {
                        Threshold useMe = getThreshold(threshold, sorted);
                        futures.addScalarOutput(dataFactory.getMapKey(leadTime, useMe),
                                                processDichotomousThreshold(leadTime, useMe, input, dichotomousScalar));
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
     * Builds a metric future for a {@link MetricCollection} that consumes {@link DichotomousPairs} at a specific lead
     * time and {@link Threshold}.
     * 
     * @param leadTime the lead time
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @return true if the future was added successfully
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>> processDichotomousThreshold(Integer leadTime,
                                                                                                       Threshold threshold,
                                                                                                       SingleValuedPairs pairs,
                                                                                                       MetricCollection<DichotomousPairs, T> collection)
    {
        //Define a mapper to convert the single-valued pairs to dichotomous pairs
        Function<PairOfDoubles, PairOfBooleans> mapper = pair -> dataFactory.pairOf(threshold.test(pair.getItemOne()),
                                                                                    threshold.test(pair.getItemTwo()));
        //Slice the pairs
        DichotomousPairs transformed = dataFactory.getSlicer().transformPairs(pairs, mapper);
        return CompletableFuture.supplyAsync(() -> collection.apply(transformed));
    }

}
