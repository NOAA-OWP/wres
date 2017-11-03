package wres.engine.statistics.metric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricInput;
import wres.datamodel.MetricInputSliceException;
import wres.datamodel.MetricOutput;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByMetric;
import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.Threshold;
import wres.engine.statistics.metric.MetricProcessorByLeadTime.MetricFuturesByLeadTime.MetricFuturesByLeadTimeBuilder;

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

class MetricProcessorSingleValuedPairsByLeadTime extends MetricProcessorByLeadTime
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     */

    private final MetricCollection<DichotomousPairs, ScalarOutput> dichotomousScalar;

    @Override
    public MetricOutputForProjectByLeadThreshold apply( MetricInput<?> input )
    {
        if ( ! ( input instanceof SingleValuedPairs ) )
        {
            throw new MetricCalculationException( "Expected single-valued pairs for metric processing." );
        }
        Integer leadTime = input.getMetadata().getLeadTimeInHours();
        Objects.requireNonNull( leadTime, "Expected a non-null forecast lead time in the input metadata." );

        //Metric futures 
        MetricFuturesByLeadTimeBuilder futures = new MetricFuturesByLeadTimeBuilder();
        futures.addDataFactory( dataFactory );

        //Process the metrics that consume single-valued pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            processSingleValuedPairs( leadTime, (SingleValuedPairs) input, futures );
        }
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            processDichotomousPairs( leadTime, (SingleValuedPairs) input, futures );
        }

        // Log
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Completed processing of metrics for feature '{}' at lead time {}.",
                          input.getMetadata().getIdentifier().getGeospatialID(),
                          input.getMetadata().getLeadTimeInHours() );
        }

        //Process and return the result       
        MetricFuturesByLeadTime futureResults = futures.build();
        //Add for merge with existing futures, if required
        addToMergeMap( leadTime, futureResults );
        return futureResults.getMetricOutput();
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(MetricInput)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    MetricProcessorSingleValuedPairsByLeadTime( final DataFactory dataFactory,
                                                final ProjectConfig config,
                                                final ExecutorService thresholdExecutor,
                                                final ExecutorService metricExecutor,
                                                final MetricOutputGroup... mergeList )
            throws MetricConfigurationException
    {
        super( dataFactory, config, thresholdExecutor, metricExecutor, mergeList );
        //Construct the metrics
        try
        {
            if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR ) )
            {
                dichotomousScalar =
                        metricFactory.ofDichotomousScalarCollection( metricExecutor,
                                                                     getSelectedMetrics( metrics,
                                                                                         MetricInputGroup.DICHOTOMOUS,
                                                                                         MetricOutputGroup.SCALAR ) );
            }
            else
            {
                dichotomousScalar = null;
            }
        }
        catch ( MetricParameterException e )
        {
            throw new MetricConfigurationException( "Failed to construct one or more metrics.", e );
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

    private void processDichotomousPairs( Integer leadTime,
                                          SingleValuedPairs input,
                                          MetricFuturesByLeadTimeBuilder futures )
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR ) )
        {
            //Deal with global thresholds
            if ( hasGlobalThresholds( MetricInputGroup.DICHOTOMOUS ) )
            {
                List<Threshold> global = globalThresholds.get( MetricInputGroup.DICHOTOMOUS );
                double[] sorted = getSortedClimatology( input, global );
                Map<Threshold, MetricCalculationException> failures = new HashMap<>();
                global.forEach( threshold -> {
                    //Only process discrete thresholds
                    if ( threshold.isFinite() )
                    {
                        Threshold useMe = getThreshold( threshold, sorted );
                        MetricCalculationException result =
                                processDichotomousThreshold( leadTime, input, futures, useMe );
                        if ( !Objects.isNull( result ) )
                        {
                            failures.put( useMe, result );
                        }
                    }
                } );
                //Handle any failures
                handleThresholdFailures( failures, global.size(), input.getMetadata(), MetricInputGroup.DICHOTOMOUS );
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new MetricCalculationException( unsupportedException );
            }
        }
    }

    /**
     * Processes one threshold for metrics that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function, and produce a {@link MetricOutputGroup#SCALAR}. 
     * 
     * @param leadTime the lead time
     * @param input the input pairs
     * @param futures the metric futures
     * @param threshold the threshold
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private MetricCalculationException processDichotomousThreshold( Integer leadTime,
                                                                    SingleValuedPairs input,
                                                                    MetricFuturesByLeadTime.MetricFuturesByLeadTimeBuilder futures,
                                                                    Threshold threshold )
    {
        MetricCalculationException returnMe = null;
        try
        {
            futures.addScalarOutput( dataFactory.getMapKey( leadTime, threshold ),
                                     processDichotomousThreshold( threshold,
                                                                  input,
                                                                  dichotomousScalar ) );
        }
        //Insufficient data for one threshold: log, but allow
        catch ( MetricInputSliceException e )
        {
            returnMe = new MetricCalculationException( e.getMessage(), e );
        }
        return returnMe;
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link DichotomousPairs} at a specific lead
     * time and {@link Threshold}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @return true if the future was added successfully
     * @throws MetricInputSliceException if the pairs contain insufficient data to compute the metrics
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processDichotomousThreshold( Threshold threshold,
                                         SingleValuedPairs pairs,
                                         MetricCollection<DichotomousPairs, T> collection )
                    throws MetricInputSliceException
    {
        //Check the data before transformation
        checkSlice( pairs, threshold );
        //Define a mapper to convert the single-valued pairs to dichotomous pairs
        Function<PairOfDoubles, PairOfBooleans> mapper =
                pair -> dataFactory.pairOf( threshold.test( pair.getItemOne() ),
                                            threshold.test( pair.getItemTwo() ) );
        //Slice the pairs
        DichotomousPairs transformed = dataFactory.getSlicer().transformPairs( pairs, mapper );
        //Check the data after transformation
        checkDichotomousSlice( transformed, threshold );
        return CompletableFuture.supplyAsync( () -> collection.apply( transformed ), thresholdExecutor );
    }

    /**
     * Validates the {@link DichotomousPairs} and throws an exception if the smaller of the number of 
     * occurrences ({@link VectorOfBooleans#} = 0) or non-occurrences ({@link PairOfDoubles#getItemOne()} = 1) 
     * is less than the {@link minimumSampleSize}.
     * 
     * @param subset the data to validate
     * @param threshold the threshold used to localize the error message
     * @throws MetricInputSliceException if the input contains insufficient data for metric calculation 
     */

    private void checkDichotomousSlice( DichotomousPairs subset, Threshold threshold )
            throws MetricInputSliceException
    {
        long occurrences = subset.getData().stream().filter( a -> a.getBooleans()[0] ).count();
        double min = Math.min( occurrences, subset.size() - occurrences );
        if ( min < minimumSampleSize )
        {
            throw new MetricInputSliceException( "Failed to compute one or more metrics for threshold '"
                                                 + threshold
                                                 + "', as the (smaller of the) number of observed occurrences and "
                                                 + "non-occurrences was less than the prescribed minimum of '"
                                                 + minimumSampleSize
                                                 + "'." );
        }
    }

}
