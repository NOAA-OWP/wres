package wres.engine.statistics.metric.processing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigurationException;
import wres.engine.statistics.metric.processing.MetricProcessorByTime.MetricFuturesByTime.MetricFuturesByTimeBuilder;

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

public class MetricProcessorByTimeSingleValuedPairs extends MetricProcessorByTime<SingleValuedPairs>
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     */

    private final MetricCollection<DichotomousPairs, MatrixOutput, ScalarOutput> dichotomousScalar;

    @Override
    public MetricOutputForProjectByTimeAndThreshold apply( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, "Expected non-null input to the metric processor." );
        TimeWindow timeWindow = input.getMetadata().getTimeWindow();
        Objects.requireNonNull( timeWindow, "Expected a non-null time window in the input metadata." );

        //Slicer
        Slicer slicer = dataFactory.getSlicer();

        //Remove missing values. 
        //TODO: when time-series metrics are supported, leave missings in place for time-series
        SingleValuedPairs inputNoMissing = input;
        try
        {
            inputNoMissing = slicer.filter( input, ADMISSABLE_DATA, true );
        }
        catch ( MetricInputSliceException e )
        {
            throw new MetricCalculationException( "While attempting to remove missing values: ", e );
        }

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();
        futures.addDataFactory( dataFactory );

        //Process the metrics that consume single-valued pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            processSingleValuedPairs( timeWindow, inputNoMissing, futures );
        }
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            processDichotomousPairs( timeWindow, inputNoMissing, futures );
        }

        // Log
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Completed processing of metrics for feature '{}' at time window '{}'.",
                          input.getMetadata().getIdentifier().getGeospatialID(),
                          input.getMetadata().getTimeWindow() );
        }

        //Process and return the result       
        MetricFuturesByTime futureResults = futures.build();
        //Add for merge with existing futures, if required
        addToMergeMap( timeWindow, futureResults );
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
     *            {@link #apply(SingleValuedPairs)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public MetricProcessorByTimeSingleValuedPairs( final DataFactory dataFactory,
                                                   final ProjectConfig config,
                                                   final ExecutorService thresholdExecutor,
                                                   final ExecutorService metricExecutor,
                                                   final MetricOutputGroup... mergeList )
            throws MetricConfigurationException, MetricParameterException
    {
        super( dataFactory, config, thresholdExecutor, metricExecutor, mergeList );
        //Construct the metrics
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

    @Override
    void validate( ProjectConfig config ) throws MetricConfigurationException
    {
        //Check the metrics individually, as some may belong to multiple groups
        for ( MetricConstants next : metrics )
        {
            if ( ! ( next.isInGroup( MetricInputGroup.SINGLE_VALUED )
                     || next.isInGroup( MetricInputGroup.DICHOTOMOUS ) ) )
            {
                throw new MetricConfigurationException( "Cannot configure '" + next
                                                        + "' for "
                                                        + MetricInputGroup.SINGLE_VALUED
                                                        + ": correct the configuration '"
                                                        + config.getLabel()
                                                        + "'." );
            }
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function. Skips any thresholds for which
     * {@link Double#isFinite(double)} returns <code>false</code> on the threshold value(s).
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     * @throws InsufficientDataException if there is insufficient data to compute any metrics
     */

    private void processDichotomousPairs( TimeWindow timeWindow,
                                          SingleValuedPairs input,
                                          MetricFuturesByTimeBuilder futures )
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
                                processDichotomousThreshold( timeWindow, input, futures, useMe );
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
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param threshold the threshold
     * @return a MetricCalculationException for information if the threshold cannot be computed
     */

    private MetricCalculationException processDichotomousThreshold( TimeWindow timeWindow,
                                                                    SingleValuedPairs input,
                                                                    MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                                    Threshold threshold )
    {
        MetricCalculationException returnMe = null;
        try
        {
            futures.addScalarOutput( Pair.of( timeWindow, threshold ),
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
                                         MetricCollection<DichotomousPairs, MatrixOutput, T> collection )
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
     * occurrences or non-occurrences is less than the {@link #minimumSampleSize}.
     * 
     * @param subset the data to validate
     * @param threshold the threshold used to localize the error message
     * @throws MetricInputSliceException if the input contains insufficient data for metric calculation 
     */

    private void checkDichotomousSlice( DichotomousPairs subset, Threshold threshold )
            throws MetricInputSliceException
    {
        long occurrences = subset.getData().stream().filter( a -> a.getBooleans()[0] ).count();
        double min = Math.min( occurrences, subset.getData().size() - occurrences );
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
