package wres.engine.statistics.metric;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;

import wres.config.generated.ProjectConfig;
import wres.datamodel.BoxPlotOutput;
import wres.datamodel.DataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricInput;
import wres.datamodel.MetricInputSliceException;
import wres.datamodel.MetricOutput;
import wres.datamodel.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.MetricOutputMapByMetric;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.Slicer;
import wres.datamodel.Threshold;
import wres.datamodel.TimeWindow;
import wres.datamodel.VectorOutput;
import wres.engine.statistics.metric.MetricProcessorByTime.MetricFuturesByTime.MetricFuturesByTimeBuilder;

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

class MetricProcessorEnsemblePairsByTime extends MetricProcessorByTime
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
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce {@link ScalarOutput}.
     */

    private final MetricCollection<EnsemblePairs, ScalarOutput> ensembleScalar;

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
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce
     * {@link BoxPlotOutput}.
     */

    final MetricCollection<EnsemblePairs, BoxPlotOutput> ensembleBoxPlot;

    /**
     * Default function that maps between ensemble pairs and single-valued pairs.
     */

    private final Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> toSingleValues;

    /**
     * Function to map between ensemble pairs and discrete probabilities.
     */

    private final BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> toDiscreteProbabilities;

    @Override
    public MetricOutputForProjectByTimeAndThreshold apply( MetricInput<?> input )
    {
        if ( ! ( input instanceof EnsemblePairs ) )
        {
            throw new MetricCalculationException( "Expected ensemble pairs for metric processing." );
        }
        TimeWindow timeWindow = input.getMetadata().getTimeWindow();
        Objects.requireNonNull( timeWindow, "Expected a non-null time window in the input metadata." );

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();
        futures.addDataFactory( dataFactory );

        //Slicer
        Slicer slicer = dataFactory.getSlicer();

        //Process the metrics that consume ensemble pairs
        if ( hasMetrics( MetricInputGroup.ENSEMBLE ) )
        {
            processEnsemblePairs( timeWindow, (EnsemblePairs) input, futures );
        }
        //Process the metrics that consume single-valued pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            //Derive the single-valued pairs from the ensemble pairs using the configured mapper
            SingleValuedPairs singleValued = slicer.transformPairs( (EnsemblePairs) input, toSingleValues );
            processSingleValuedPairs( timeWindow, singleValued, futures );
        }
        //Process the metrics that consume discrete probability pairs
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY ) )
        {
            processDiscreteProbabilityPairs( timeWindow, (EnsemblePairs) input, futures );
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
     *            {@link #apply(MetricInput)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    MetricProcessorEnsemblePairsByTime( final DataFactory dataFactory,
                                        final ProjectConfig config,
                                        final ExecutorService thresholdExecutor,
                                        final ExecutorService metricExecutor,
                                        final MetricOutputGroup... mergeList )
            throws MetricConfigurationException
    {
        super( dataFactory, config, thresholdExecutor, metricExecutor, mergeList );
        try
        {
            //Construct the metrics
            //Discrete probability input, vector output
            if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR ) )
            {
                discreteProbabilityVector =
                        metricFactory.ofDiscreteProbabilityVectorCollection( metricExecutor,
                                                                             getSelectedMetrics( metrics,
                                                                                                 MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                                 MetricOutputGroup.VECTOR ) );
            }
            else
            {
                discreteProbabilityVector = null;
            }
            //Discrete probability input, multi-vector output
            if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ) )
            {
                discreteProbabilityMultiVector =
                        metricFactory.ofDiscreteProbabilityMultiVectorCollection( metricExecutor,
                                                                                  getSelectedMetrics( metrics,
                                                                                                      MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                                      MetricOutputGroup.MULTIVECTOR ) );
            }
            else
            {
                discreteProbabilityMultiVector = null;
            }
            //Ensemble input, vector output
            if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR ) )
            {
                ensembleVector = metricFactory.ofEnsembleVectorCollection( metricExecutor,
                                                                           getSelectedMetrics( metrics,
                                                                                               MetricInputGroup.ENSEMBLE,
                                                                                               MetricOutputGroup.VECTOR ) );
            }
            else
            {
                ensembleVector = null;
            }
            //Ensemble input, scalar output
            if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCALAR ) )
            {
                ensembleScalar = metricFactory.ofEnsembleScalarCollection( metricExecutor,
                                                                           getSelectedMetrics( metrics,
                                                                                               MetricInputGroup.ENSEMBLE,
                                                                                               MetricOutputGroup.SCALAR ) );
            }
            else
            {
                ensembleScalar = null;
            }
            //Ensemble input, multi-vector output
            if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
            {
                ensembleMultiVector = metricFactory.ofEnsembleMultiVectorCollection( metricExecutor,
                                                                                     getSelectedMetrics( metrics,
                                                                                                         MetricInputGroup.ENSEMBLE,
                                                                                                         MetricOutputGroup.MULTIVECTOR ) );
            }
            else
            {
                ensembleMultiVector = null;
            }
            //Ensemble input, box-plot output
            if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.BOXPLOT ) )
            {
                ensembleBoxPlot = metricFactory.ofEnsembleBoxPlotCollection( metricExecutor,
                                                                             getSelectedMetrics( metrics,
                                                                                                 MetricInputGroup.ENSEMBLE,
                                                                                                 MetricOutputGroup.BOXPLOT ) );
            }
            else
            {
                ensembleBoxPlot = null;
            }
        }
        catch ( MetricParameterException e )
        {
            throw new MetricConfigurationException( "Failed to construct one or more metrics.", e );
        }

        //Construct the default mapper from ensembles to single-values: this is not currently configurable
        toSingleValues = in -> dataFactory.pairOf( in.getItemOne(),
                                                   Arrays.stream( in.getItemTwo() ).average().getAsDouble() );

        //Construct the default mapper from ensembles to probabilities: this is not currently configurable
        toDiscreteProbabilities = dataFactory.getSlicer()::transformPair;
    }

    @Override
    void validate( ProjectConfig config ) throws MetricConfigurationException
    {
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            throw new MetricConfigurationException( "Cannot configure dichotomous metrics for ensemble inputs: correct the configuration '"
                                                    + config.getLabel() + "'." );
        }
        if ( hasMetrics( MetricInputGroup.MULTICATEGORY ) )
        {
            throw new MetricConfigurationException( "Cannot configure multicategory metrics for ensemble inputs: correct the configuration '"
                                                    + config.getLabel() + "'." );
        }
        //Ensemble input, vector output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR )
             && metrics.contains( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
             && Objects.isNull( config.getInputs().getBaseline() ) )
        {
            throw new MetricConfigurationException( "Specify a non-null baseline from which to generate the '"
                                                    + MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE
                                                    + "'." );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link EnsemblePairs}, which are mapped from the input pairs,
     * {@link EnsemblePairs}, using a configured mapping function.
     * 
     * TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairs( TimeWindow timeWindow, EnsemblePairs input, MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCALAR ) )
        {
            processEnsembleThresholds( timeWindow, input, futures, MetricOutputGroup.SCALAR );
        }
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.VECTOR ) )
        {
            processEnsembleThresholds( timeWindow, input, futures, MetricOutputGroup.VECTOR );
        }
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
        {
            processEnsembleThresholds( timeWindow, input, futures, MetricOutputGroup.MULTIVECTOR );
        }
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.BOXPLOT ) )
        {
            processEnsembleThresholds( timeWindow, input, futures, MetricOutputGroup.BOXPLOT );
        }
    }

    /**
     * Processes all thresholds for metrics that consume {@link EnsemblePairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsembleThresholds( TimeWindow timeWindow,
                                            EnsemblePairs input,
                                            MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                            MetricOutputGroup outGroup )
    {
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Deal with global thresholds
        if ( hasGlobalThresholds( MetricInputGroup.ENSEMBLE ) )
        {
            List<Threshold> global = globalThresholds.get( MetricInputGroup.ENSEMBLE );
            double[] sorted = getSortedClimatology( input, global );
            Map<Threshold, MetricCalculationException> failures = new HashMap<>();
            global.forEach( threshold -> {
                Threshold useMe = getThreshold( threshold, sorted );
                MetricCalculationException result =
                        processEnsembleThreshold( timeWindow, input, futures, outGroup, useMe );
                if ( !Objects.isNull( result ) )
                {
                    failures.put( useMe, result );
                }
            } );
            //Handle any failures
            handleThresholdFailures( failures, global.size(), input.getMetadata(), MetricInputGroup.ENSEMBLE );
        }
        //Deal with metric-local thresholds
        else
        {
            //Hook for future logic
            throw new MetricCalculationException( unsupportedException );
        }
    }

    /**
     * Processes one threshold for metrics that consume {@link EnsemblePairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param threshold the threshold
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private MetricCalculationException processEnsembleThreshold( TimeWindow timeWindow,
                                                                 EnsemblePairs input,
                                                                 MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                                 MetricOutputGroup outGroup,
                                                                 Threshold threshold )
    {
        MetricCalculationException returnMe = null;
        try
        {
            if ( outGroup == MetricOutputGroup.SCALAR )
            {
                futures.addScalarOutput( dataFactory.getMapKey( timeWindow, threshold ),
                                         processEnsembleThreshold( threshold,
                                                                   input,
                                                                   ensembleScalar ) );
            }
            else if ( outGroup == MetricOutputGroup.VECTOR )
            {
                futures.addVectorOutput( dataFactory.getMapKey( timeWindow, threshold ),
                                         processEnsembleThreshold( threshold,
                                                                   input,
                                                                   ensembleVector ) );
            }
            else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
            {
                futures.addMultiVectorOutput( dataFactory.getMapKey( timeWindow, threshold ),
                                              processEnsembleThreshold( threshold,
                                                                        input,
                                                                        ensembleMultiVector ) );
            }
            //Only process box plots for "all data" threshold
            else if ( outGroup == MetricOutputGroup.BOXPLOT && !threshold.isFinite() )
            {
                futures.addBoxPlotOutput( dataFactory.getMapKey( timeWindow, threshold ),
                                          processEnsembleThreshold( threshold,
                                                                    input,
                                                                    ensembleBoxPlot ) );
            }

        }
        //Insufficient data for one threshold: log, but allow
        catch ( MetricInputSliceException e )
        {
            returnMe = new MetricCalculationException( e.getMessage(), e );
        }
        return returnMe;
    }

    /**
     * Processes a set of metric futures that consume {@link DiscreteProbabilityPairs}, which are mapped from the input
     * pairs, {@link EnsemblePairs}, using a configured mapping function. Skips any thresholds for which
     * {@link Double#isFinite(double)} returns <code>false</code> on the threshold value(s).
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityPairs( TimeWindow timeWindow,
                                                  EnsemblePairs input,
                                                  MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR ) )
        {
            processDiscreteProbabilityThresholds( timeWindow, input, futures, MetricOutputGroup.VECTOR );
        }
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ) )
        {
            processDiscreteProbabilityThresholds( timeWindow, input, futures, MetricOutputGroup.MULTIVECTOR );
        }
    }

    /**
     * Processes all thresholds for metrics that consume {@link DiscreteProbabilityPairs} for a given 
     * {@link MetricOutputGroup}. The {@link DiscreteProbabilityPairs} are produced from the input 
     * {@link EnsemblePairs} using a configured transformation. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityThresholds( TimeWindow timeWindow,
                                                       EnsemblePairs input,
                                                       MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                       MetricOutputGroup outGroup )
    {
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Deal with global thresholds
        if ( hasGlobalThresholds( MetricInputGroup.DISCRETE_PROBABILITY ) )
        {
            List<Threshold> global = globalThresholds.get( MetricInputGroup.DISCRETE_PROBABILITY );
            double[] sorted = getSortedClimatology( input, global );
            Map<Threshold, MetricCalculationException> failures = new HashMap<>();
            global.forEach( threshold -> {
                //Only process discrete thresholds
                if ( threshold.isFinite() )
                {
                    Threshold useMe = getThreshold( threshold, sorted );
                    MetricCalculationException result =
                            processDiscreteProbabilityThreshold( timeWindow, input, futures, outGroup, useMe );
                    if ( !Objects.isNull( result ) )
                    {
                        failures.put( useMe, result );
                    }
                }
            } );
            //Handle any failures
            handleThresholdFailures( failures,
                                     global.size(),
                                     input.getMetadata(),
                                     MetricInputGroup.DISCRETE_PROBABILITY );
        }
        //Deal with metric-local thresholds
        else
        {
            //Hook for future logic
            throw new MetricCalculationException( unsupportedException );
        }
    }

    /**
     * Processes one threshold for metrics that consume {@link DiscreteProbabilityPairs} for a given 
     * {@link MetricOutputGroup}. The {@link DiscreteProbabilityPairs} are produced from the input 
     * {@link EnsemblePairs} using a configured transformation. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param threshold the threshold
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private MetricCalculationException processDiscreteProbabilityThreshold( TimeWindow timeWindow,
                                                                            EnsemblePairs input,
                                                                            MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                                            MetricOutputGroup outGroup,
                                                                            Threshold threshold )
    {
        MetricCalculationException returnMe = null;
        try
        {
            if ( outGroup == MetricOutputGroup.VECTOR )
            {
                futures.addVectorOutput( dataFactory.getMapKey( timeWindow, threshold ),
                                         processDiscreteProbabilityThreshold( threshold,
                                                                              input,
                                                                              discreteProbabilityVector ) );
            }
            else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
            {
                futures.addMultiVectorOutput( dataFactory.getMapKey( timeWindow, threshold ),
                                              processDiscreteProbabilityThreshold( threshold,
                                                                                   input,
                                                                                   discreteProbabilityMultiVector ) );
            }
        }
        //Insufficient data for one threshold: log, but allow
        catch ( MetricInputSliceException e )
        {
            returnMe = new MetricCalculationException( e.getMessage(), e );
        }

        return returnMe;
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link DiscreteProbabilityPairs} at a 
     * specific {@link Threshold}, following transformation from the input {@link EnsemblePairs}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     * @throws MetricInputSliceException if the pairs contain insufficient data to compute the metrics
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processDiscreteProbabilityThreshold( Threshold threshold,
                                                 EnsemblePairs pairs,
                                                 MetricCollection<DiscreteProbabilityPairs, T> collection )
                    throws MetricInputSliceException
    {
        //Check the slice before transformation
        checkSlice( pairs, threshold );
        DiscreteProbabilityPairs transformed = dataFactory.getSlicer()
                                                          .transformPairs( pairs, threshold, toDiscreteProbabilities );
        //Check the slice after transformation
        checkDiscreteProbabilitySlice( transformed, threshold );
        return CompletableFuture.supplyAsync( () -> collection.apply( transformed ), thresholdExecutor );
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link EnsemblePairs} at a specific
     * {@link Threshold} and appends it to the input map of futures.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     * @throws MetricInputSliceException if the threshold fails to slice sufficient data to compute the metrics
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processEnsembleThreshold( Threshold threshold,
                                      EnsemblePairs pairs,
                                      MetricCollection<EnsemblePairs, T> collection )
                    throws MetricInputSliceException
    {
        //Slice the pairs
        EnsemblePairs subset = dataFactory.getSlicer().sliceByLeft( pairs, threshold );
        checkSlice( subset, threshold );
        return CompletableFuture.supplyAsync( () -> collection.apply( subset ), thresholdExecutor );
    }

    /**
     * Validates the {@link DiscreteProbabilityPairs} and throws an exception if the smaller of the number of 
     * occurrences ({@link PairOfDoubles#getItemOne()} = 0) or non-occurrences ({@link PairOfDoubles#getItemOne()} = 1) 
     * is less than the {@link minimumSampleSize}.
     * 
     * @param subset the data to validate
     * @param threshold the threshold used to localize the error message
     * @throws MetricInputSliceException if the input contains insufficient data for metric calculation 
     */

    private void checkDiscreteProbabilitySlice( DiscreteProbabilityPairs subset, Threshold threshold )
            throws MetricInputSliceException
    {
        long nonOccurrences = subset.getData().stream().filter( a -> Double.compare( a.getItemOne(), 0 ) == 0 ).count();
        double min = Math.min( nonOccurrences, subset.size() - nonOccurrences );
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
