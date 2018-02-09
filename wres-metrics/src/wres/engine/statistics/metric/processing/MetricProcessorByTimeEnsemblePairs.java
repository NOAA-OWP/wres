package wres.engine.statistics.metric.processing;

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
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScoreOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigurationException;
import wres.engine.statistics.metric.processing.MetricProcessorByTime.MetricFuturesByTime.MetricFuturesByTimeBuilder;

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

public class MetricProcessorByTimeEnsemblePairs extends MetricProcessorByTime<EnsemblePairs>
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DiscreteProbabilityPairs} and produce
     * {@link DoubleScoreOutput}.
     */

    private final MetricCollection<DiscreteProbabilityPairs, DoubleScoreOutput, DoubleScoreOutput> discreteProbabilityScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScoreOutput}.
     */

    private final MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput, MultiVectorOutput> discreteProbabilityMultiVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce {@link DoubleScoreOutput}.
     */

    private final MetricCollection<EnsemblePairs, DoubleScoreOutput, DoubleScoreOutput> ensembleScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce
     * {@link MultiVectorOutput}.
     */

    private final MetricCollection<EnsemblePairs, MultiVectorOutput, MultiVectorOutput> ensembleMultiVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link EnsemblePairs} and produce
     * {@link BoxPlotOutput}.
     */

    final MetricCollection<EnsemblePairs, BoxPlotOutput, BoxPlotOutput> ensembleBoxPlot;

    /**
     * Default function that maps between ensemble pairs and single-valued pairs.
     */

    private final Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> toSingleValues;

    /**
     * Function to map between ensemble pairs and discrete probabilities.
     */

    private final BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> toDiscreteProbabilities;

    @Override
    public MetricOutputForProjectByTimeAndThreshold apply( EnsemblePairs input )
    {
        Objects.requireNonNull( input, "Expected non-null input to the metric processor." );
        TimeWindow timeWindow = input.getMetadata().getTimeWindow();
        Objects.requireNonNull( timeWindow, "Expected a non-null time window in the input metadata." );

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();
        futures.addDataFactory( dataFactory );

        //Slicer
        Slicer slicer = dataFactory.getSlicer();

        //Remove missing values. 
        //TODO: when time-series metrics are supported, leave missings in place for time-series
        EnsemblePairs inputNoMissing = input;
        try
        {
            inputNoMissing = slicer.filter( input, ADMISSABLE_DATA, true );
        }
        catch ( MetricInputSliceException e )
        {
            throw new MetricCalculationException( "While attempting to remove missing values: ", e );
        }

        //Process the metrics that consume ensemble pairs
        if ( hasMetrics( MetricInputGroup.ENSEMBLE ) )
        {
            processEnsemblePairs( timeWindow, inputNoMissing, futures );
        }
        //Process the metrics that consume single-valued pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            //Derive the single-valued pairs from the ensemble pairs using the configured mapper
            SingleValuedPairs singleValued = slicer.transformPairs( inputNoMissing, toSingleValues );
            processSingleValuedPairs( timeWindow, singleValued, futures );
        }
        //Process the metrics that consume discrete probability pairs
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY ) )
        {
            processDiscreteProbabilityPairs( timeWindow, inputNoMissing, futures );
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
        addToMergeList( futureResults );
        return futureResults.getMetricOutput();
    }

    /**
     * Constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(EnsemblePairs)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public MetricProcessorByTimeEnsemblePairs( final DataFactory dataFactory,
                                               final ProjectConfig config,
                                               final ExecutorService thresholdExecutor,
                                               final ExecutorService metricExecutor,
                                               final MetricOutputGroup... mergeList )
            throws MetricConfigurationException, MetricParameterException
    {
        super( dataFactory, config, thresholdExecutor, metricExecutor, mergeList );

        //Construct the metrics
        //Discrete probability input, vector output
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.SCORE ) )
        {
            discreteProbabilityScore =
                    metricFactory.ofDiscreteProbabilityScoreCollection( metricExecutor,
                                                                        getSelectedMetrics( metrics,
                                                                                            MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                            MetricOutputGroup.SCORE ) );
        }
        else
        {
            discreteProbabilityScore = null;
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
        //Ensemble input, score output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCORE ) )
        {
            ensembleScore = metricFactory.ofEnsembleScoreCollection( metricExecutor,
                                                                     getSelectedMetrics( metrics,
                                                                                         MetricInputGroup.ENSEMBLE,
                                                                                         MetricOutputGroup.SCORE ) );
        }
        else
        {
            ensembleScore = null;
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
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCORE )
             && metrics.contains( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
             && Objects.isNull( config.getInputs().getBaseline() ) )
        {
            throw new MetricConfigurationException( "Specify a non-null baseline from which to generate the '"
                                                    + MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE
                                                    + "'." );
        }
    }

    @Override
    void completeCachedOutput()
    {
        //Currently, no outputs that need to be completed
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
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.SCORE ) )
        {
            processEnsembleThresholds( timeWindow, input, futures, MetricOutputGroup.SCORE );
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
     * @throws InsufficientDataException if there is insufficient data to compute any metrics
     */

    private void processEnsembleThresholds( TimeWindow timeWindow,
                                            EnsemblePairs input,
                                            MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                            MetricOutputGroup outGroup )
    {
        //Process thresholds
        List<Threshold> global = getThresholds( MetricInputGroup.ENSEMBLE, outGroup );
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

    /**
     * Processes one threshold for metrics that consume {@link EnsemblePairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param threshold the threshold
     * @return a MetricCalculationException for information if the threshold failed
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
            if ( outGroup == MetricOutputGroup.SCORE )
            {
                futures.addDoubleScoreOutput( Pair.of( timeWindow, threshold ),
                                              processEnsembleThreshold( threshold,
                                                                        input,
                                                                        ensembleScore ) );
            }
            else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
            {
                futures.addMultiVectorOutput( Pair.of( timeWindow, threshold ),
                                              processEnsembleThreshold( threshold,
                                                                        input,
                                                                        ensembleMultiVector ) );
            }
            else if ( outGroup == MetricOutputGroup.BOXPLOT )
            {
                futures.addBoxPlotOutput( Pair.of( timeWindow, threshold ),
                                          processEnsembleThreshold( threshold,
                                                                    input,
                                                                    ensembleBoxPlot ) );
            }

        }
        //Insufficient data for one threshold: log, but allow
        catch ( MetricInputSliceException | InsufficientDataException e )
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
     * @throws InsufficientDataException if there is insufficient data to compute any metrics
     */

    private void processDiscreteProbabilityPairs( TimeWindow timeWindow,
                                                  EnsemblePairs input,
                                                  MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.SCORE ) )
        {
            processDiscreteProbabilityThresholds( timeWindow, input, futures, MetricOutputGroup.SCORE );
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
     * @throws InsufficientDataException if there is insufficient data to compute any metrics
     */

    private void processDiscreteProbabilityThresholds( TimeWindow timeWindow,
                                                       EnsemblePairs input,
                                                       MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                       MetricOutputGroup outGroup )
    {
        //Process thresholds
        List<Threshold> global = getThresholds( MetricInputGroup.DISCRETE_PROBABILITY, outGroup );
        double[] sorted = getSortedClimatology( input, global );
        Map<Threshold, MetricCalculationException> failures = new HashMap<>();
        global.forEach( threshold -> {
            Threshold useMe = getThreshold( threshold, sorted );
            MetricCalculationException result =
                    processDiscreteProbabilityThreshold( timeWindow, input, futures, outGroup, useMe );
            if ( !Objects.isNull( result ) )
            {
                failures.put( useMe, result );
            }
        } );
        //Handle any failures
        handleThresholdFailures( failures,
                                 global.size(),
                                 input.getMetadata(),
                                 MetricInputGroup.DISCRETE_PROBABILITY );
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
     * @return an exception for information if the calculation failed
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
            if ( outGroup == MetricOutputGroup.SCORE )
            {
                futures.addDoubleScoreOutput( Pair.of( timeWindow, threshold ),
                                              processDiscreteProbabilityThreshold( threshold,
                                                                                   input,
                                                                                   discreteProbabilityScore ) );
            }
            else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
            {
                futures.addMultiVectorOutput( Pair.of( timeWindow, threshold ),
                                              processDiscreteProbabilityThreshold( threshold,
                                                                                   input,
                                                                                   discreteProbabilityMultiVector ) );
            }
        }
        //Insufficient data for one threshold: log, but allow
        catch ( MetricInputSliceException | InsufficientDataException e )
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
     * @throws InsufficientDataException if the pairs contain only missing values after slicing
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processDiscreteProbabilityThreshold( Threshold threshold,
                                                 EnsemblePairs pairs,
                                                 MetricCollection<DiscreteProbabilityPairs, T, T> collection )
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
     * @throws InsufficientDataException if the pairs contain only missing values after slicing
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processEnsembleThreshold( Threshold threshold,
                                      EnsemblePairs pairs,
                                      MetricCollection<EnsemblePairs, T, T> collection )
                    throws MetricInputSliceException
    {
        //Slice the pairs
        EnsemblePairs subset = pairs;
        if ( threshold.isFinite() )
        {
            subset = dataFactory.getSlicer().filterByLeft( pairs, threshold );
        }
        checkSlice( subset, threshold );
        EnsemblePairs finalPairs = subset;
        return CompletableFuture.supplyAsync( () -> collection.apply( finalPairs ), thresholdExecutor );
    }

    /**
     * Validates the {@link DiscreteProbabilityPairs} and throws an exception if the smaller of the number of 
     * occurrences ({@link PairOfDoubles#getItemOne()} = 0) or non-occurrences ({@link PairOfDoubles#getItemOne()} = 1) 
     * is less than the {@link #minimumSampleSize}.
     * 
     * @param subset the data to validate
     * @param threshold the threshold used to localize the error message
     * @throws MetricInputSliceException if the input contains insufficient data for metric calculation 
     */

    private void checkDiscreteProbabilitySlice( DiscreteProbabilityPairs subset, Threshold threshold )
            throws MetricInputSliceException
    {
        long nonOccurrences = subset.getData().stream().filter( a -> Double.compare( a.getItemOne(), 0 ) == 0 ).count();
        double min = Math.min( nonOccurrences, subset.getData().size() - nonOccurrences );
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
