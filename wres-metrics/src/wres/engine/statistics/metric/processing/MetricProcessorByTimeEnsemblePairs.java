package wres.engine.statistics.metric.processing;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.OneOrTwoThresholds;
import wres.datamodel.Slicer;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdConstants.ThresholdGroup;
import wres.datamodel.ThresholdsByMetric;
import wres.datamodel.ThresholdsByType;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
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
import wres.engine.statistics.metric.config.MetricConfigHelper;
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
        //Process the metrics that consume dichotomous pairs
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
        addToMergeList( futureResults );
        return futureResults.getMetricOutput();
    }

    /**
     * Constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
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
                                               final Map<MetricConfigName, ThresholdsByType> externalThresholds,
                                               final ExecutorService thresholdExecutor,
                                               final ExecutorService metricExecutor,
                                               final MetricOutputGroup... mergeList )
            throws MetricConfigurationException, MetricParameterException
    {
        super( dataFactory, config, externalThresholds, thresholdExecutor, metricExecutor, mergeList );

        //Construct the metrics
        //Discrete probability input, vector output
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            discreteProbabilityScore =
                    metricFactory.ofDiscreteProbabilityScoreCollection( metricExecutor,
                                                                        getMetrics( metrics,
                                                                                    MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                    MetricOutputGroup.DOUBLE_SCORE ) );
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
                                                                              getMetrics( metrics,
                                                                                          MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                          MetricOutputGroup.MULTIVECTOR ) );
        }
        else
        {
            discreteProbabilityMultiVector = null;
        }
        //Ensemble input, score output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            ensembleScore = metricFactory.ofEnsembleScoreCollection( metricExecutor,
                                                                     getMetrics( metrics,
                                                                                 MetricInputGroup.ENSEMBLE,
                                                                                 MetricOutputGroup.DOUBLE_SCORE ) );
        }
        else
        {
            ensembleScore = null;
        }

        //Ensemble input, multi-vector output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
        {
            ensembleMultiVector = metricFactory.ofEnsembleMultiVectorCollection( metricExecutor,
                                                                                 getMetrics( metrics,
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
                                                                         getMetrics( metrics,
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

        // Finalize validation now all required parameters are available
        // This is also called by the constructor of the superclass, but local parameters must be validated too
        validate( config );
    }

    @Override
    void validate( ProjectConfig config ) throws MetricConfigurationException
    {
        Objects.requireNonNull( config, MetricConfigHelper.NULL_CONFIGURATION_ERROR );

        // This method checks local parameters, so ensure they have been set.
        // If null, this is being called by the superclass constructor, not the local constructor
        if ( Objects.nonNull( this.toSingleValues ) )
        {

            // Thresholds required for dichotomous and probability metrics
            for ( MetricConstants next : this.metrics )
            {
                if ( ( next.isInGroup( MetricInputGroup.DICHOTOMOUS )
                       || next.isInGroup( MetricInputGroup.DISCRETE_PROBABILITY ) )
                     && !this.getThresholdsByMetric().hasThresholdsForThisMetricAndTheseTypes( next,
                                                                                               ThresholdGroup.PROBABILITY,
                                                                                               ThresholdGroup.VALUE ) )
                {
                    throw new MetricConfigurationException( "Cannot configure '" + next
                                                            + "' without thresholds to define the events: correct the "
                                                            + "configuration labelled '"
                                                            + config.getLabel()
                                                            + "'." );
                }
            }

            validateCategoricalState();

            //Ensemble input, vector output
            if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.DOUBLE_SCORE )
                 && this.metrics.contains( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                 && Objects.isNull( config.getInputs().getBaseline() ) )
            {
                throw new MetricConfigurationException( "Specify a non-null baseline from which to generate the '"
                                                        + MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.name()
                                                        + "'." );
            }
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
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processEnsemblePairsByThreshold( timeWindow, input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
        {
            processEnsemblePairsByThreshold( timeWindow, input, futures, MetricOutputGroup.MULTIVECTOR );
        }
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.BOXPLOT ) )
        {
            processEnsemblePairsByThreshold( timeWindow, input, futures, MetricOutputGroup.BOXPLOT );
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

    private void processEnsemblePairsByThreshold( TimeWindow timeWindow,
                                                  EnsemblePairs input,
                                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                  MetricOutputGroup outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( MetricInputGroup.ENSEMBLE, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<Threshold> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );
        Map<OneOrTwoThresholds, MetricCalculationException> failures = new HashMap<>();
        union.forEach( threshold -> {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold useMe = addQuantilesToThreshold( threshold, sorted );

            try
            {
                //Slice the pairs if required
                EnsemblePairs pairs = input;
                if ( threshold.isFinite() )
                {
                    pairs = dataFactory.getSlicer().filterByLeft( input, useMe );
                }

                processEnsemblePairs( Pair.of( timeWindow, OneOrTwoThresholds.of( useMe ) ),
                                      pairs,
                                      futures,
                                      outGroup,
                                      ignoreTheseMetrics );

            }
            //Insufficient data for one threshold: log, but allow
            catch ( MetricInputSliceException | InsufficientDataException e )
            {
                failures.put( OneOrTwoThresholds.of( useMe ), new MetricCalculationException( e.getMessage(), e ) );
            }
        } );
        //Handle any failures
        logThresholdFailures( failures, union.size(), input.getMetadata(), MetricInputGroup.ENSEMBLE );
    }

    /**
     * Processes one threshold for metrics that consume {@link EnsemblePairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param key the key against which the results should be stored
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    private void processEnsemblePairs( Pair<TimeWindow, OneOrTwoThresholds> key,
                                       EnsemblePairs input,
                                       MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                       MetricOutputGroup outGroup,
                                       Set<MetricConstants> ignoreTheseMetrics )
    {
        if ( outGroup == MetricOutputGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( key, processEnsemblePairs( input,
                                                                     ensembleScore,
                                                                     ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
        {
            futures.addMultiVectorOutput( key, processEnsemblePairs( input,
                                                                     ensembleMultiVector,
                                                                     ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.BOXPLOT )
        {
            futures.addBoxPlotOutput( key, processEnsemblePairs( input,
                                                                 ensembleBoxPlot,
                                                                 ignoreTheseMetrics ) );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DiscreteProbabilityPairs}, which are mapped from the input
     * pairs, {@link EnsemblePairs}, using a configured mapping function.
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
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processDiscreteProbabilityPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ) )
        {
            processDiscreteProbabilityPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.MULTIVECTOR );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input
     * pairs, {@link EnsemblePairs}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairs( TimeWindow timeWindow,
                                          EnsemblePairs input,
                                          MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processDichotomousPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.MATRIX ) )
        {
            processDichotomousPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.MATRIX );
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

    private void processDiscreteProbabilityPairsByThreshold( TimeWindow timeWindow,
                                                             EnsemblePairs input,
                                                             MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                             MetricOutputGroup outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( MetricInputGroup.DISCRETE_PROBABILITY, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<Threshold> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );
        Map<OneOrTwoThresholds, MetricCalculationException> failures = new HashMap<>();
        union.forEach( threshold -> {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold useMe = addQuantilesToThreshold( threshold, sorted );

            try
            {
                // Transform the pairs
                DiscreteProbabilityPairs transformed = dataFactory.getSlicer()
                                                                  .transformPairs( input,
                                                                                   useMe,
                                                                                   toDiscreteProbabilities );

                processDiscreteProbabilityPairs( Pair.of( timeWindow, OneOrTwoThresholds.of( useMe ) ),
                                                 transformed,
                                                 futures,
                                                 outGroup,
                                                 ignoreTheseMetrics );

            }
            //Insufficient data for one threshold: log, but allow
            catch ( InsufficientDataException e )
            {
                failures.put( OneOrTwoThresholds.of( useMe ), new MetricCalculationException( e.getMessage(), e ) );
            }

        } );
        //Handle any failures
        logThresholdFailures( failures,
                              union.size(),
                              input.getMetadata(),
                              MetricInputGroup.DISCRETE_PROBABILITY );
    }

    /**
     * Processes one threshold for metrics that consume {@link DiscreteProbabilityPairs} for a given 
     * {@link MetricOutputGroup}.
     * 
     * @param key the key against which the results should be stored
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param threshold the threshold
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    private void processDiscreteProbabilityPairs( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                  DiscreteProbabilityPairs input,
                                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                  MetricOutputGroup outGroup,
                                                  Set<MetricConstants> ignoreTheseMetrics )
    {
        if ( outGroup == MetricOutputGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( key, processDiscreteProbabilityPairs( input,
                                                                                discreteProbabilityScore,
                                                                                ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
        {
            futures.addMultiVectorOutput( key, processDiscreteProbabilityPairs( input,
                                                                                discreteProbabilityMultiVector,
                                                                                ignoreTheseMetrics ) );
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link DiscreteProbabilityPairs}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param pairs the pairs
     * @param collection the metric collection
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     * @return the future result
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processDiscreteProbabilityPairs( DiscreteProbabilityPairs pairs,
                                             MetricCollection<DiscreteProbabilityPairs, T, T> collection,
                                             Set<MetricConstants> ignoreTheseMetrics )
    {
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ignoreTheseMetrics ),
                                              thresholdExecutor );
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link EnsemblePairs} at a specific
     * {@link Threshold} and appends it to the input map of futures.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     * @return the future result
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processEnsemblePairs( EnsemblePairs pairs,
                                  MetricCollection<EnsemblePairs, T, T> collection,
                                  Set<MetricConstants> ignoreTheseMetrics )
    {
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ignoreTheseMetrics ),
                                              thresholdExecutor );
    }

    /**
     * Processes all thresholds for metrics that consume {@link DichotomousPairs} for a given 
     * {@link MetricOutputGroup}. The {@link DichotomousPairs} are produced from the input 
     * {@link EnsemblePairs} using a configured transformation. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( TimeWindow timeWindow,
                                                     EnsemblePairs input,
                                                     MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                     MetricOutputGroup outGroup )
    {
        // Find the thresholds filtered by group
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( MetricInputGroup.DICHOTOMOUS, outGroup );

        // Find the thresholds filtered by outer type
        ThresholdsByMetric filteredByOuter = filtered.filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the thresholds filtered by inner type
        ThresholdsByMetric filteredByInner = filtered.filterByType( ThresholdGroup.PROBABILITY_CLASSIFIER );

        // Find the union across metrics
        Set<Threshold> union = filteredByOuter.union();

        double[] sorted = getSortedClimatology( input, union );
        Map<OneOrTwoThresholds, MetricCalculationException> failures = new HashMap<>();
        union.forEach( threshold -> {

            Set<MetricConstants> ignoreTheseMetrics =
                    filteredByOuter.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold outerThreshold = addQuantilesToThreshold( threshold, sorted );

            try
            {
                // Transform the pairs to probabilities first
                DiscreteProbabilityPairs transformed = dataFactory.getSlicer()
                                                                  .transformPairs( input,
                                                                                   outerThreshold,
                                                                                   toDiscreteProbabilities );

                // Find the union of classifiers across all metrics   
                Set<Threshold> classifiers = filteredByInner.union();

                for ( Threshold innerThreshold : classifiers )
                {
                    // Metrics for which the current classifier is not required
                    Set<MetricConstants> innerIgnoreTheseMetrics =
                            filteredByInner.doesNotHaveTheseMetricsForThisThreshold( innerThreshold );

                    // Union of metrics to ignore, either because the threshold is not required or
                    // because the classifier is not required
                    Set<MetricConstants> unionToIgnore = new HashSet<>( ignoreTheseMetrics );
                    unionToIgnore.addAll( innerIgnoreTheseMetrics );

                    // Derive compound threshold from outerThreshold and innerThreshold
                    OneOrTwoThresholds compound = OneOrTwoThresholds.of( outerThreshold, innerThreshold );

                    Pair<TimeWindow, OneOrTwoThresholds> nextKey = Pair.of( timeWindow, compound );

                    //Define a mapper to convert the discrete probability pairs to dichotomous pairs
                    Function<PairOfDoubles, PairOfBooleans> mapper =
                            pair -> dataFactory.pairOf( innerThreshold.test( pair.getItemOne() ),
                                                        innerThreshold.test( pair.getItemTwo() ) );
                    //Transform the pairs
                    DichotomousPairs dichotomous = dataFactory.getSlicer().transformPairs( transformed, mapper );
                    processDichotomousPairs( nextKey, dichotomous, futures, outGroup, unionToIgnore );
                }

            }
            //Insufficient data for one threshold: log, but allow
            catch ( InsufficientDataException e )
            {
                failures.put( OneOrTwoThresholds.of( outerThreshold ),
                              new MetricCalculationException( e.getMessage(), e ) );
            }

        } );
        //Handle any failures
        logThresholdFailures( failures,
                              union.size(),
                              input.getMetadata(),
                              MetricInputGroup.DISCRETE_PROBABILITY );
    }

    /**
     * Validates the current state for categorical metrics.
     * 
     * @throws MetricConfigurationException
     */

    private void validateCategoricalState() throws MetricConfigurationException
    {

        // All groups that contain dichotomous and multicategory metrics must 
        // have thresholds of type ThresholdType.PROBABILITY_CLASSIFIER
        // Check that the relevant parameters have been set first

        Map<MetricConstants, Set<Threshold>> probabilityClassifiers =
                this.getThresholdsByMetric().getThresholds( ThresholdGroup.PROBABILITY_CLASSIFIER );

        // Dichotomous
        if ( this.hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            MetricConstants[] check = this.getMetrics( this.metrics, MetricInputGroup.DICHOTOMOUS, null );


            if ( !Arrays.stream( check ).allMatch( next -> probabilityClassifiers.containsKey( next )
                                                           && !probabilityClassifiers.get( next ).isEmpty() ) )
            {
                throw new MetricConfigurationException( "In order to configure dichotomous metrics for ensemble "
                                                        + "inputs, every metric group that contains dichotomous "
                                                        + "metrics must also contain thresholds for classifying "
                                                        + "the forecast probabilities into occurrences and "
                                                        + "non-occurrences." );
            }
        }

        // Multicategory
        if ( this.hasMetrics( MetricInputGroup.MULTICATEGORY ) )
        {
            MetricConstants[] check = this.getMetrics( this.metrics, MetricInputGroup.MULTICATEGORY, null );
            if ( !Arrays.stream( check ).allMatch( next -> probabilityClassifiers.containsKey( next )
                                                           && !probabilityClassifiers.get( next ).isEmpty() ) )
            {
                throw new MetricConfigurationException( "In order to configure multicategory metrics for ensemble "
                                                        + "inputs, every metric group that contains dichotomous "
                                                        + "metrics must also contain thresholds for classifying "
                                                        + "the forecast probabilities into occurrences and "
                                                        + "non-occurrences." );
            }
        }
    }

}
