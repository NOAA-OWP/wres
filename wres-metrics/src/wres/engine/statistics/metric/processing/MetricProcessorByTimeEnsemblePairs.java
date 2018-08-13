package wres.engine.statistics.metric.processing;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.pairs.DichotomousPair;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPair;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPair;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.ListOfMetricOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProject;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScoreOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;
import wres.engine.statistics.metric.processing.MetricFuturesByTime.MetricFuturesByTimeBuilder;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * {@link EnsemblePairs} and configured transformations of {@link EnsemblePairs}. For example, metrics that consume
 * {@link SingleValuedPairs} may be processed after transforming the {@link EnsemblePairs} with an appropriate mapping
 * function, such as an ensemble mean.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricProcessorByTimeEnsemblePairs extends MetricProcessorByTime<EnsemblePairs>
{

    /**
     * Function that computes an average from an array.
     */

    private static final ToDoubleFunction<double[]> AVERAGE = right -> Arrays.stream( right ).average().getAsDouble();

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

    private final Function<EnsemblePair, SingleValuedPair> toSingleValues;

    /**
     * Function to map between ensemble pairs and discrete probabilities.
     */

    private final BiFunction<EnsemblePair, Threshold, DiscreteProbabilityPair> toDiscreteProbabilities;

    @Override
    public MetricOutputForProject apply( EnsemblePairs input )
    {
        Objects.requireNonNull( input, "Expected non-null input to the metric processor." );

        Objects.requireNonNull( input.getMetadata().getTimeWindow(),
                                "Expected a non-null time window in the input metadata." );

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();

        //Remove missing values. 
        //TODO: when time-series metrics are supported, leave missings in place for time-series
        EnsemblePairs inputNoMissing =
                Slicer.filter( input, Slicer.leftAndEachOfRight( ADMISSABLE_DATA ), ADMISSABLE_DATA );

        //Process the metrics that consume ensemble pairs
        if ( hasMetrics( MetricInputGroup.ENSEMBLE ) )
        {
            processEnsemblePairs( inputNoMissing, futures );
        }

        //Process the metrics that consume single-valued pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            //Derive the single-valued pairs from the ensemble pairs using the configured mapper
            SingleValuedPairs singleValued = Slicer.toSingleValuedPairs( inputNoMissing, toSingleValues );
            processSingleValuedPairs( singleValued, futures );
        }

        //Process the metrics that consume discrete probability pairs
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY ) )
        {
            processDiscreteProbabilityPairs( inputNoMissing, futures );
        }

        //Process the metrics that consume dichotomous pairs
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            processDichotomousPairs( inputNoMissing, futures );
        }

        // Log
        LOGGER.debug( PROCESSING_COMPLETE_MESSAGE,
                      input.getMetadata().getIdentifier().getGeospatialID(),
                      input.getMetadata().getTimeWindow() );

        //Process and return the result       
        MetricFuturesByTime futureResults = futures.build();

        //Add for merge with existing futures, if required
        addToMergeList( futureResults );

        return futureResults.getMetricOutput();
    }

    /**
     * Constructor.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @param mergeSet a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    public MetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                               final ThresholdsByMetric externalThresholds,
                                               final ExecutorService thresholdExecutor,
                                               final ExecutorService metricExecutor,
                                               final Set<MetricOutputGroup> mergeSet )
            throws MetricParameterException
    {
        super( config, externalThresholds, thresholdExecutor, metricExecutor, mergeSet );

        //Construct the metrics
        //Discrete probability input, vector output
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            discreteProbabilityScore =
                    MetricFactory.ofDiscreteProbabilityScoreCollection( metricExecutor,
                                                                        this.getMetrics( MetricInputGroup.DISCRETE_PROBABILITY,
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
                    MetricFactory.ofDiscreteProbabilityMultiVectorCollection( metricExecutor,
                                                                              this.getMetrics( MetricInputGroup.DISCRETE_PROBABILITY,
                                                                                               MetricOutputGroup.MULTIVECTOR ) );
        }
        else
        {
            discreteProbabilityMultiVector = null;
        }
        //Ensemble input, score output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            ensembleScore = MetricFactory.ofEnsembleScoreCollection( metricExecutor,
                                                                     this.getMetrics( MetricInputGroup.ENSEMBLE,
                                                                                      MetricOutputGroup.DOUBLE_SCORE ) );
        }
        else
        {
            ensembleScore = null;
        }

        //Ensemble input, multi-vector output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
        {
            ensembleMultiVector = MetricFactory.ofEnsembleMultiVectorCollection( metricExecutor,
                                                                                 this.getMetrics( MetricInputGroup.ENSEMBLE,
                                                                                                  MetricOutputGroup.MULTIVECTOR ) );
        }
        else
        {
            ensembleMultiVector = null;
        }
        //Ensemble input, box-plot output
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.BOXPLOT ) )
        {
            ensembleBoxPlot = MetricFactory.ofEnsembleBoxPlotCollection( metricExecutor,
                                                                         this.getMetrics( MetricInputGroup.ENSEMBLE,
                                                                                          MetricOutputGroup.BOXPLOT ) );
        }
        else
        {
            ensembleBoxPlot = null;
        }

        //Construct the default mapper from ensembles to single-values: this is not currently configurable
        toSingleValues = in -> SingleValuedPair.of( in.getLeft(),
                                                    Arrays.stream( in.getRight() ).average().getAsDouble() );

        //Construct the default mapper from ensembles to probabilities: this is not currently configurable
        toDiscreteProbabilities = Slicer::toDiscreteProbabilityPair;

        // Finalize validation now all required parameters are available
        // This is also called by the constructor of the superclass, but local parameters must be validated too
        validate( config );
    }

    /**
     * Helper that returns a predicate for filtering pairs based on the {@link Threshold#getDataType()}
     * of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link Threshold#getDataType()} is null
     */

    static Predicate<EnsemblePair> getFilterForEnsemblePairs( Threshold input )
    {
        ThresholdConstants.ThresholdDataType type = input.getDataType();

        Predicate<EnsemblePair> returnMe = null;

        switch ( type )
        {
            case LEFT:
                returnMe = Slicer.leftVector( input::test );
                break;
            case RIGHT:
                returnMe = Slicer.allOfRight( input::test );
                break;
            case LEFT_AND_RIGHT:
                returnMe = Slicer.leftAndAllOfRight( input::test );
                break;
            case ANY_RIGHT:
                returnMe = Slicer.anyOfRight( input::test );
                break;
            case LEFT_AND_ANY_RIGHT:
                returnMe = Slicer.leftAndAnyOfRight( input::test );
                break;
            case RIGHT_MEAN:
                returnMe = Slicer.right( input::test, AVERAGE );
                break;
            case LEFT_AND_RIGHT_MEAN:
                returnMe = Slicer.leftAndRight( input::test, AVERAGE );
                break;
        }

        return returnMe;
    }

    @Override
    void validate( ProjectConfig config )
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
                    throw new MetricConfigException( "Cannot configure '" + next
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
                throw new MetricConfigException( "Specify a non-null baseline from which to generate the '"
                                                 + MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE
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
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairs( EnsemblePairs input, MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processEnsemblePairsByThreshold( input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ) )
        {
            processEnsemblePairsByThreshold( input, futures, MetricOutputGroup.MULTIVECTOR );
        }
        if ( hasMetrics( MetricInputGroup.ENSEMBLE, MetricOutputGroup.BOXPLOT ) )
        {
            processEnsemblePairsByThreshold( input, futures, MetricOutputGroup.BOXPLOT );
        }
    }

    /**
     * Processes all thresholds for metrics that consume {@link EnsemblePairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairsByThreshold( EnsemblePairs input,
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

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold useMe = addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            Metadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = Metadata.of( input.getMetadataForBaseline(), oneOrTwo );
            }

            EnsemblePairs pairs = EnsemblePairs.of( input,
                                                    Metadata.of( input.getMetadata(), oneOrTwo ),
                                                    baselineMeta );

            //Filter the pairs if required
            if ( threshold.isFinite() )
            {
                Predicate<EnsemblePair> filter =
                        MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( useMe );

                pairs = Slicer.filter( pairs, filter, null );
            }

            processEnsemblePairs( pairs,
                                  futures,
                                  outGroup,
                                  ignoreTheseMetrics );

        }
    }

    /**
     * Processes one threshold for metrics that consume {@link EnsemblePairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    private void processEnsemblePairs( EnsemblePairs input,
                                       MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                       MetricOutputGroup outGroup,
                                       Set<MetricConstants> ignoreTheseMetrics )
    {
        if ( outGroup == MetricOutputGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( processEnsemblePairs( input,
                                                                ensembleScore,
                                                                ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
        {
            futures.addMultiVectorOutput( processEnsemblePairs( input,
                                                                ensembleMultiVector,
                                                                ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.BOXPLOT )
        {
            futures.addBoxPlotOutput( processEnsemblePairs( input,
                                                            ensembleBoxPlot,
                                                            ignoreTheseMetrics ) );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DiscreteProbabilityPairs}, which are mapped from the input
     * pairs, {@link EnsemblePairs}, using a configured mapping function.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityPairs( EnsemblePairs input,
                                                  MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processDiscreteProbabilityPairsByThreshold( input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ) )
        {
            processDiscreteProbabilityPairsByThreshold( input, futures, MetricOutputGroup.MULTIVECTOR );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input
     * pairs, {@link EnsemblePairs}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairs( EnsemblePairs input,
                                          MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processDichotomousPairsByThreshold( input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.MATRIX ) )
        {
            processDichotomousPairsByThreshold( input, futures, MetricOutputGroup.MATRIX );
        }
    }

    /**
     * Processes all thresholds for metrics that consume {@link DiscreteProbabilityPairs} for a given 
     * {@link MetricOutputGroup}. The {@link DiscreteProbabilityPairs} are produced from the input 
     * {@link EnsemblePairs} using a configured transformation. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityPairsByThreshold( EnsemblePairs input,
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

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold useMe = addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            // Transform the pairs
            DiscreteProbabilityPairs transformed = Slicer.toDiscreteProbabilityPairs( input,
                                                                                      useMe,
                                                                                      toDiscreteProbabilities );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            Metadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = Metadata.of( transformed.getMetadataForBaseline(), oneOrTwo );
            }

            transformed = DiscreteProbabilityPairs.of( transformed,
                                                       Metadata.of( transformed.getMetadata(), oneOrTwo ),
                                                       baselineMeta );

            processDiscreteProbabilityPairs( transformed,
                                             futures,
                                             outGroup,
                                             ignoreTheseMetrics );

        }
    }

    /**
     * Processes one threshold for metrics that consume {@link DiscreteProbabilityPairs} for a given 
     * {@link MetricOutputGroup}.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param threshold the threshold
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    private void processDiscreteProbabilityPairs( DiscreteProbabilityPairs input,
                                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                  MetricOutputGroup outGroup,
                                                  Set<MetricConstants> ignoreTheseMetrics )
    {
        if ( outGroup == MetricOutputGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( processDiscreteProbabilityPairs( input,
                                                                           discreteProbabilityScore,
                                                                           ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
        {
            futures.addMultiVectorOutput( processDiscreteProbabilityPairs( input,
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

    private <T extends MetricOutput<?>> Future<ListOfMetricOutput<T>>
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

    private <T extends MetricOutput<?>> Future<ListOfMetricOutput<T>>
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
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( EnsemblePairs input,
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

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics =
                    filteredByOuter.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold outerThreshold = addQuantilesToThreshold( threshold, sorted );

            // Transform the pairs to probabilities first
            DiscreteProbabilityPairs transformed = Slicer.toDiscreteProbabilityPairs( input,
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

                //Define a mapper to convert the discrete probability pairs to dichotomous pairs
                Function<DiscreteProbabilityPair, DichotomousPair> mapper =
                        pair -> DichotomousPair.of( innerThreshold.test( pair.getLeft() ),
                                                    innerThreshold.test( pair.getRight() ) );
                //Transform the pairs
                DichotomousPairs dichotomous = Slicer.toDichotomousPairs( transformed, mapper );

                // Add the threshold to the metadata, in order to fully qualify the pairs
                Metadata baselineMeta = null;
                if ( input.hasBaseline() )
                {
                    baselineMeta = Metadata.of( dichotomous.getMetadataForBaseline(), compound );
                }

                dichotomous = DichotomousPairs.ofDichotomousPairs( dichotomous,
                                                                   Metadata.of( dichotomous.getMetadata(), compound ),
                                                                   baselineMeta );

                this.processDichotomousPairs( dichotomous, futures, outGroup, unionToIgnore );
            }
        }
    }

    /**
     * Validates the current state for categorical metrics.
     * 
     * @throws MetricConfigException
     */

    private void validateCategoricalState()
    {

        // All groups that contain dichotomous and multicategory metrics must 
        // have thresholds of type ThresholdType.PROBABILITY_CLASSIFIER
        // Check that the relevant parameters have been set first

        Map<MetricConstants, Set<Threshold>> probabilityClassifiers =
                this.getThresholdsByMetric().getThresholds( ThresholdGroup.PROBABILITY_CLASSIFIER );

        // Dichotomous
        if ( this.hasMetrics( MetricInputGroup.MULTICATEGORY ) )
        {
            MetricConstants[] check = this.getMetrics( MetricInputGroup.MULTICATEGORY, null );


            if ( !Arrays.stream( check ).allMatch( next -> probabilityClassifiers.containsKey( next )
                                                           && !probabilityClassifiers.get( next ).isEmpty() ) )
            {
                throw new MetricConfigException( "In order to configure multicategory metrics for ensemble "
                                                 + "inputs, every metric group that contains multicategory "
                                                 + "metrics must also contain thresholds for classifying "
                                                 + "the forecast probabilities into occurrences and "
                                                 + "non-occurrences." );
            }
        }

        // Multicategory
        if ( this.hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            MetricConstants[] check = this.getMetrics( MetricInputGroup.DICHOTOMOUS, null );
            if ( !Arrays.stream( check ).allMatch( next -> probabilityClassifiers.containsKey( next )
                                                           && !probabilityClassifiers.get( next ).isEmpty() ) )
            {
                throw new MetricConfigException( "In order to configure dichotomous metrics for ensemble "
                                                 + "inputs, every metric group that contains dichotomous "
                                                 + "metrics must also contain thresholds for classifying "
                                                 + "the forecast probabilities into occurrences and "
                                                 + "non-occurrences." );
            }
        }
    }

}
