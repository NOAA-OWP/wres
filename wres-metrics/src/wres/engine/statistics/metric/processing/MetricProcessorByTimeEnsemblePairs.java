package wres.engine.statistics.metric.processing;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.BasicPool.Builder;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.Metrics;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
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
 * ensemble pairs and configured transformations of ensemble pairs. For example, metrics that consume single-valued
 * pairs may be processed after transforming the ensemble pairs with an appropriate mapping
 * function, such as an ensemble mean.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricProcessorByTimeEnsemblePairs extends MetricProcessorByTime<Pool<Pair<Double, Ensemble>>>
{
    /**
     * Logger instance.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricProcessorByTimeEnsemblePairs.class );

    /**
     * Function that computes an average from an array.
     */

    private static final ToDoubleFunction<double[]> AVERAGE = right -> Arrays.stream( right ).average().getAsDouble();

    /**
     * A {@link MetricCollection} of {@link Metric} that consume discrete probability pairs and produce
     * {@link DoubleScoreStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> discreteProbabilityScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume discrete probability pairs and produce
     * {@link ScoreStatistic}.
     */

    private final MetricCollection<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter> discreteProbabilityDiagrams;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce {@link DoubleScoreStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> ensembleScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce 
     * {@link DoubleScoreStatisticOuter}. This collection does not include any scores that require a reference or 
     * baseline dataset.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> ensembleScoreNoBaseline;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce
     * {@link DiagramStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter, DiagramStatisticOuter> ensembleDiagrams;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce
     * {@link BoxplotStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, BoxplotStatisticOuter, BoxplotStatisticOuter> ensembleBoxPlot;

    /**
     * Default function that maps between ensemble pairs and single-valued pairs.
     */

    private final Function<Pair<Double, Ensemble>, Pair<Double, Double>> toSingleValues;

    @Override
    public StatisticsForProject apply( Pool<Pair<Double, Ensemble>> input )
    {
        Objects.requireNonNull( input, "Expected non-null input to the metric processor." );

        Objects.requireNonNull( input.getMetadata().getTimeWindow(),
                                "Expected a non-null time window in the input metadata." );

        // Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();

        // Remove missing values. 
        // TODO: when time-series metrics are supported, leave missings in place for time-series
        // Also retain the time-series shape, where required
        Pool<Pair<Double, Ensemble>> inputNoMissing =
                Slicer.transform( input, Slicer.leftAndEachOfRight( MetricProcessor.ADMISSABLE_DATA ) );

        // Process the metrics that consume ensemble pairs
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE ) )
        {
            this.processEnsemblePairs( inputNoMissing, futures );
        }

        // Process the metrics that consume single-valued pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED ) )
        {
            //Derive the single-valued pairs from the ensemble pairs using the configured mapper
            Pool<Pair<Double, Double>> singleValued =
                    Slicer.transform( inputNoMissing, toSingleValues );

            super.processSingleValuedPairs( singleValued, futures );
        }

        //Process the metrics that consume discrete probability pairs
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY ) )
        {
            this.processDiscreteProbabilityPairs( inputNoMissing, futures );
        }

        //Process the metrics that consume dichotomous pairs
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            this.processDichotomousPairs( inputNoMissing, futures );
        }

        // Log
        LOGGER.debug( PROCESSING_COMPLETE_MESSAGE,
                      MessageFactory.parse( input.getMetadata().getPool().getGeometryTuples( 0 ) ),
                      input.getMetadata().getTimeWindow() );

        // Process and return the result       
        MetricFuturesByTime futureResults = futures.build();

        // Add for merge with existing futures, if required
        this.addToMergeList( futureResults );

        return futureResults.getMetricOutput();
    }

    /**
     * Constructor.
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @param mergeSet a list of {@link StatisticType} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    public MetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                               final Metrics metrics,
                                               final ExecutorService thresholdExecutor,
                                               final ExecutorService metricExecutor,
                                               final Set<StatisticType> mergeSet )
            throws MetricParameterException
    {
        super( config, metrics, thresholdExecutor, metricExecutor, mergeSet );

        //Construct the metrics
        //Discrete probability input, vector output
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE ) )
        {
            this.discreteProbabilityScore =
                    MetricFactory.ofDiscreteProbabilityScoreCollection( metricExecutor,
                                                                        this.getMetrics( SampleDataGroup.DISCRETE_PROBABILITY,
                                                                                         StatisticType.DOUBLE_SCORE ) );

            LOGGER.debug( "Created the discrete probability scores for processing. {}", this.discreteProbabilityScore );
        }
        else
        {
            this.discreteProbabilityScore = null;
        }
        //Discrete probability input, multi-vector output
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DIAGRAM ) )
        {
            this.discreteProbabilityDiagrams =
                    MetricFactory.ofDiscreteProbabilityDiagramCollection( metricExecutor,
                                                                          this.getMetrics( SampleDataGroup.DISCRETE_PROBABILITY,
                                                                                           StatisticType.DIAGRAM ) );

            LOGGER.debug( "Created the discrete probability diagrams for processing. {}",
                          this.discreteProbabilityDiagrams );
        }
        else
        {
            this.discreteProbabilityDiagrams = null;
        }
        //Ensemble input, score output
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ) )
        {
            MetricConstants[] ensembleMetrics = this.getMetrics( SampleDataGroup.ENSEMBLE,
                                                                 StatisticType.DOUBLE_SCORE );

            this.ensembleScore = MetricFactory.ofEnsembleScoreCollection( metricExecutor,
                                                                          ensembleMetrics );

            // Create a set of metrics for when no baseline is available, assuming there is at least one metric
            if ( ensembleMetrics.length > 1 )
            {
                Set<MetricConstants> filteredMetrics = Arrays.stream( ensembleMetrics )
                                                             .filter( next -> next != MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                                                             .collect( Collectors.toSet() );

                MetricConstants[] filteredArray =
                        filteredMetrics.toArray( new MetricConstants[filteredMetrics.size()] );

                this.ensembleScoreNoBaseline = MetricFactory.ofEnsembleScoreCollection( metricExecutor,
                                                                                        filteredArray );
            }
            else
            {
                this.ensembleScoreNoBaseline = null;
            }

            LOGGER.debug( "Created the ensemble scores for processing. {}", this.ensembleScore );
        }
        else
        {
            this.ensembleScore = null;
            this.ensembleScoreNoBaseline = null;
        }

        //Ensemble input, multi-vector output
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DIAGRAM ) )
        {
            this.ensembleDiagrams = MetricFactory.ofEnsembleDiagramCollection( metricExecutor,
                                                                               this.getMetrics( SampleDataGroup.ENSEMBLE,
                                                                                                StatisticType.DIAGRAM ) );

            LOGGER.debug( "Created the ensemble diagrams for processing. {}", this.ensembleDiagrams );
        }
        else
        {
            this.ensembleDiagrams = null;
        }
        //Ensemble input, box-plot output
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.BOXPLOT_PER_PAIR ) )
        {
            this.ensembleBoxPlot = MetricFactory.ofEnsembleBoxPlotCollection( metricExecutor,
                                                                              this.getMetrics( SampleDataGroup.ENSEMBLE,
                                                                                               StatisticType.BOXPLOT_PER_PAIR ) );

            LOGGER.debug( "Created the ensemble box plots for processing. {}", this.ensembleBoxPlot );
        }
        else
        {
            this.ensembleBoxPlot = null;
        }

        //Construct the default mapper from ensembles to single-values: this is not currently configurable
        this.toSingleValues = in -> Pair.of( in.getLeft(),
                                             Arrays.stream( in.getRight().getMembers() )
                                                   .average()
                                                   .getAsDouble() );

        // Finalize validation now all required parameters are available
        // This is also called by the constructor of the superclass, but local parameters must be validated too
        this.validate( config );
    }

    /**
     * <p>Removes a duplicate instance of the {@link MetricConstants.SAMPLE_SIZE}, which may appear in more than one
     * context.
     * 
     * <p>See #65138. 
     * 
     * <p>TODO: remove this on completing #65101 and removing the sample size metric
     * 
     * @param inGroup the {@link SampleDataGroup}, may be null
     * @param outGroup the {@link StatisticType}, may be null
     * @return a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}
     *         or an empty array if both inputs are defined and no corresponding metrics are present
     */

    @Override
    MetricConstants[] getMetrics( SampleDataGroup inGroup, StatisticType outGroup )
    {

        Set<MetricConstants> metrics = new HashSet<>( Arrays.asList( super.getMetrics( inGroup, outGroup ) ) );

        if ( inGroup == SampleDataGroup.SINGLE_VALUED && metrics.contains( MetricConstants.SAMPLE_SIZE ) )
        {
            metrics.remove( MetricConstants.SAMPLE_SIZE );
        }

        return metrics.toArray( new MetricConstants[metrics.size()] );
    }

    /**
     * Helper that returns a predicate for filtering pairs based on the {@link ThresholdOuter#getDataType()}
     * of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link ThresholdOuter#getDataType()} is null
     * @throws IllegalStateException if the {@link ThresholdOuter#getDataType()} is not recognized
     */

    static Predicate<Pair<Double, Ensemble>> getFilterForEnsemblePairs( ThresholdOuter input )
    {
        switch ( input.getDataType() )
        {
            case LEFT:
                return Slicer.leftVector( input::test );
            case RIGHT:
                return Slicer.allOfRight( input::test );
            case LEFT_AND_RIGHT:
                return Slicer.leftAndAllOfRight( input::test );
            case ANY_RIGHT:
                return Slicer.anyOfRight( input::test );
            case LEFT_AND_ANY_RIGHT:
                return Slicer.leftAndAnyOfRight( input::test );
            case RIGHT_MEAN:
                return Slicer.right( input::test, AVERAGE );
            case LEFT_AND_RIGHT_MEAN:
                return Slicer.leftAndRight( input::test, AVERAGE );
            default:
                throw new IllegalStateException( "Unrecognized threshold type '" + input.getDataType() + "'." );
        }

    }

    @Override
    void validate( ProjectConfig config )
    {
        Objects.requireNonNull( config, MetricConfigHelper.NULL_CONFIGURATION_ERROR );

        // Annotate any configuration error, if possible
        String configurationLabel = ".";
        if ( Objects.nonNull( config.getLabel() ) )
        {
            configurationLabel = " labelled '"
                                 + config.getLabel()
                                 + "'.";
        }

        // This method checks local parameters, so ensure they have been set.
        // If null, this is being called by the superclass constructor, not the local constructor
        if ( Objects.nonNull( this.toSingleValues ) )
        {

            // Thresholds required for dichotomous and probability metrics
            for ( MetricConstants next : super.getMetrics().getMetrics() )
            {
                if ( ( next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                       || next.isInGroup( SampleDataGroup.DISCRETE_PROBABILITY ) )
                     && !super.getMetrics().getThresholdsByMetric()
                                           .hasThresholdsForThisMetricAndTheseTypes( next,
                                                                                     ThresholdGroup.PROBABILITY,
                                                                                     ThresholdGroup.VALUE ) )
                {
                    throw new MetricConfigException( "Cannot configure '" + next
                                                     + "' without thresholds to define the events: "
                                                     + "add one or more thresholds to the configuration"
                                                     + configurationLabel );
                }
            }

            this.validateCategoricalState();

            //Ensemble input, vector output
            if ( hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE )
                 && this.getMetrics().getMetrics().contains( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
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
     * Processes a set of metric futures that consume ensemble pairs, which are mapped from the input pairs,
     * ensemble pairs, using a configured mapping function.
     * 
     * TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairs( Pool<Pair<Double, Ensemble>> input, MetricFuturesByTimeBuilder futures )
    {
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ) )
        {
            processEnsemblePairsByThreshold( input, futures, StatisticType.DOUBLE_SCORE );
        }
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DIAGRAM ) )
        {
            processEnsemblePairsByThreshold( input, futures, StatisticType.DIAGRAM );
        }
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.BOXPLOT_PER_PAIR ) )
        {
            processEnsemblePairsByThreshold( input, futures, StatisticType.BOXPLOT_PER_PAIR );
        }
    }

    /**
     * Processes all thresholds for metrics that consume ensemble pairs and produce a specified 
     * {@link StatisticType}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairsByThreshold( Pool<Pair<Double, Ensemble>> input,
                                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                  StatisticType outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = super.getMetrics().getThresholdsByMetric()
                                                        .filterByGroup( SampleDataGroup.ENSEMBLE, outGroup )
                                                        .filterByType( ThresholdGroup.PROBABILITY,
                                                                       ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<ThresholdOuter> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( ThresholdOuter threshold : union )
        {
            // Add quantiles to threshold
            ThresholdOuter useMe = addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            PoolMetadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = PoolMetadata.of( input.getBaselineData().getMetadata(), oneOrTwo );
            }

            //Filter the pairs if required
            Pool<Pair<Double, Ensemble>> pairs = input;

            if ( threshold.isFinite() )
            {
                Predicate<Pair<Double, Ensemble>> filter =
                        MetricProcessorByTimeEnsemblePairs.getFilterForEnsemblePairs( useMe );

                pairs = Slicer.filter( pairs, filter, null );
            }


            Builder<Pair<Double, Ensemble>> builder = new Builder<>();
            pairs = builder.addData( pairs )
                           .setMetadata( PoolMetadata.of( pairs.getMetadata(),
                                                          oneOrTwo ) )
                           .setMetadataForBaseline( baselineMeta )
                           .build();


            this.processEnsemblePairs( pairs,
                                       futures,
                                       outGroup );

        }
    }

    /**
     * Processes one threshold for metrics that consume ensemble pairs and produce a specified 
     * {@link StatisticType}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     */

    private void processEnsemblePairs( Pool<Pair<Double, Ensemble>> input,
                                       MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                       StatisticType outGroup )
    {
        if ( outGroup == StatisticType.DOUBLE_SCORE )
        {
            // Baseline pool without a baseline of its own?
            if ( input.getMetadata().getPool().getIsBaselinePool() && !input.hasBaseline() )
            {
                futures.addDoubleScoreOutput( this.processEnsemblePairs( input, this.ensembleScoreNoBaseline ) );
            }
            else
            {
                futures.addDoubleScoreOutput( this.processEnsemblePairs( input, this.ensembleScore ) );
            }
        }
        else if ( outGroup == StatisticType.DIAGRAM )
        {
            futures.addDiagramOutput( this.processEnsemblePairs( input, this.ensembleDiagrams ) );
        }
        else if ( outGroup == StatisticType.BOXPLOT_PER_PAIR )
        {
            futures.addBoxPlotOutputPerPair( this.processEnsemblePairs( input, this.ensembleBoxPlot ) );
        }
    }

    /**
     * Processes a set of metric futures that consume discrete probability pairs, which are mapped from ensemble pairs 
     * using a configured mapping function.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityPairs( Pool<Pair<Double, Ensemble>> input,
                                                  MetricFuturesByTimeBuilder futures )
    {
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE ) )
        {
            this.processDiscreteProbabilityPairsByThreshold( input, futures, StatisticType.DOUBLE_SCORE );
        }
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DIAGRAM ) )
        {
            this.processDiscreteProbabilityPairsByThreshold( input, futures, StatisticType.DIAGRAM );
        }
    }

    /**
     * Processes a set of metric futures that consume dichotomous pairs, which are mapped from ensemble pairs. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairs( Pool<Pair<Double, Ensemble>> input,
                                          MetricFuturesByTimeBuilder futures )
    {
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ) )
        {
            this.processDichotomousPairsByThreshold( input, futures, StatisticType.DOUBLE_SCORE );
        }
    }

    /**
     * Processes all thresholds for metrics that consume discrete probability pairs for a given {@link StatisticType}. 
     * The discrete probability pairs are produced from ensemble pairs using a configured transformation. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityPairsByThreshold( Pool<Pair<Double, Ensemble>> input,
                                                             MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                             StatisticType outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = super.getMetrics().getThresholdsByMetric()
                                                        .filterByGroup( SampleDataGroup.DISCRETE_PROBABILITY, outGroup )
                                                        .filterByType( ThresholdGroup.PROBABILITY,
                                                                       ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<ThresholdOuter> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( ThresholdOuter threshold : union )
        {
            // Add quantiles to threshold
            ThresholdOuter useMe = this.addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            // Transform the pairs
            Function<Pair<Double, Ensemble>, Pair<Probability, Probability>> transformer =
                    pair -> Slicer.toDiscreteProbabilityPair( pair,
                                                              useMe );

            Pool<Pair<Probability, Probability>> transformed = Slicer.transform( input, transformer );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            PoolMetadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = PoolMetadata.of( transformed.getBaselineData().getMetadata(), oneOrTwo );
            }

            Builder<Pair<Probability, Probability>> builder = new Builder<>();
            transformed = builder.addData( transformed )
                                 .setMetadata( PoolMetadata.of( transformed.getMetadata(), oneOrTwo ) )
                                 .setMetadataForBaseline( baselineMeta )
                                 .build();

            this.processDiscreteProbabilityPairs( transformed,
                                                  futures,
                                                  outGroup );

        }
    }

    /**
     * Processes one threshold for metrics that consume discrete probability pairs for a given {@link StatisticType}.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param threshold the threshold
     */

    private void processDiscreteProbabilityPairs( Pool<Pair<Probability, Probability>> input,
                                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                  StatisticType outGroup )
    {
        if ( outGroup == StatisticType.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( this.processDiscreteProbabilityPairs( input,
                                                                                this.discreteProbabilityScore ) );
        }
        else if ( outGroup == StatisticType.DIAGRAM )
        {
            futures.addDiagramOutput( this.processDiscreteProbabilityPairs( input, this.discreteProbabilityDiagrams ) );
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes discrete probability pairs.
     * 
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     */

    private <T extends Statistic<?>> Future<List<T>>
            processDiscreteProbabilityPairs( Pool<Pair<Probability, Probability>> pairs,
                                             MetricCollection<Pool<Pair<Probability, Probability>>, T, T> collection )
    {
        // More samples than the minimum sample size?
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();

        int actualSampleSize = this.getSampleSizeForProbPairs( pairs );

        // Log and return an empty result if the sample size is too small
        if ( actualSampleSize < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While evaluating discrete probability pairs for pool {}, discovered that the smaller of "
                              + "the number of left occurrences (Pr=1.0) and non-occurrences (Pr=0.0) was {}, which is "
                              + "less than the minimum sample size of {}. The following metrics will not be computed "
                              + "for this pool: {}.",
                              pairs.getMetadata(),
                              actualSampleSize,
                              minimumSampleSize,
                              collection.getMetrics() );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        // Are there skill metrics and does the baseline also meet the minimum sample size constraint?
        return super.processWithOrWithoutSkillMetrics( pairs, collection );
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes ensemble pairs at a specific
     * {@link ThresholdOuter} and appends it to the input map of futures.
     * 
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     */

    private <T extends Statistic<?>> Future<List<T>>
            processEnsemblePairs( Pool<Pair<Double, Ensemble>> pairs,
                                  MetricCollection<Pool<Pair<Double, Ensemble>>, T, T> collection )
    {

        // More samples than the minimum sample size?
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();
        int actualSampleSize = pairs.getRawData().size();

        // Log and return an empty result if the sample size is too small
        if ( actualSampleSize < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                Set<MetricConstants> collected = new HashSet<>( collection.getMetrics() );
                collected.remove( MetricConstants.SAMPLE_SIZE );

                LOGGER.debug( "While processing pairs for pool {}, discovered {} pairs, which is fewer than the "
                              + "minimum sample size of {} pairs. The following metrics will not be computed for this "
                              + "pool: {}.",
                              pairs.getMetadata(),
                              actualSampleSize,
                              minimumSampleSize,
                              collected );
            }

            // Allow the sample size through without constraint
            if ( collection.getMetrics().contains( MetricConstants.SAMPLE_SIZE ) )
            {
                Set<MetricConstants> ss = Set.of( MetricConstants.SAMPLE_SIZE );
                return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ss ),
                                                      this.thresholdExecutor );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        // Are there skill metrics and does the baseline also meet the minimum sample size constraint?
        return super.processWithOrWithoutSkillMetrics( pairs, collection );
    }

    /**
     * Processes all thresholds for metrics that consume dichotomous pairs for a given {@link StatisticType}. The 
     * dichotomous pairs are produced from the input ensemble pairs using a configured transformation. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( Pool<Pair<Double, Ensemble>> input,
                                                     MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                     StatisticType outGroup )
    {
        // Find the thresholds filtered by group
        ThresholdsByMetric filtered = super.getMetrics().getThresholdsByMetric()
                                                        .filterByGroup( SampleDataGroup.DICHOTOMOUS, outGroup );

        // Find the thresholds filtered by outer type
        ThresholdsByMetric filteredByOuter = filtered.filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the thresholds filtered by inner type
        ThresholdsByMetric filteredByInner = filtered.filterByType( ThresholdGroup.PROBABILITY_CLASSIFIER );

        // Find the union across metrics
        Set<ThresholdOuter> union = filteredByOuter.union();

        double[] sorted = getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( ThresholdOuter threshold : union )
        {
            // Add quantiles to threshold
            ThresholdOuter outerThreshold = addQuantilesToThreshold( threshold, sorted );

            // Transform the pairs to probabilities first
            // Transform the pairs
            Function<Pair<Double, Ensemble>, Pair<Probability, Probability>> transformer =
                    pair -> Slicer.toDiscreteProbabilityPair( pair,
                                                              outerThreshold );

            Pool<Pair<Probability, Probability>> transformed = Slicer.transform( input, transformer );

            // Find the union of classifiers across all metrics   
            Set<ThresholdOuter> classifiers = filteredByInner.union();

            for ( ThresholdOuter innerThreshold : classifiers )
            {
                // Derive compound threshold from outerThreshold and innerThreshold
                OneOrTwoThresholds compound = OneOrTwoThresholds.of( outerThreshold, innerThreshold );

                //Define a mapper to convert the discrete probability pairs to dichotomous pairs
                Function<Pair<Probability, Probability>, Pair<Boolean, Boolean>> mapper =
                        pair -> Pair.of( innerThreshold.test( pair.getLeft().getProbability() ),
                                         innerThreshold.test( pair.getRight().getProbability() ) );
                //Transform the pairs
                Pool<Pair<Boolean, Boolean>> dichotomous = Slicer.transform( transformed, mapper );

                // Add the threshold to the metadata, in order to fully qualify the pairs
                PoolMetadata baselineMeta = null;
                if ( input.hasBaseline() )
                {
                    baselineMeta = PoolMetadata.of( dichotomous.getBaselineData().getMetadata(), compound );
                }

                Builder<Pair<Boolean, Boolean>> builder = new Builder<>();
                dichotomous = builder.addData( dichotomous )
                                     .setMetadata( PoolMetadata.of( dichotomous.getMetadata(), compound ) )
                                     .setMetadataForBaseline( baselineMeta )
                                     .build();

                super.processDichotomousPairs( dichotomous, futures, outGroup );
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
        Map<MetricConstants, Set<ThresholdOuter>> probabilityClassifiers =
                super.getMetrics().getThresholdsByMetric()
                                  .getThresholds( ThresholdGroup.PROBABILITY_CLASSIFIER );

        // Multicategory
        if ( this.hasMetrics( SampleDataGroup.MULTICATEGORY ) )
        {
            MetricConstants[] check = this.getMetrics( SampleDataGroup.MULTICATEGORY, null );

            if ( !Arrays.stream( check )
                        .allMatch( next -> probabilityClassifiers.containsKey( next )
                                           && !probabilityClassifiers.get( next ).isEmpty() ) )
            {
                throw new MetricConfigException( "In order to configure multicategory metrics for ensemble "
                                                 + "inputs, every metric group that contains multicategory "
                                                 + "metrics must also contain thresholds for classifying "
                                                 + "the forecast probabilities into occurrences and "
                                                 + "non-occurrences." );
            }
        }

        // Dichotomous
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            MetricConstants[] check = this.getMetrics( SampleDataGroup.DICHOTOMOUS, null );
            if ( !Arrays.stream( check )
                        .allMatch( next -> probabilityClassifiers.containsKey( next )
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

    /**
     * @param pairs the pairs whose sample size is required
     * @return the sample size for a pool of probability pairs, which is the smaller of left occurrences (Pr=1.0) and 
     *            non-occurrences (Pr=0.0)
     */

    private int getSampleSizeForProbPairs( Pool<Pair<Probability, Probability>> pairs )
    {
        int occurrences = 0;
        int nonOccurrences = 0;

        for ( Pair<Probability, Probability> next : pairs.getRawData() )
        {
            if ( next.getLeft().equals( Probability.ONE ) )
            {
                occurrences++;
            }
            else
            {
                nonOccurrences++;
            }
        }

        return Math.min( occurrences, nonOccurrences );
    }

}
