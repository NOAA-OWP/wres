package wres.pipeline.statistics;

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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationException;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.metrics.Metric;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricCollection;
import wres.metrics.MetricFactory;
import wres.metrics.MetricParameterException;
import wres.statistics.generated.Pool.EnsembleAverageType;

/**
 * Builds and processes all {@link MetricCollection} associated with an evaluation for metrics that consume ensemble
 * pairs and configured transformations of ensemble pairs. For example, metrics that consume single-valued pairs may be
 * processed after transforming the ensemble pairs with an appropriate mapping function, such as an ensemble mean.
 *
 * @author James Brown
 */

public class EnsembleStatisticsProcessor extends StatisticsProcessor<Pool<TimeSeries<Pair<Double, Ensemble>>>>
{
    /**
     * Logger instance.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( EnsembleStatisticsProcessor.class );

    /**
     * Function that computes an average from an array.
     */

    private static final ToDoubleFunction<double[]> AVERAGE = right -> Arrays.stream( right )
                                                                             .average()
                                                                             .orElse( MissingValues.DOUBLE );

    /**
     * A {@link MetricCollection} of {@link Metric} that consume discrete probability pairs and produce
     * {@link DoubleScoreStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            discreteProbabilityScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume discrete probability pairs and produce
     * {@link ScoreStatistic}.
     */

    private final MetricCollection<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter>
            discreteProbabilityDiagrams;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce {@link DoubleScoreStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ensembleScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce 
     * {@link DoubleScoreStatisticOuter}. This collection does not include any scores that require a reference or 
     * baseline dataset.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ensembleScoreNoBaseline;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce
     * {@link DiagramStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter, DiagramStatisticOuter>
            ensembleDiagrams;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume ensemble pairs and produce
     * {@link BoxplotStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Ensemble>>, BoxplotStatisticOuter, BoxplotStatisticOuter>
            ensembleBoxPlot;

    /**
     * Default function that maps between ensemble pairs and single-valued pairs.
     */

    private final Function<Pair<Double, Ensemble>, Pair<Double, Double>> toSingleValues;

    /**
     * Instance of a single-valued processor for computing metrics from single-valued pairs derived from the ensemble.
     */

    private final SingleValuedStatisticsProcessor singleValuedProcessor;

    /**
     * Ensemble average type.
     */

    private final EnsembleAverageType ensembleAverageType;

    /**
     * Constructor.
     *
     * @param metricsAndThresholds the metrics and thresholds
     * @param slicingExecutor an {@link ExecutorService} for executing slicing and dicing of pools, cannot be null
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @throws DeclarationException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    public EnsembleStatisticsProcessor( MetricsAndThresholds metricsAndThresholds,
                                        ExecutorService slicingExecutor,
                                        ExecutorService metricExecutor )
    {
        super( metricsAndThresholds, slicingExecutor, metricExecutor );

        this.ensembleAverageType = this.getEnsembleAverageType( metricsAndThresholds.ensembleAverageType() );

        // Construct the metrics
        // Discrete probability input, vector output
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE ) )
        {
            this.discreteProbabilityScore =
                    MetricFactory.ofDiscreteProbabilityScores( metricExecutor,
                                                               this.getMetrics( SampleDataGroup.DISCRETE_PROBABILITY,
                                                                                StatisticType.DOUBLE_SCORE ) );

            LOGGER.debug( "Created the discrete probability scores for processing. {}", this.discreteProbabilityScore );
        }
        else
        {
            this.discreteProbabilityScore = null;
        }
        // Discrete probability input, multi-vector output
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DIAGRAM ) )
        {
            this.discreteProbabilityDiagrams =
                    MetricFactory.ofDiscreteProbabilityDiagrams( metricExecutor,
                                                                 this.getMetrics( SampleDataGroup.DISCRETE_PROBABILITY,
                                                                                  StatisticType.DIAGRAM ) );

            LOGGER.debug( "Created the discrete probability diagrams for processing. {}",
                          this.discreteProbabilityDiagrams );
        }
        else
        {
            this.discreteProbabilityDiagrams = null;
        }
        // Ensemble input, score output
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ) )
        {
            MetricConstants[] ensembleMetrics = this.getMetrics( SampleDataGroup.ENSEMBLE,
                                                                 StatisticType.DOUBLE_SCORE );

            this.ensembleScore = MetricFactory.ofEnsembleScores( metricExecutor,
                                                                 ensembleMetrics );

            // Create a set of metrics for when no baseline is available, assuming there is at least one metric
            // But, first, remove the CRPSS, which requires a baseline
            Set<MetricConstants> filteredMetrics = Arrays.stream( ensembleMetrics )
                                                         .filter( next -> next
                                                                          != MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                                                         .collect( Collectors.toSet() );
            if ( !filteredMetrics.isEmpty() )
            {
                MetricConstants[] filteredArray =
                        filteredMetrics.toArray( new MetricConstants[0] );

                this.ensembleScoreNoBaseline = MetricFactory.ofEnsembleScores( metricExecutor,
                                                                               filteredArray );

                LOGGER.debug( "Created the ensemble scores for processing pairs without a baseline. {}",
                              this.ensembleScoreNoBaseline );
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

        // Ensemble input, multi-vector output
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.DIAGRAM ) )
        {
            this.ensembleDiagrams = MetricFactory.ofEnsembleDiagrams( metricExecutor,
                                                                      this.getMetrics( SampleDataGroup.ENSEMBLE,
                                                                                       StatisticType.DIAGRAM ) );

            LOGGER.debug( "Created the ensemble diagrams for processing. {}", this.ensembleDiagrams );
        }
        else
        {
            this.ensembleDiagrams = null;
        }
        // Ensemble input, box-plot output
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE, StatisticType.BOXPLOT_PER_PAIR ) )
        {
            this.ensembleBoxPlot = MetricFactory.ofEnsembleBoxplots( metricExecutor,
                                                                     this.getMetrics( SampleDataGroup.ENSEMBLE,
                                                                                      StatisticType.BOXPLOT_PER_PAIR ) );

            LOGGER.debug( "Created the ensemble box plots for processing. {}", this.ensembleBoxPlot );
        }
        else
        {
            this.ensembleBoxPlot = null;
        }

        this.toSingleValues = this.getSingleValuedTransformer( this.ensembleAverageType );

        // Create a single-valued processor for computing single-valued measures, but first eliminate any metrics that
        // relate to ensemble or probabilistic pairs.
        this.singleValuedProcessor = this.getSingleValuedProcessor( metricsAndThresholds,
                                                                    slicingExecutor,
                                                                    metricExecutor );

        // Finalize validation now all required parameters are available
        // This is also called by the constructor of the superclass, but local parameters must be validated too
        this.validate();
    }

    @Override
    public StatisticsStore apply( Pool<TimeSeries<Pair<Double, Ensemble>>> pool )
    {
        Objects.requireNonNull( pool, "Expected a non-null pool as input to the metric processor." );

        Objects.requireNonNull( pool.getMetadata()
                                    .getTimeWindow(),
                                "Expected a non-null time window in the pool metadata." );

        LOGGER.debug( "Computing ensemble statistics for pool: {}.", pool.getMetadata() );

        // Statistics futures
        StatisticsStore.Builder futures = new StatisticsStore.Builder();

        // Add the ensemble average type used by this processor to the pool metadata.
        String typeName = this.getEnsembleAverageType()
                              .name();
        wres.statistics.generated.Pool.EnsembleAverageType averageType =
                wres.statistics.generated.Pool.EnsembleAverageType.valueOf( typeName );
        UnaryOperator<PoolMetadata> metaMapper = unadjusted -> PoolMetadata.of( unadjusted, averageType );

        // Do the metadata transformation only with an identity function applied to the pooled data
        Pool<TimeSeries<Pair<Double, Ensemble>>> adjustedPool
                = PoolSlicer.transform( pool,
                                        Function.identity(),
                                        unadjusted -> PoolMetadata.of( unadjusted,
                                                                       averageType ) );
        Pool<Pair<Double, Ensemble>> unpacked =
                this.doWorkWithSlicingExecutor( () -> PoolSlicer.unpack( adjustedPool ) );


        LOGGER.debug( "Computing ensemble statistics from {} pairs.",
                      unpacked.get()
                              .size() );

        // Process the metrics that consume ensemble pairs
        if ( this.hasMetrics( SampleDataGroup.ENSEMBLE ) )
        {
            this.processEnsemblePairs( unpacked, futures );
        }

        // Process the metrics that consume discrete probability pairs derived from the ensemble pairs
        if ( this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY ) )
        {
            this.processDiscreteProbabilityPairs( unpacked, futures );
        }

        // Process the metrics that consume dichotomous pairs derived from the ensemble pairs
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) && this.hasDecisionThresholds() )
        {
            LOGGER.debug( "Encountered dichotomous metrics and decision thresholds, which means that dichtomous "
                          + "metrics will be computed for the ensemble pairs." );

            this.processPairsForDichotomousMetrics( unpacked, futures );
        }

        // Process the ensemble result, which do not yet include single-valued metrics
        StatisticsStore results = futures.build();

        // Process the metrics that consume single-valued pairs, which includes any dichotomous metrics derived from 
        // single-valued pairs: #109783. See later for dichotomous metrics produced from ensemble pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED ) || this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            // Derive the single-valued pairs from the ensemble pairs using the configured mapper
            Function<TimeSeries<Pair<Double, Ensemble>>, TimeSeries<Pair<Double, Double>>> mapper =
                    in -> TimeSeriesSlicer.transform( in, this.toSingleValues, null );
            Pool<TimeSeries<Pair<Double, Double>>> singleValued =
                    this.doWorkWithSlicingExecutor( () -> PoolSlicer.transform( pool,
                                                                                mapper,
                                                                                metaMapper ) );  // Add the ensemble average type to the metadata too

            LOGGER.debug( "Computing single-valued statistics for an ensemble forecast from {} time-series.",
                          singleValued.get()
                                      .size() );

            // Compute the results and merge with the ensemble statistics
            StatisticsStore statistics = this.singleValuedProcessor.apply( singleValued );

            results = results.combine( statistics );
        }

        LOGGER.debug( PROCESSING_COMPLETE_MESSAGE,
                      pool.getMetadata()
                          .getFeatureGroup(),
                      pool.getMetadata()
                          .getTimeWindow() );

        return results;
    }

    /**
     * <p>Removes a duplicate instance of the {@link MetricConstants#SAMPLE_SIZE}, which may appear in more than one
     * context.
     *
     * <p>See #65138. 
     *
     * <p>TODO: remove this on completing #65101 and removing the sample size metric
     *
     * @param inGroup the {@link SampleDataGroup}, may be null
     * @param outGroup the {@link StatisticType}, may be null
     * @return an array of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}
     *         or an empty array if both inputs are defined and no corresponding metrics are present
     */

    @Override
    MetricConstants[] getMetrics( SampleDataGroup inGroup, StatisticType outGroup )
    {
        Set<MetricConstants> metrics = new HashSet<>( Arrays.asList( super.getMetrics( inGroup, outGroup ) ) );

        if ( inGroup == SampleDataGroup.SINGLE_VALUED )
        {
            metrics.remove( MetricConstants.SAMPLE_SIZE );
        }

        return metrics.toArray( new MetricConstants[0] );
    }

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticType}, false
     * otherwise.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticType}
     * @return true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticType}, false
     *         otherwise
     */

    @Override
    boolean hasMetrics( SampleDataGroup inGroup, StatisticType outGroup )
    {
        return this.getMetrics( inGroup, outGroup ).length > 0;
    }

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup}, false otherwise.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @return true if metrics are available for the input {@link SampleDataGroup} false otherwise
     */
    @Override
    boolean hasMetrics( SampleDataGroup inGroup )
    {
        return this.getMetrics( inGroup, null ).length > 0;
    }

    /**
     * Returns true if metrics are available for the input {@link StatisticType}, false otherwise.
     *
     * @return true if metrics are available for the input {@link StatisticType} false otherwise
     */
    @Override
    boolean hasDoubleScoreMetrics()
    {
        return this.getMetrics( null, StatisticType.DOUBLE_SCORE ).length > 0;
    }

    /**
     * Helper that returns a predicate for filtering pairs based on the {@link ThresholdOuter#getOrientation()}
     * of the input threshold.
     *
     * @param input the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link ThresholdOuter#getOrientation()} is null
     * @throws IllegalStateException if the {@link ThresholdOuter#getOrientation()} is not recognized
     */

    static Predicate<Pair<Double, Ensemble>> getFilterForEnsemblePairs( ThresholdOuter input )
    {
        return switch ( input.getOrientation() )
                {
                    case LEFT -> Slicer.leftVector( input );
                    case RIGHT -> Slicer.allOfRight( input );
                    case LEFT_AND_RIGHT -> Slicer.leftAndAllOfRight( input );
                    case ANY_RIGHT -> Slicer.anyOfRight( input );
                    case LEFT_AND_ANY_RIGHT -> Slicer.leftAndAnyOfRight( input );
                    case RIGHT_MEAN -> Slicer.right( input, AVERAGE );
                    case LEFT_AND_RIGHT_MEAN -> Slicer.leftAndRight( input, AVERAGE );
                };
    }

    /**
     * <p>Processes a set of metric futures that consume ensemble pairs, which are mapped from the input pairs,
     * ensemble pairs, using a configured mapping function.
     *
     * <p>TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     *
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairs( Pool<Pair<Double, Ensemble>> input, StatisticsStore.Builder futures )
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
     * @return the ensemble averaging method
     */
    private EnsembleAverageType getEnsembleAverageType()
    {
        return this.ensembleAverageType;
    }

    /**
     * Processes all thresholds for metrics that consume ensemble pairs and produce a specified 
     * {@link StatisticType}. 
     *
     * @param pool the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processEnsemblePairsByThreshold( Pool<Pair<Double, Ensemble>> pool,
                                                  StatisticsStore.Builder futures,
                                                  StatisticType outGroup )
    {
        // Filter the thresholds for the feature group associated with this pool and for the required types
        Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = super.getThresholds();

        FeatureGroup featureGroup = pool.getMetadata()
                                        .getFeatureGroup();
        Map<FeatureTuple, Set<ThresholdOuter>> filteredThresholds =
                super.getFilteredThresholds( thresholdsByFeature,
                                             featureGroup,
                                             ThresholdType.PROBABILITY,
                                             ThresholdType.VALUE );

        if ( filteredThresholds.isEmpty() )
        {
            throw new MetricCalculationException( "Could not find any thresholds for feature tuples within feature "
                                                  + "group "
                                                  + featureGroup
                                                  + ". Thresholds were available for these feature tuples: "
                                                  + thresholdsByFeature.keySet()
                                                  + "." );
        }

        // Add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles = ThresholdSlicer.addQuantiles( filteredThresholds,
                                                                                             pool.getClimatology() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );

        // Decompose the thresholds into groups whereby each group contains one "logical" threshold per feature. For
        // example, a logical threshold is a threshold that is consistently named "banana" across all features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Predicate<Pair<Double, Ensemble>>> slicers =
                    ThresholdSlicer.getFiltersFromThresholds( thresholds,
                                                              EnsembleStatisticsProcessor::getFilterForEnsemblePairs );

            // Add the threshold to the pool metadata            
            ThresholdOuter outer = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            OneOrTwoThresholds composed = OneOrTwoThresholds.of( outer );
            UnaryOperator<PoolMetadata> metaTransformer =
                    untransformed -> PoolMetadata.of( untransformed, composed );

            // Decompose by feature
            Map<FeatureTuple, Pool<Pair<Double, Ensemble>>> pools =
                    PoolSlicer.decompose( pool, PoolSlicer.getFeatureMapper() );

            // Filter by threshold using the feature as a hook to tie a pool to a threshold
            Pool<Pair<Double, Ensemble>> sliced =
                    this.doWorkWithSlicingExecutor( () -> PoolSlicer.filter( pools,
                                                                             slicers,
                                                                             pool.getMetadata(),
                                                                             super.getBaselineMetadata( pool ),
                                                                             metaTransformer ) );

            this.processEnsemblePairs( sliced,
                                       futures,
                                       outGroup );
        }
    }

    /**
     * Processes one threshold for metrics that consume ensemble pairs and produce a specified 
     * {@link StatisticType}. 
     *
     * @param pairs the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     */

    private void processEnsemblePairs( Pool<Pair<Double, Ensemble>> pairs,
                                       StatisticsStore.Builder futures,
                                       StatisticType outGroup )
    {
        // Don't waste cpu cycles computing statistics for empty pairs
        if ( pairs.get().isEmpty() )
        {
            LOGGER.debug( "Skipping the calculation of statistics for an empty pool of pairs with metadata {}.",
                          pairs.getMetadata() );

            return;
        }

        if ( outGroup == StatisticType.DOUBLE_SCORE )
        {
            // Baseline pool without a baseline of its own?
            if ( pairs.getMetadata().getPool().getIsBaselinePool() && !pairs.hasBaseline() )
            {
                futures.addDoubleScoreStatistics( this.processEnsemblePairs( pairs, this.ensembleScoreNoBaseline ) );
            }
            else
            {
                futures.addDoubleScoreStatistics( this.processEnsemblePairs( pairs, this.ensembleScore ) );
            }
        }
        else if ( outGroup == StatisticType.DIAGRAM )
        {
            futures.addDiagramStatistics( this.processEnsemblePairs( pairs, this.ensembleDiagrams ) );
        }
        else if ( outGroup == StatisticType.BOXPLOT_PER_PAIR )
        {
            futures.addBoxPlotStatisticsPerPair( this.processEnsemblePairs( pairs, this.ensembleBoxPlot ) );
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
                                                  StatisticsStore.Builder futures )
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

    private void processPairsForDichotomousMetrics( Pool<Pair<Double, Ensemble>> input,
                                                    StatisticsStore.Builder futures )
    {
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ) )
        {
            this.processDichotomousPairsByThreshold( input, futures );
        }
    }

    /**
     * Processes all thresholds for metrics that consume discrete probability pairs for a given {@link StatisticType}. 
     * The discrete probability pairs are produced from ensemble pairs using a configured transformation. 
     *
     * @param pool the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDiscreteProbabilityPairsByThreshold( Pool<Pair<Double, Ensemble>> pool,
                                                             StatisticsStore.Builder futures,
                                                             StatisticType outGroup )
    {
        // Filter the thresholds for the feature group associated with this pool and for the required types
        Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = super.getThresholdsWithoutAllData();
        FeatureGroup featureGroup = pool.getMetadata()
                                        .getFeatureGroup();
        Map<FeatureTuple, Set<ThresholdOuter>> filteredThresholds =
                super.getFilteredThresholds( thresholdsByFeature,
                                             featureGroup,
                                             ThresholdType.PROBABILITY,
                                             ThresholdType.VALUE );

        // Add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles = ThresholdSlicer.addQuantiles( filteredThresholds,
                                                                                             pool.getClimatology() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );

        // Decompose the thresholds into groups whereby each group contains one "logical" threshold per feature. For
        // example, a logical threshold is a threshold that is consistently named "banana" across all features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        //Define a mapper to convert the single-valued pairs to dichotomous pairs
        Function<ThresholdOuter, Function<Pair<Double, Ensemble>, Pair<Probability, Probability>>>
                transformerGenerator =
                threshold -> pair -> Slicer.toDiscreteProbabilityPair( pair, threshold );

        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Function<Pair<Double, Ensemble>, Pair<Probability, Probability>>> transformers =
                    ThresholdSlicer.getTransformersFromThresholds( thresholds,
                                                                   transformerGenerator );

            // Add the threshold to the metadata
            ThresholdOuter outer = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            OneOrTwoThresholds composed = OneOrTwoThresholds.of( outer );
            UnaryOperator<PoolMetadata> metaTransformer = untransformed -> PoolMetadata.of( untransformed, composed );

            // Decompose by feature
            Map<FeatureTuple, Pool<Pair<Double, Ensemble>>> pools = PoolSlicer.decompose( pool,
                                                                                          PoolSlicer.getFeatureMapper() );

            // Transform by threshold using the feature as a hook to tie a pool to a threshold
            Pool<Pair<Probability, Probability>> transformed =
                    this.doWorkWithSlicingExecutor( () -> PoolSlicer.transform( pools,
                                                                                transformers,
                                                                                pool.getMetadata(),
                                                                                super.getBaselineMetadata( pool ),
                                                                                metaTransformer ) );

            this.processDiscreteProbabilityPairs( transformed,
                                                  futures,
                                                  outGroup );
        }
    }

    /**
     * Processes one threshold for metrics that consume discrete probability pairs for a given {@link StatisticType}.
     *
     * @param pairs the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     */

    private void processDiscreteProbabilityPairs( Pool<Pair<Probability, Probability>> pairs,
                                                  StatisticsStore.Builder futures,
                                                  StatisticType outGroup )
    {
        // Don't waste cpu cycles computing statistics for empty pairs
        if ( pairs.get().isEmpty() )
        {
            LOGGER.debug( "Skipping the calculation of statistics for an empty pool of pairs with metadata {}.",
                          pairs.getMetadata() );

            return;
        }

        if ( outGroup == StatisticType.DOUBLE_SCORE )
        {
            futures.addDoubleScoreStatistics( this.processDiscreteProbabilityPairs( pairs,
                                                                                    this.discreteProbabilityScore ) );
        }
        else if ( outGroup == StatisticType.DIAGRAM )
        {
            futures.addDiagramStatistics( this.processDiscreteProbabilityPairs( pairs,
                                                                                this.discreteProbabilityDiagrams ) );
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
        int minimumSampleSize = super.getMinimumSampleSize();

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
        return super.processMetricsRequiredForThisPool( pairs, collection );
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
        int minimumSampleSize = super.getMinimumSampleSize();
        int actualSampleSize = pairs.get().size();

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
                                                      this.getSlicingExecutor() );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        // Are there skill metrics and does the baseline also meet the minimum sample size constraint?
        return super.processMetricsRequiredForThisPool( pairs, collection );
    }

    /**
     * Processes all thresholds for metrics that consume dichotomous pairs for a given {@link StatisticType}. The 
     * dichotomous pairs are produced from the input ensemble pairs using a configured transformation. 
     *
     * @param pool the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( Pool<Pair<Double, Ensemble>> pool,
                                                     StatisticsStore.Builder futures )
    {
        // Filter the thresholds for the feature group associated with this pool and for the required types
        Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = super.getThresholdsWithoutAllData();
        FeatureGroup featureGroup = pool.getMetadata()
                                        .getFeatureGroup();
        Map<FeatureTuple, Set<ThresholdOuter>> filteredThresholds =
                super.getFilteredThresholds( thresholdsByFeature,
                                             featureGroup,
                                             ThresholdType.PROBABILITY,
                                             ThresholdType.VALUE );

        // Classifier thresholds available? If not, nothing to compute
        Map<FeatureTuple, Set<ThresholdOuter>> classifiers =
                super.getFilteredThresholds( thresholdsByFeature,
                                             featureGroup,
                                             ThresholdType.PROBABILITY_CLASSIFIER );

        // Add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles = ThresholdSlicer.addQuantiles( filteredThresholds,
                                                                                             pool.getClimatology() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );

        // Decompose the thresholds into groups whereby each group contains one "logical" threshold per feature. For
        // example, a logical threshold is a threshold that is consistently named "banana" across all features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        // Define a mapper to convert the single-valued pairs to dichotomous pairs
        Function<ThresholdOuter, Function<Pair<Double, Ensemble>, Pair<Probability, Probability>>>
                transformerGenerator =
                threshold -> pair -> Slicer.toDiscreteProbabilityPair( pair, threshold );


        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Function<Pair<Double, Ensemble>, Pair<Probability, Probability>>> transformers =
                    ThresholdSlicer.getTransformersFromThresholds( thresholds,
                                                                   transformerGenerator );

            // Decompose by feature
            Map<FeatureTuple, Pool<Pair<Double, Ensemble>>> pools = PoolSlicer.decompose( pool,
                                                                                          PoolSlicer.getFeatureMapper() );

            // Transform by threshold using the feature as a hook to tie a pool to a threshold
            Pool<Pair<Probability, Probability>> transformed =
                    this.doWorkWithSlicingExecutor( () -> PoolSlicer.transform( pools,
                                                                                transformers,
                                                                                pool.getMetadata(),
                                                                                super.getBaselineMetadata( pool ),
                                                                                meta -> meta ) );

            // Get the composed threshold for the metadata
            ThresholdOuter composedOuter = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );

            List<Map<FeatureTuple, ThresholdOuter>> decomposedInner = ThresholdSlicer.decompose( classifiers );

            // Define a mapper to convert the discrete probability pairs to dichotomous pairs
            Function<ThresholdOuter, Function<Pair<Probability, Probability>, Pair<Boolean, Boolean>>>
                    innerTransformerGenerator =
                    threshold -> pair -> Pair.of( threshold.test( pair.getLeft().getProbability() ),
                                                  threshold.test( pair.getRight().getProbability() ) );

            for ( Map<FeatureTuple, ThresholdOuter> innerThresholds : decomposedInner )
            {
                Map<FeatureTuple, Function<Pair<Probability, Probability>, Pair<Boolean, Boolean>>> innerTransformers =
                        ThresholdSlicer.getTransformersFromThresholds( innerThresholds,
                                                                       innerTransformerGenerator );

                // Add the threshold to the metadata
                ThresholdOuter composedInner = ThresholdSlicer.compose( Set.copyOf( innerThresholds.values() ) );

                OneOrTwoThresholds composed = OneOrTwoThresholds.of( composedOuter, composedInner );
                UnaryOperator<PoolMetadata> metaTransformer =
                        untransformed -> PoolMetadata.of( untransformed, composed );

                // Decompose by feature
                Map<FeatureTuple, Pool<Pair<Probability, Probability>>> innerPools =
                        PoolSlicer.decompose( transformed,
                                              PoolSlicer.getFeatureMapper() );

                // Transform by threshold using the feature as a hook to tie a pool to a threshold
                Pool<Pair<Boolean, Boolean>> dichotomous =
                        this.doWorkWithSlicingExecutor( () -> PoolSlicer.transform( innerPools,
                                                                                    innerTransformers,
                                                                                    transformed.getMetadata(),
                                                                                    super.getBaselineMetadata(
                                                                                            transformed ),
                                                                                    metaTransformer ) );

                super.processDichotomousPairs( dichotomous, futures );
            }
        }
    }

    /**
     * @return whether decision thresholds are available for dichotomous metrics as they apply to ensembles
     */

    private boolean hasDecisionThresholds()
    {
        // Classifier thresholds available? If not, nothing to compute
        return this.getThresholds()
                   .values()
                   .stream()
                   .flatMap( Set::stream )
                   .anyMatch( next -> next.getType() == ThresholdType.PROBABILITY_CLASSIFIER );
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

        for ( Pair<Probability, Probability> next : pairs.get() )
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

    /**
     * @param ensembleAverageType the existing ensemble average type
     * @return the existing type or a default if null
     */

    private EnsembleAverageType getEnsembleAverageType( EnsembleAverageType ensembleAverageType )
    {
        if ( Objects.isNull( ensembleAverageType ) )
        {
            return EnsembleAverageType.MEAN;
        }
        else
        {
            return ensembleAverageType;
        }
    }

    /**
     * Returns a function that transforms ensemble pairs into single-valued pairs. The ensemble average is only
     * computed explicitly if not supplied inband to the {@link Ensemble}.
     * @param averageType the ensemble average type, which is only used if the ensemble average is not already present
     * @return a function to transform an ensemble pair to a single-valued pair
     */

    private Function<Pair<Double, Ensemble>, Pair<Double, Double>> getSingleValuedTransformer( EnsembleAverageType averageType )
    {
        // Construct the default mapper from ensembles to single-values: this is not currently configurable
        // Handle missings here too
        ToDoubleFunction<Ensemble> ensembleMapper = Slicer.getEnsembleAverageFunction( averageType );

        UnaryOperator<Pair<Double, Ensemble>> missingFilter =
                Slicer.leftAndEachOfRight( MissingValues::isNotMissingValue );

        return in -> {
            // Handle missings
            Pair<Double, Ensemble> inWithoutMissings = missingFilter.apply( in );

            // Some data present?
            if ( Objects.nonNull( inWithoutMissings ) )
            {
                double left = inWithoutMissings.getLeft();
                Ensemble members = inWithoutMissings.getRight();
                double right = ensembleMapper.applyAsDouble( members );
                return Pair.of( left, right );
            }

            // No data
            return null;
        };
    }

    /**
     * Validates the internal state of the processor.
     */

    private void validate()
    {
        // This method checks local parameters, so ensure they have been set.
        // If null, this is being called by the superclass constructor, not the local constructor
        if ( Objects.nonNull( this.toSingleValues ) )
        {

            // Thresholds required for dichotomous and probability metrics
            for ( MetricConstants next : super.getMetrics() )
            {
                // Thresholds required for dichotomous metrics
                if ( ( next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                       || next.isInGroup( SampleDataGroup.DISCRETE_PROBABILITY ) )
                     && super.getThresholds()
                             .values()
                             .stream()
                             .flatMap( Set::stream )
                             // Ignore the "all data" threshold in this check
                             .filter( t -> !ThresholdOuter.ALL_DATA.equals( t ) )
                             .noneMatch( threshold -> threshold.getType() == ThresholdType.VALUE
                                                      || threshold.getType() == ThresholdType.PROBABILITY ) )
                {
                    throw new DeclarationException( "Cannot configure '" + next
                                                    + "' without thresholds to define the events: "
                                                    + "add one or more thresholds to the configuration "
                                                    + "for the '"
                                                    + next
                                                    + "'." );
                }
            }
        }
    }

    /**
     * Creates and returns a processor for single-valued statistics, where needed.
     * @param metricsAndThresholds the metrics and thresholds
     * @param thresholdExecutor the thresholds executor
     * @param metricExecutor the metric executor
     * @return the single-valued processor or null
     */

    private SingleValuedStatisticsProcessor getSingleValuedProcessor( MetricsAndThresholds metricsAndThresholds,
                                                                      ExecutorService thresholdExecutor,
                                                                      ExecutorService metricExecutor )
    {
        Set<MetricConstants> singleValuedMetrics = this.getSingleValuedMetrics( metricsAndThresholds.metrics() );
        if ( !singleValuedMetrics.isEmpty() )
        {
            LOGGER.debug( "Discovered the following single-valued metrics for processing: {}.",
                          singleValuedMetrics );
            MetricsAndThresholds singleValuedMetricsAndThresholds =
                    new MetricsAndThresholds( singleValuedMetrics,
                                              metricsAndThresholds.thresholds(),
                                              metricsAndThresholds.minimumSampleSize(),
                                              null );
            return new SingleValuedStatisticsProcessor( singleValuedMetricsAndThresholds,
                                                        thresholdExecutor,
                                                        metricExecutor );
        }
        else
        {
            LOGGER.debug( "Discovered no single-valued metrics for processing." );
            return null;
        }
    }

    /**
     * Returns the single-valued measures.
     * @param metrics metrics
     * @return the single-valued metrics among the inputs
     */

    private Set<MetricConstants> getSingleValuedMetrics( Set<MetricConstants> metrics )
    {
        // Use a negative filter against ensemble-like measures rather than a positive filter for single-valued
        // measures, because some metrics (e.g., sample size) appear in more than one context
        return metrics.stream()
                      .filter( next -> !next.isInGroup( SampleDataGroup.ENSEMBLE )
                                       && !next.isInGroup( SampleDataGroup.DISCRETE_PROBABILITY ) )
                      .collect( Collectors.toSet() );
    }
}
