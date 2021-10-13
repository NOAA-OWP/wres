package wres.pipeline.statistics;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.metrics.Metric;
import wres.metrics.MetricCollection;
import wres.metrics.MetricFactory;
import wres.metrics.MetricParameterException;
import wres.statistics.generated.Threshold;

/**
 * Creates statistics by computing {@link Metric} with {@link Pool} and stores them in a {@link StatisticsStore}.
 * 
 * @author James Brown
 */

public abstract class MetricProcessor<S extends Pool<?>> implements Function<S, StatisticsStore>
{
    /**
     * Logger instance.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricProcessor.class );

    /**
     * Filter for admissible numerical data.
     */

    static final DoublePredicate ADMISSABLE_DATA = Double::isFinite;

    /**
     * The all data threshold.
     */

    final ThresholdOuter allDataThreshold;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume single-valued pairs and produce {@link ScoreStatistic}.
     */

    final MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> singleValuedScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume single-valued pairs and produce 
     * {@link DiagramStatisticOuter}.
     */

    final MetricCollection<Pool<Pair<Double, Double>>, DiagramStatisticOuter, DiagramStatisticOuter> singleValuedDiagrams;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume single-valued pairs and produce
     * {@link BoxplotStatisticOuter}.
     */

    final MetricCollection<Pool<Pair<Double, Double>>, BoxplotStatisticOuter, BoxplotStatisticOuter> singleValuedBoxPlot;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume dichotomous pairs and produce {@link ScoreStatistic}.
     */

    final MetricCollection<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> dichotomousScalar;

    /**
     * The metrics to process.
     */

    final ThresholdsByMetricAndFeature metrics;

    /**
     * An {@link ExecutorService} used to process the thresholds.
     */

    final ExecutorService thresholdExecutor;

    /**
     * Returns the metrics to process.
     * 
     * @return the metrics
     */

    public ThresholdsByMetricAndFeature getMetrics()
    {
        return this.metrics;
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

    boolean hasMetrics( SampleDataGroup inGroup, StatisticType outGroup )
    {
        return this.getMetrics()
                   .getThresholdsByMetricAndFeature()
                   .values()
                   .stream()
                   .anyMatch( next -> next.hasMetrics( inGroup, outGroup ) );
    }

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup}, false otherwise.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @return true if metrics are available for the input {@link SampleDataGroup} false otherwise
     */

    boolean hasMetrics( SampleDataGroup inGroup )
    {
        return this.getMetrics()
                   .getThresholdsByMetricAndFeature()
                   .values()
                   .stream()
                   .anyMatch( next -> next.hasMetrics( inGroup ) );
    }

    /**
     * Returns true if metrics are available for the input {@link StatisticType}, false otherwise.
     * 
     * @param outGroup the {@link StatisticType}
     * @return true if metrics are available for the input {@link StatisticType} false otherwise
     */

    boolean hasMetrics( StatisticType outGroup )
    {
        return this.getMetrics()
                   .getThresholdsByMetricAndFeature()
                   .values()
                   .stream()
                   .anyMatch( next -> next.hasMetrics( outGroup ) );
    }

    /**
     * Constructor.
     * 
     * @param metrics the metrics to process
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    MetricProcessor( ThresholdsByMetricAndFeature metrics,
                     ExecutorService thresholdExecutor,
                     ExecutorService metricExecutor )
    {
        Objects.requireNonNull( thresholdExecutor, "Specify a non-null threshold executor service." );

        Objects.requireNonNull( metricExecutor, "Specify a non-null metric executor service." );

        Objects.requireNonNull( metrics, "Specify a non-null collection of metrics to process." );

        this.metrics = metrics;

        LOGGER.debug( "Based on the project declaration, the following metrics will be computed: {}.", this.metrics );

        //Construct the metrics that are common to more than one type of input pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) )
        {
            this.singleValuedScore =
                    MetricFactory.ofSingleValuedScoreCollection( metricExecutor,
                                                                 this.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                                  StatisticType.DOUBLE_SCORE ) );

            LOGGER.debug( "Created the single-valued scores for processing. {}", this.singleValuedScore );
        }
        else
        {
            this.singleValuedScore = null;
        }

        // Diagrams
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DIAGRAM ) )
        {
            this.singleValuedDiagrams =
                    MetricFactory.ofSingleValuedDiagramCollection( metricExecutor,
                                                                   this.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                                    StatisticType.DIAGRAM ) );

            LOGGER.debug( "Created the single-valued diagrams for processing. {}", this.singleValuedDiagrams );
        }
        else
        {
            this.singleValuedDiagrams = null;
        }

        //Dichotomous scores
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ) )
        {
            this.dichotomousScalar =
                    MetricFactory.ofDichotomousScoreCollection( metricExecutor,
                                                                this.getMetrics( SampleDataGroup.DICHOTOMOUS,
                                                                                 StatisticType.DOUBLE_SCORE ) );

            LOGGER.debug( "Created the dichotomous scores for processing. {}", this.dichotomousScalar );
        }
        else
        {
            this.dichotomousScalar = null;
        }

        //Box plots
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ) )
        {
            this.singleValuedBoxPlot =
                    MetricFactory.ofSingleValuedBoxPlotCollection( metricExecutor,
                                                                   this.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                                    StatisticType.BOXPLOT_PER_POOL ) );

            LOGGER.debug( "Created the single-valued box plots for processing. {}", this.singleValuedBoxPlot );
        }
        else
        {
            this.singleValuedBoxPlot = null;
        }

        //Set the executor for processing thresholds
        this.thresholdExecutor = thresholdExecutor;

        this.allDataThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT_AND_RIGHT );
    }

    /**
     * Returns the all data threshold.
     * 
     * @return the all data threshold
     */

    ThresholdOuter getAllDataThreshold()
    {
        return this.allDataThreshold;
    }

    /**
     * Returns true if the input list of thresholds contains one or more probability thresholds, false otherwise.
     * 
     * @param check the thresholds to check
     * @return true if the input list contains one or more probability thresholds, false otherwise
     */

    boolean hasProbabilityThreshold( Set<ThresholdOuter> check )
    {
        return check.stream().anyMatch( ThresholdOuter::hasProbabilities );
    }

    /**
     * Returns a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}.
     * Individual elements of the contingency table are not considered.
     * 
     * @param inGroup the {@link SampleDataGroup}, may be null
     * @param outGroup the {@link StatisticType}, may be null
     * @return a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}
     *         or an empty array if both inputs are defined and no corresponding metrics are present
     */

    MetricConstants[] getMetrics( SampleDataGroup inGroup,
                                  StatisticType outGroup )
    {
        Set<MetricConstants> filtered = this.getMetrics()
                                            .getThresholdsByMetricAndFeature()
                                            .values()
                                            .stream()
                                            .flatMap( next -> next.getMetrics( inGroup, outGroup ).stream() )
                                            .collect( Collectors.toCollection( HashSet::new ) );

        // Remove contingency table elements
        filtered.remove( MetricConstants.TRUE_POSITIVES );
        filtered.remove( MetricConstants.FALSE_POSITIVES );
        filtered.remove( MetricConstants.FALSE_NEGATIVES );
        filtered.remove( MetricConstants.TRUE_NEGATIVES );

        return filtered.toArray( new MetricConstants[filtered.size()] );
    }

    /**
     * Adds the prescribed threshold to the pool metadata.
     * @param pool the pool
     * @param threshold the threshold
     * @return the pool with the input threshold in the metadata
     * @throws NullPointerException if either input is null
     */

    <T> Pool<T> addThresholdToPoolMetadata( Pool<T> pool, OneOrTwoThresholds threshold )
    {
        Objects.requireNonNull( pool );
        Objects.requireNonNull( threshold );

        Pool.Builder<T> builder = new Pool.Builder<T>();
        for ( Pool<T> nextPool : pool.getMiniPools() )
        {
            Pool<T> inner = this.addThresholdToPoolMetadataInner( nextPool, threshold );
            builder.addPool( inner, false );
        }

        return builder.build();
    }

    /**
     * Adds the prescribed threshold to the pool metadata.
     * @param pool the pool
     * @param threshold the threshold
     * @return the pool with the input threshold in the metadata
     */

    private <T> Pool<T> addThresholdToPoolMetadataInner( Pool<T> pool, OneOrTwoThresholds threshold )
    {
        PoolMetadata unadjustedMetadata = pool.getMetadata();
        Threshold eventThreshold = MessageFactory.parse( threshold.first() );
        wres.statistics.generated.Pool.Builder poolBuilder = unadjustedMetadata.getPool()
                                                                               .toBuilder()
                                                                               .setEventThreshold( eventThreshold );

        Threshold decisionThreshold = null;
        if ( threshold.hasTwo() )
        {
            decisionThreshold = MessageFactory.parse( threshold.second() );
            poolBuilder.setDecisionThreshold( decisionThreshold );
        }

        PoolMetadata adjustedMetadata = PoolMetadata.of( unadjustedMetadata.getEvaluation(),
                                                         poolBuilder.build() );
        Pool.Builder<T> builder = new Pool.Builder<T>().addData( pool.get() )
                                                       .setMetadata( adjustedMetadata )
                                                       .setClimatology( pool.getClimatology() );

        if ( pool.hasBaseline() )
        {
            wres.statistics.generated.Pool.Builder baselinePoolBuilder = pool.getBaselineData()
                                                                             .getMetadata()
                                                                             .getPool()
                                                                             .toBuilder()
                                                                             .setEventThreshold( eventThreshold );

            if ( threshold.hasTwo() )
            {
                baselinePoolBuilder.setDecisionThreshold( decisionThreshold );
            }
            PoolMetadata adjustedMetadataForBaseline = PoolMetadata.of( unadjustedMetadata.getEvaluation(),
                                                                        baselinePoolBuilder.build() );
            builder.addDataForBaseline( pool.getBaselineData().get() )
                   .setMetadataForBaseline( adjustedMetadataForBaseline );

        }

        return builder.build();
    }

}
