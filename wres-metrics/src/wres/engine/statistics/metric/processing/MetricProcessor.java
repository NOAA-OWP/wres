package wres.engine.statistics.metric.processing;

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
import wres.config.generated.ProjectConfig;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.ThresholdsByMetricAndFeature;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.Threshold;

/**
 * <p>
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for all configured
 * {@link ThresholdOuter}. Typically, this will represent a single forecast lead time within a processing pipeline. The
 * {@link MetricCollection} are computed by calling {@link #apply(Object)}.
 * </p>
 * <p>
 * The current implementation adopts the following simplifying assumptions:
 * </p>
 * <ol>
 * <li>That a global set of {@link ThresholdOuter} is defined for all {@link Metric} within a {@link ProjectConfig} and 
 * hence {@link MetricCollection}. Using metric-specific thresholds will require additional logic to disaggregate a
 * {@link MetricCollection} into {@link Metric} for which common thresholds are defined.</li>
 * <li>If the {@link ThresholdOuter#hasProbabilities()}, the corresponding quantiles are derived from the 
 * observations associated with the {@link Pool} at runtime, i.e. upon calling
 * {@link #apply(Object)}</li>
 * </ol>
 * <p>
 * Upon construction, the {@link ProjectConfig} is validated to ensure that appropriate {@link Metric} are configured
 * for the type of {@link Pool} consumed. These metrics are stored in a series of {@link MetricCollection} that
 * consume a given {@link Pool} and produce a given {@link Statistic}. If the type of {@link Pool}
 * consumed by any given {@link MetricCollection} differs from the {@link Pool} for which the
 * {@link MetricProcessor} is primed, a transformation must be applied. For example, {@link Metric} that consume
 * single-valued pairs may be computed for ensemble pairs if an appropriate transformation is configured.
 * Subclasses must define and apply any transformation required. If inappropriate {@link Pool} are provided to
 * {@link #apply(Object)} for the {@link MetricCollection} configured, an unchecked {@link MetricCalculationException}
 * will be thrown. If metrics are configured incorrectly, a checked {@link MetricConfigException} will be thrown.
 * </p>
 * <p>
 * Upon calling {@link #apply(Object)} with a concrete {@link Pool}, the configured {@link Metric} are computed
 * asynchronously for each {@link ThresholdOuter}.
 * </p>
 * <p>
 * The {@link Statistic} are computed and stored by {@link StatisticType}.
 * </p>
 * 
 * @author James Brown
 */

public abstract class MetricProcessor<S extends Pool<?>>
        implements Function<S, StatisticsForProject>
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
