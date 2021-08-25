package wres.engine.statistics.metric.processing;

import java.util.Arrays;
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
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.Metrics;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdType;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;

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

    final Metrics metrics;

    /**
     * An {@link ExecutorService} used to process the thresholds.
     */

    final ExecutorService thresholdExecutor;

    /**
     * The number of decimal places to use when rounding.
     */

    private static final int DECIMALS = 5;

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticType}, false
     * otherwise.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticType}
     * @return true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticType}, false
     *         otherwise
     */

    public boolean hasMetrics( SampleDataGroup inGroup, StatisticType outGroup )
    {
        return this.getMetrics( inGroup, outGroup ).length > 0;
    }

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup}, false otherwise.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @return true if metrics are available for the input {@link SampleDataGroup} false otherwise
     */

    public boolean hasMetrics( SampleDataGroup inGroup )
    {
        return this.metrics.getMetrics()
                           .stream()
                           .anyMatch( a -> a.isInGroup( inGroup ) );

    }

    /**
     * Returns true if metrics are available for the input {@link StatisticType}, false otherwise.
     * 
     * @param outGroup the {@link StatisticType}
     * @return true if metrics are available for the input {@link StatisticType} false otherwise
     */

    public boolean hasMetrics( StatisticType outGroup )
    {
        return this.metrics.getMetrics()
                           .stream()
                           .anyMatch( a -> a.isInGroup( outGroup ) );
    }

    /**
     * <p>
     * Returns true if metrics are available that require {@link ThresholdOuter}, in order to define a discrete event from a
     * continuous variable, false otherwise. The metrics that require {@link ThresholdOuter} belong to one of:
     * </p>
     * <ol>
     * <li>{@link SampleDataGroup#DISCRETE_PROBABILITY}</li>
     * <li>{@link SampleDataGroup#DICHOTOMOUS}</li>
     * <li>{@link SampleDataGroup#MULTICATEGORY}</li>
     * </ol>
     * 
     * @return true if metrics are available that require {@link ThresholdOuter}, false otherwise
     */

    public boolean hasThresholdMetrics()
    {
        return this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY )
               || this.hasMetrics( SampleDataGroup.MULTICATEGORY )
               || this.hasMetrics( SampleDataGroup.DICHOTOMOUS );
    }

    /**
     * Returns the metrics to process.
     * 
     * @return the metrics
     */

    public Metrics getMetrics()
    {
        return this.metrics;
    }

    /**
     * Validates the configuration and throws a {@link MetricConfigException} if the configuration is invalid.
     * When validating parameters that are set locally, ensure that: 1) this method is called on completion of the 
     * subclass constructor; and 2) that it checks for the presence of local parameters, because this method is 
     * initially called within the superclass constructor, i.e. before any local parameters have been set.
     * 
     * @param config the configuration to validate
     * @throws MetricConfigException if the configuration is invalid
     */

    abstract void validate( ProjectConfig config );

    /**
     * Constructor.
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    MetricProcessor( final ProjectConfig config,
                     final Metrics metrics,
                     final ExecutorService thresholdExecutor,
                     final ExecutorService metricExecutor )
    {

        Objects.requireNonNull( config, MetricConfigHelper.NULL_CONFIGURATION_ERROR );

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

        //Finally, validate the configuration against the parameters set
        this.validate( config );
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

        // Unconditional set
        Set<MetricConstants> unconditional = new HashSet<>( this.metrics.getMetrics() );

        // Remove metrics not in the input group
        if ( Objects.nonNull( inGroup ) )
        {
            unconditional.removeIf( a -> !a.isInGroup( inGroup ) );
        }

        // Remove metrics not in the output group
        if ( Objects.nonNull( outGroup ) )
        {
            unconditional.removeIf( a -> !a.isInGroup( outGroup ) );
        }

        // Remove contingency table elements
        unconditional.remove( MetricConstants.TRUE_POSITIVES );
        unconditional.remove( MetricConstants.FALSE_POSITIVES );
        unconditional.remove( MetricConstants.FALSE_NEGATIVES );
        unconditional.remove( MetricConstants.TRUE_NEGATIVES );

        return unconditional.toArray( new MetricConstants[unconditional.size()] );
    }

    /**
     * Adds quantiles to the input thresholds and filters out any thresholds that differ by probability values only.
     * @param input the pool
     * @param thresholds the thresholds
     * @return the filtered quantile thresholds
     */

    Set<ThresholdOuter> getUniqueThresholdsWithQuantiles( Pool<?> pool, Set<ThresholdOuter> thresholds )
    {
        // Find the union across metrics and filter out non-unique thresholds
        double[] sorted = this.getSortedClimatology( pool, thresholds );
        Set<ThresholdOuter> returnMe = thresholds.stream()
                                                 .map( next -> this.addQuantilesToThreshold( next, sorted ) )
                                                 .collect( Collectors.toUnmodifiableSet() );
        return Slicer.filter( returnMe );
    }

    /**
     * <p>Helper that inspects the {@link Pool#getClimatology()} and returns a sorted set of values when the 
     * following two conditions are both met, otherwise <code>null</code>:
     * 
     * <ol>
     * <li>The {@link Pool#hasClimatology()} returns <code>true</code>; and</li>
     * <li>One or more of the input thresholds is a probability threshold according to 
     * {@link ThresholdOuter#hasProbabilities()}.</li>
     * </ol>
     * 
     * @param input the inputs pairs
     * @param thresholds the thresholds to test
     * @return a sorted array of values or null
     */

    private double[] getSortedClimatology( Pool<?> input, Set<ThresholdOuter> thresholds )
    {
        double[] sorted = null;
        if ( this.hasProbabilityThreshold( thresholds ) && input.hasClimatology() )
        {
            sorted = input.getClimatology().getDoubles();
            Arrays.sort( sorted );
        }
        return sorted;
    }

    /**
     * Adds the quantile values to the input threshold if the threshold contains probability values. This method is
     * lenient with regard to the input type, returning the input threshold if it is not a 
     * {@link ThresholdType#PROBABILITY_ONLY} type.
     * 
     * @param threshold the input threshold
     * @param sorted a sorted set of values from which to determine the quantiles
     * @return the threshold with quantiles added, if required
     * @throws MetricCalculationException if the sorted array is null and quantiles are required
     */

    private ThresholdOuter addQuantilesToThreshold( ThresholdOuter threshold, double[] sorted )
    {
        if ( threshold.getType() != ThresholdType.PROBABILITY_ONLY )
        {
            return threshold;
        }
        if ( Objects.isNull( sorted ) )
        {
            throw new MetricCalculationException( "Unable to determine quantile threshold from probability "
                                                  + "threshold: no climatological observations were available in "
                                                  + "the input." );
        }

        return Slicer.getQuantileFromProbability( threshold,
                                                  sorted,
                                                  DECIMALS );
    }

}
