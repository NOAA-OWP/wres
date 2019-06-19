package wres.engine.statistics.metric.processing;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.DoublePredicate;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticAccessException;
import wres.datamodel.statistics.StatisticException;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.MultiVectorStatistic;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;

/**
 * <p>
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for all configured
 * {@link Threshold}. Typically, this will represent a single forecast lead time within a processing pipeline. The
 * {@link MetricCollection} are computed by calling {@link #apply(Object)}.
 * </p>
 * <p>
 * The current implementation adopts the following simplifying assumptions:
 * </p>
 * <ol>
 * <li>That a global set of {@link Threshold} is defined for all {@link Metric} within a {@link ProjectConfig} and hence
 * {@link MetricCollection}. Using metric-specific thresholds will require additional logic to disaggregate a
 * {@link MetricCollection} into {@link Metric} for which common thresholds are defined.</li>
 * <li>If the {@link Threshold#hasProbabilities()}, the corresponding quantiles are derived from the 
 * observations associated with the {@link SampleData} at runtime, i.e. upon calling
 * {@link #apply(Object)}</li>
 * </ol>
 * <p>
 * Upon construction, the {@link ProjectConfig} is validated to ensure that appropriate {@link Metric} are configured
 * for the type of {@link SampleData} consumed. These metrics are stored in a series of {@link MetricCollection} that
 * consume a given {@link SampleData} and produce a given {@link Statistic}. If the type of {@link SampleData}
 * consumed by any given {@link MetricCollection} differs from the {@link SampleData} for which the
 * {@link MetricProcessor} is primed, a transformation must be applied. For example, {@link Metric} that consume
 * {@link SingleValuedPairs} may be computed for {@link EnsemblePairs} if an appropriate transformation is configured.
 * Subclasses must define and apply any transformation required. If inappropriate {@link SampleData} are provided to
 * {@link #apply(Object)} for the {@link MetricCollection} configured, an unchecked {@link MetricCalculationException}
 * will be thrown. If metrics are configured incorrectly, a checked {@link MetricConfigException} will be thrown.
 * </p>
 * <p>
 * Upon calling {@link #apply(Object)} with a concrete {@link SampleData}, the configured {@link Metric} are computed
 * asynchronously for each {@link Threshold}.
 * </p>
 * <p>
 * The {@link Statistic} are computed and stored by {@link StatisticGroup}. For {@link Statistic} that are not
 * consumed until the end of a processing pipeline, the results from sequential calls to {@link #apply(Object)} may be
 * cached and merged. This is achieved by constructing a {@link MetricProcessor} with a <code>vararg</code> of
 * {@link StatisticGroup} whose results will be cached across successive calls. The merged results are accessible
 * from {@link #getCachedMetricOutput()}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class MetricProcessor<S extends SampleData<?>, T extends StatisticsForProject>
        implements Function<S, T>
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
     * The dall data threshold.
     */

    final Threshold allDataThreshold;

    /**
     * Set of thresholds associated with each metric.
     */

    final ThresholdsByMetric thresholdsByMetric;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link ScoreStatistic}.
     */

    final MetricCollection<SingleValuedPairs, DoubleScoreStatistic, DoubleScoreStatistic> singleValuedScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorStatistic}.
     */

    final MetricCollection<SingleValuedPairs, MultiVectorStatistic, MultiVectorStatistic> singleValuedMultiVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link BoxPlotStatistics}.
     */

    final MetricCollection<SingleValuedPairs, BoxPlotStatistics, BoxPlotStatistics> singleValuedBoxPlot;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScoreStatistic}.
     */

    final MetricCollection<DichotomousPairs, MatrixStatistic, DoubleScoreStatistic> dichotomousScalar;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link MatrixStatistic}.
     */

    final MetricCollection<DichotomousPairs, MatrixStatistic, MatrixStatistic> dichotomousMatrix;

    /**
     * The set of metrics associated with the verification project.
     */

    final Set<MetricConstants> metrics;

    /**
     * An array of {@link StatisticGroup} that should be retained and merged across calls. May be null.
     */

    final Set<StatisticGroup> mergeSet;

    /**
     * An {@link ExecutorService} used to process the thresholds.
     */

    final ExecutorService thresholdExecutor;

    /**
     * The number of decimal places to use when rounding.
     */

    private static final int DECIMALS = 5;

    /**
     * Returns true if a prior call led to the caching of metric outputs.
     * 
     * @return true if stored results are available, false otherwise
     */

    public abstract boolean hasCachedMetricOutput();

    /**
     * Returns the (possibly empty) set of {@link StatisticGroup} that were cached across successive calls to 
     * {@link #apply(Object)}. This may differ from the set of cached outputs that were declared on construction, 
     * because some outputs are cached automatically. For the set of cached outputs declared on construction, 
     * see: {@link #getMetricOutputTypesToCache()}.
     * 
     * @return the output types that were cached
     * @throws InterruptedException if the retrieval was interrupted
     * @throws StatisticException if the output could not be retrieved
     * @throws MetricOutputMergeException if the cached output cannot be merged across calls
     */

    public Set<StatisticGroup> getCachedMetricOutputTypes() throws InterruptedException
    {
        return this.getCachedMetricOutput().getStatisticTypes();
    }

    /**
     * Returns a {@link StatisticsForProject} for the last available results or null if
     * {@link #hasCachedMetricOutput()} returns false.
     * 
     * @return a {@link StatisticsForProject} or null
     * @throws InterruptedException if the retrieval was interrupted
     * @throws StatisticException if the output could not be retrieved
     * @throws MetricOutputMergeException if the cached output cannot be merged across calls
     */

    public T getCachedMetricOutput() throws InterruptedException
    {
        //Complete any end-of-pipeline processing
        this.completeCachedOutput();

        //Return the results
        return this.getCachedMetricOutputInternal();
    }

    /**
     * Returns the (possibly empty) set of {@link StatisticGroup} that will be cached across successive calls to 
     * {@link #apply(Object)}. This contains the set of types to cache that were declared on construction of the 
     * {@link MetricProcessor}. It may differ from the actual set of cached outputs, because some outputs are
     * cached automatically. For the full set of cached outputs, post-computation, 
     * see: {@link #getCachedMetricOutputTypes()}.
     * 
     * @return the output types that will be cached
     */

    public Set<StatisticGroup> getMetricOutputTypesToCache()
    {
        return Collections.unmodifiableSet( new HashSet<>( this.mergeSet ) );
    }

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticGroup}, false
     * otherwise.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticGroup}
     * @return true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticGroup}, false
     *         otherwise
     */

    public boolean hasMetrics( SampleDataGroup inGroup, StatisticGroup outGroup )
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
        return metrics.stream().anyMatch( a -> a.isInGroup( inGroup ) );

    }

    /**
     * Returns true if metrics are available for the input {@link StatisticGroup}, false otherwise.
     * 
     * @param outGroup the {@link StatisticGroup}
     * @return true if metrics are available for the input {@link StatisticGroup} false otherwise
     */

    public boolean hasMetrics( StatisticGroup outGroup )
    {
        return this.metrics.stream().anyMatch( a -> a.isInGroup( outGroup ) );
    }

    /**
     * <p>
     * Returns true if metrics are available that require {@link Threshold}, in order to define a discrete event from a
     * continuous variable, false otherwise. The metrics that require {@link Threshold} belong to one of:
     * </p>
     * <ol>
     * <li>{@link SampleDataGroup#DISCRETE_PROBABILITY}</li>
     * <li>{@link SampleDataGroup#DICHOTOMOUS}</li>
     * <li>{@link SampleDataGroup#MULTICATEGORY}</li>
     * </ol>
     * 
     * @return true if metrics are available that require {@link Threshold}, false otherwise
     */

    public boolean hasThresholdMetrics()
    {
        return this.hasMetrics( SampleDataGroup.DISCRETE_PROBABILITY )
               || this.hasMetrics( SampleDataGroup.MULTICATEGORY )
               || this.hasMetrics( SampleDataGroup.DICHOTOMOUS );
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
     * Completes any processing of cached output at the end of a processing pipeline. This may be required when 
     * computing results that rely on other cached results (e.g. summary statistics). Note that this method may be
     * called more than once.
     * 
     * @throws StatisticAccessException if the cached output cannot be completed because the cached outputs on 
     *            which completion depends cannot be accessed
     */

    abstract void completeCachedOutput() throws InterruptedException;

    /**
     * Returns a {@link StatisticsForProject} for the last available results.
     * 
     * @return a {@link StatisticsForProject} or null
     * @throws MetricOutputMergeException if the outputs cannot be merged across calls
     */

    abstract T getCachedMetricOutputInternal();

    /**
     * Constructor.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @param mergeSet a list of {@link StatisticGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    MetricProcessor( final ProjectConfig config,
                     final ThresholdsByMetric externalThresholds,
                     final ExecutorService thresholdExecutor,
                     final ExecutorService metricExecutor,
                     final Set<StatisticGroup> mergeSet )
            throws MetricParameterException
    {

        Objects.requireNonNull( config, MetricConfigHelper.NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( thresholdExecutor, "Specify a non-null threshold executor service." );

        Objects.requireNonNull( metricExecutor, "Specify a non-null metric executor service." );

        this.metrics = MetricConfigHelper.getMetricsFromConfig( config );

        LOGGER.debug( "Based on the project declaration, the following metrics will be computed: {}.", this.metrics );

        //Construct the metrics that are common to more than one type of input pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ) )
        {
            this.singleValuedScore =
                    MetricFactory.ofSingleValuedScoreCollection( metricExecutor,
                                                                 this.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                                  StatisticGroup.DOUBLE_SCORE ) );

            LOGGER.debug( "Created the single-valued scores for processing. {}", this.singleValuedScore );
        }
        else
        {
            this.singleValuedScore = null;
        }

        // Diagrams
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.MULTIVECTOR ) )
        {
            this.singleValuedMultiVector =
                    MetricFactory.ofSingleValuedMultiVectorCollection( metricExecutor,
                                                                       this.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                                        StatisticGroup.MULTIVECTOR ) );

            LOGGER.debug( "Created the single-valued diagrams for processing. {}", this.singleValuedMultiVector );
        }
        else
        {
            this.singleValuedMultiVector = null;
        }

        //Dichotomous scores
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticGroup.DOUBLE_SCORE ) )
        {
            this.dichotomousScalar =
                    MetricFactory.ofDichotomousScoreCollection( metricExecutor,
                                                                this.getMetrics( SampleDataGroup.DICHOTOMOUS,
                                                                                 StatisticGroup.DOUBLE_SCORE ) );

            LOGGER.debug( "Created the dichotomous scores for processing. {}", this.dichotomousScalar );
        }
        else
        {
            this.dichotomousScalar = null;
        }

        // Contingency table
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticGroup.MATRIX ) )
        {
            this.dichotomousMatrix =
                    MetricFactory.ofDichotomousMatrixCollection( metricExecutor,
                                                                 this.getMetrics( SampleDataGroup.DICHOTOMOUS,
                                                                                  StatisticGroup.MATRIX ) );

            LOGGER.debug( "Created the contingency table metrics for processing. {}", this.dichotomousMatrix );
        }
        else
        {
            this.dichotomousMatrix = null;
        }

        //Box plots
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.BOXPLOT_PER_POOL ) )
        {
            this.singleValuedBoxPlot =
                    MetricFactory.ofSingleValuedBoxPlotCollection( metricExecutor,
                                                                   this.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                                                    StatisticGroup.BOXPLOT_PER_POOL ) );

            LOGGER.debug( "Created the single-valued box plots for processing. {}", this.singleValuedBoxPlot );
        }
        else
        {
            this.singleValuedBoxPlot = null;
        }

        //Set the thresholds: canonical --> metric-local overrides --> global        
        this.thresholdsByMetric = MetricConfigHelper.getThresholdsFromConfig( config, externalThresholds );

        if ( Objects.nonNull( mergeSet ) )
        {
            this.mergeSet = Collections.unmodifiableSet( new HashSet<>( mergeSet ) );
        }
        else
        {
            this.mergeSet = Collections.emptySet();
        }

        //Set the executor for processing thresholds
        this.thresholdExecutor = thresholdExecutor;

        this.allDataThreshold = Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                              Operator.GREATER,
                                              ThresholdDataType.LEFT_AND_RIGHT );

        //Finally, validate the configuration against the parameters set
        this.validate( config );
    }

    /**
     * Returns the thresholds.
     * 
     * @return the thresholds
     */

    ThresholdsByMetric getThresholdsByMetric()
    {
        return this.thresholdsByMetric;
    }

    /**
     * Returns the all data threshold.
     * 
     * @return the all data threshold
     */

    Threshold getAllDataThreshold()
    {
        return this.allDataThreshold;
    }

    /**
     * Returns true if the input list of thresholds contains one or more probability thresholds, false otherwise.
     * 
     * @param check the thresholds to check
     * @return true if the input list contains one or more probability thresholds, false otherwise
     */

    boolean hasProbabilityThreshold( Set<Threshold> check )
    {
        return check.stream().anyMatch( Threshold::hasProbabilities );
    }

    /**
     * Returns a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticGroup}.
     * If the specified {@link SampleDataGroup} is a {@link SampleDataGroup#ENSEMBLE} and this processor is already
     * computing single-valued metrics, then the {@link MetricConstants#SAMPLE_SIZE} is removed from the returned set,
     * in order to avoid duplication, since the {@link MetricConstants#SAMPLE_SIZE} belongs to both groups.
     * 
     * @param inGroup the {@link SampleDataGroup}, may be null
     * @param outGroup the {@link StatisticGroup}, may be null
     * @return a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticGroup}
     *         or an empty array if both inputs are defined and no corresponding metrics are present
     */

    MetricConstants[] getMetrics( SampleDataGroup inGroup,
                                  StatisticGroup outGroup )
    {

        // Unconditional set
        Set<MetricConstants> unconditional = new HashSet<>( this.metrics );

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

        // Return, removing any duplicate sample size instance, if needed
        return unconditional.toArray( new MetricConstants[unconditional.size()] );
    }

    /**
     * Helper that returns a sorted set of values from the left side of the input pairs if any of the thresholds have
     * probabilities associated with them.
     * 
     * @param input the inputs pairs
     * @param thresholds the thresholds to test
     * @return a sorted array of values or null
     */

    double[] getSortedClimatology( SampleData<?> input, Set<Threshold> thresholds )
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

    Threshold addQuantilesToThreshold( Threshold threshold, double[] sorted )
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
