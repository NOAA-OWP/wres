package wres.engine.statistics.metric.processing;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.DoublePredicate;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.ThresholdConstants.ThresholdDataType;
import wres.datamodel.ThresholdConstants.ThresholdType;
import wres.datamodel.ThresholdsByMetric;
import wres.datamodel.ThresholdsByType;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProject;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScoreOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;
import wres.engine.statistics.metric.config.MetricConfigurationException;

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
 * observations associated with the {@link MetricInput} at runtime, i.e. upon calling
 * {@link #apply(Object)}</li>
 * </ol>
 * <p>
 * Upon construction, the {@link ProjectConfig} is validated to ensure that appropriate {@link Metric} are configured
 * for the type of {@link MetricInput} consumed. These metrics are stored in a series of {@link MetricCollection} that
 * consume a given {@link MetricInput} and produce a given {@link MetricOutput}. If the type of {@link MetricInput}
 * consumed by any given {@link MetricCollection} differs from the {@link MetricInput} for which the
 * {@link MetricProcessor} is primed, a transformation must be applied. For example, {@link Metric} that consume
 * {@link SingleValuedPairs} may be computed for {@link EnsemblePairs} if an appropriate transformation is configured.
 * Subclasses must define and apply any transformation required. If inappropriate {@link MetricInput} are provided to
 * {@link #apply(Object)} for the {@link MetricCollection} configured, an unchecked {@link MetricCalculationException}
 * will be thrown. If metrics are configured incorrectly, a checked {@link MetricConfigurationException} will be thrown.
 * </p>
 * <p>
 * Upon calling {@link #apply(Object)} with a concrete {@link MetricInput}, the configured {@link Metric} are computed
 * asynchronously for each {@link Threshold}.
 * </p>
 * <p>
 * The {@link MetricOutput} are computed and stored by {@link MetricOutputGroup}. For {@link MetricOutput} that are not
 * consumed until the end of a processing pipeline, the results from sequential calls to {@link #apply(Object)} may be
 * cached and merged. This is achieved by constructing a {@link MetricProcessor} with a <code>vararg</code> of
 * {@link MetricOutputGroup} whose results will be cached across successive calls. The merged results are accessible
 * from {@link #getCachedMetricOutput()}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */

public abstract class MetricProcessor<S extends MetricInput<?>, T extends MetricOutputForProject<?>>
        implements Function<S, T>
{

    /**
     * Default logger.
     */

    static final Logger LOGGER = LoggerFactory.getLogger( MetricProcessor.class );

    /**
     * Filter for admissible numerical data.
     */

    static final DoublePredicate ADMISSABLE_DATA = Double::isFinite;

    /**
     * The dall data threshold.
     */

    final Threshold allDataThreshold;

    /**
     * Instance of a {@link MetricFactory}.
     */

    final MetricFactory metricFactory;

    /**
     * Instance of a {@link DataFactory}.
     */

    final DataFactory dataFactory;

    /**
     * Set of thresholds associated with each metric.
     */

    final ThresholdsByMetric thresholdsByMetric;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link ScoreOutput}.
     */

    final MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> singleValuedScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorOutput}.
     */

    final MetricCollection<SingleValuedPairs, MultiVectorOutput, MultiVectorOutput> singleValuedMultiVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScoreOutput}.
     */

    final MetricCollection<DichotomousPairs, MatrixOutput, DoubleScoreOutput> dichotomousScalar;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link MatrixOutput}.
     */

    final MetricCollection<DichotomousPairs, MatrixOutput, MatrixOutput> dichotomousMatrix;

    /**
     * The set of metrics associated with the verification project.
     */

    final Set<MetricConstants> metrics;

    /**
     * An array of {@link MetricOutputGroup} that should be retained and merged across calls. May be null.
     */

    final MetricOutputGroup[] mergeList;

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
     * Returns a {@link MetricOutputForProject} for the last available results or null if
     * {@link #hasCachedMetricOutput()} returns false.
     * 
     * @return a {@link MetricOutputForProject} or null
     * @throws MetricOutputAccessException if the cached output cannot be completed
     */

    public T getCachedMetricOutput() throws MetricOutputAccessException
    {
        //Complete any end-of-pipeline processing
        completeCachedOutput();
        //Return the results
        return getCachedMetricOutputInternal();
    }

    /**
     * Returns true if one or more metric outputs will be cached across successive calls to {@link #apply(Object)},
     * false otherwise.
     * 
     * @return true if results will be cached, false otherwise
     */

    public boolean willCacheMetricOutput()
    {
        return Objects.nonNull( mergeList ) && mergeList.length > 0;
    }

    /**
     * Returns true if a named {@link MetricOutputGroup} will be cached across successive calls to 
     * {@link #apply(Object)}, false otherwise.
     * 
     * @param outputGroup the metric output group
     * @return true if results will be cached for the outputGroup, false otherwise
     */

    public boolean willCacheMetricOutput( MetricOutputGroup outputGroup )
    {
        return Objects.nonNull( mergeList ) && Arrays.stream( mergeList ).anyMatch( a -> a.equals( outputGroup ) );
    }

    /**
     * Returns the (possibly empty) set of {@link MetricOutputGroup} that will be cached across successive calls to 
     * {@link #apply(Object)}.
     * 
     * @return the output types that will be cached
     */

    public Set<MetricOutputGroup> getMetricOutputToCache()
    {
        return Objects.nonNull( mergeList ) ? Collections.unmodifiableSet( new HashSet<>( Arrays.asList( mergeList ) ) )
                                            : Collections.emptySet();
    }

    /**
     * Returns true if metrics are available for the input {@link MetricInputGroup} and {@link MetricOutputGroup}, false
     * otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if metrics are available for the input {@link MetricInputGroup} and {@link MetricOutputGroup}, false
     *         otherwise
     */

    public boolean hasMetrics( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        return getMetrics( metrics, inGroup, outGroup ).length > 0;
    }

    /**
     * Returns true if metrics are available for the input {@link MetricInputGroup}, false otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @return true if metrics are available for the input {@link MetricInputGroup} false otherwise
     */

    public boolean hasMetrics( MetricInputGroup inGroup )
    {
        return metrics.stream().anyMatch( a -> a.isInGroup( inGroup ) );

    }

    /**
     * Returns true if metrics are available for the input {@link MetricOutputGroup}, false otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if metrics are available for the input {@link MetricOutputGroup} false otherwise
     */

    public boolean hasMetrics( MetricOutputGroup outGroup )
    {
        return metrics.stream().anyMatch( a -> a.isInGroup( outGroup ) );
    }

    /**
     * <p>
     * Returns true if metrics are available that require {@link Threshold}, in order to define a discrete event from a
     * continuous variable, false otherwise. The metrics that require {@link Threshold} belong to one of:
     * </p>
     * <ol>
     * <li>{@link MetricInputGroup#DISCRETE_PROBABILITY}</li>
     * <li>{@link MetricInputGroup#DICHOTOMOUS}</li>
     * <li>{@link MetricInputGroup#MULTICATEGORY}</li>
     * </ol>
     * 
     * @return true if metrics are available that require {@link Threshold}, false otherwise
     */

    public boolean hasThresholdMetrics()
    {
        return hasMetrics( MetricInputGroup.DISCRETE_PROBABILITY ) || hasMetrics( MetricInputGroup.DICHOTOMOUS )
               || hasMetrics( MetricInputGroup.MULTICATEGORY );
    }

    /**
     * Validates the configuration and throws a {@link MetricConfigurationException} if the configuration is invalid.
     * When validating parameters that are set locally, ensure that: 1) this method is called on completion of the 
     * subclass constructor; and 2) that it checks for the presence of local parameters, because this method is 
     * initially called within the superclass constructor, i.e. before any local parameters have been set.
     * 
     * @param config the configuration to validate
     * @throws MetricConfigurationException if the configuration is invalid
     */

    abstract void validate( ProjectConfig config ) throws MetricConfigurationException;

    /**
     * Completes any processing of cached output at the end of a processing pipeline. This may be required when 
     * computing results that rely on other cached results (e.g. summary statistics). 
     * 
     * @throws MetricOutputAccessException if the cached output cannot be completed because the cached outputs on 
     *            which completion depends cannot be accessed
     */

    abstract void completeCachedOutput() throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputForProject} for the last available results or null if
     * {@link #hasCachedMetricOutput()} returns false.
     * 
     * @return a {@link MetricOutputForProject} or null
     */

    abstract T getCachedMetricOutputInternal();

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
     *            {@link #apply(Object)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    MetricProcessor( final DataFactory dataFactory,
                     final ProjectConfig config,
                     final Map<MetricConfigName, ThresholdsByType> externalThresholds,
                     final ExecutorService thresholdExecutor,
                     final ExecutorService metricExecutor,
                     final MetricOutputGroup... mergeList )
            throws MetricConfigurationException, MetricParameterException
    {

        Objects.requireNonNull( config, MetricConfigHelper.NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataFactory, MetricConfigHelper.NULL_DATA_FACTORY_ERROR );

        this.dataFactory = dataFactory;
        metrics = MetricConfigHelper.getMetricsFromConfig( config );
        metricFactory = MetricFactory.getInstance( dataFactory );

        //Construct the metrics that are common to more than one type of input pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            singleValuedScore =
                    metricFactory.ofSingleValuedScoreCollection( metricExecutor,
                                                                 getMetrics( metrics,
                                                                             MetricInputGroup.SINGLE_VALUED,
                                                                             MetricOutputGroup.DOUBLE_SCORE ) );
        }
        else
        {
            singleValuedScore = null;
        }
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR ) )
        {
            singleValuedMultiVector =
                    metricFactory.ofSingleValuedMultiVectorCollection( metricExecutor,
                                                                       getMetrics( metrics,
                                                                                   MetricInputGroup.SINGLE_VALUED,
                                                                                   MetricOutputGroup.MULTIVECTOR ) );
        }
        else
        {
            singleValuedMultiVector = null;
        }

        //Dichotomous scores
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            dichotomousScalar =
                    metricFactory.ofDichotomousScoreCollection( metricExecutor,
                                                                getMetrics( metrics,
                                                                            MetricInputGroup.DICHOTOMOUS,
                                                                            MetricOutputGroup.DOUBLE_SCORE ) );
        }
        else
        {
            dichotomousScalar = null;
        }
        // Contingency table
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.MATRIX ) )
        {
            dichotomousMatrix =
                    metricFactory.ofDichotomousMatrixCollection( metricExecutor,
                                                                 getMetrics( metrics,
                                                                             MetricInputGroup.DICHOTOMOUS,
                                                                             MetricOutputGroup.MATRIX ) );
        }
        else
        {
            dichotomousMatrix = null;
        }

        //Set the thresholds: canonical --> metric-local overrides --> global        
        thresholdsByMetric = MetricConfigHelper.getThresholdsFromConfig( config, dataFactory, externalThresholds );

        this.mergeList = mergeList;

        //Set the executor for processing thresholds
        if ( Objects.nonNull( thresholdExecutor ) )
        {
            this.thresholdExecutor = thresholdExecutor;
        }
        else
        {
            this.thresholdExecutor = ForkJoinPool.commonPool();
        }

        this.allDataThreshold = dataFactory.ofThreshold( dataFactory.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.ALL );

        //Finally, validate the configuration against the parameters set
        validate( config );
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
     * Returns a set of {@link MetricConstants} for a specified {@link MetricInputGroup} and {@link MetricOutputGroup}.
     * If the specified {@link MetricInputGroup} is a {@link MetricInputGroup#ENSEMBLE} and this processor is already
     * computing single-valued metrics, then the {@link MetricConstants#SAMPLE_SIZE} is removed from the returned set,
     * in order to avoid duplication, since the {@link MetricConstants#SAMPLE_SIZE} belongs to both groups.
     * 
     * @param input the input constants
     * @param inGroup the {@link MetricInputGroup}, may be null
     * @param outGroup the {@link MetricOutputGroup}, may be null
     * @return a set of {@link MetricConstants} for a specified {@link MetricInputGroup} and {@link MetricOutputGroup}
     *         or an empty array if both inputs are defined and no corresponding metrics are present
     */

    MetricConstants[] getMetrics( Set<MetricConstants> input,
                                  MetricInputGroup inGroup,
                                  MetricOutputGroup outGroup )
    {
        Objects.requireNonNull( input, "Specify a non-null array of metric identifiers from which to select metrics." );

        // Unconditional set
        Set<MetricConstants> metrics = new HashSet<>( input );

        // Remove metrics not in the input group
        if ( Objects.nonNull( inGroup ) )
        {
            metrics.removeIf( a -> !a.isInGroup( inGroup ) );
        }
        // REmove metrics not in the output group
        if ( Objects.nonNull( outGroup ) )
        {
            metrics.removeIf( a -> !a.isInGroup( outGroup ) );
        }
        //Remove duplicate sample size
        if ( inGroup == MetricInputGroup.ENSEMBLE && hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            metrics.remove( MetricConstants.SAMPLE_SIZE );
        }
        return metrics.toArray( new MetricConstants[metrics.size()] );
    }

    /**
     * Helper that returns a sorted set of values from the left side of the input pairs if any of the thresholds have
     * probabilities associated with them.
     * 
     * @param input the inputs pairs
     * @param thresholds the thresholds to test
     * @return a sorted array of values or null
     */

    double[] getSortedClimatology( PairedInput<?> input, Set<Threshold> thresholds )
    {
        double[] sorted = null;
        if ( hasProbabilityThreshold( thresholds ) && input.hasClimatology() )
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

        return dataFactory.getSlicer().getQuantileFromProbability( threshold,
                                                                   sorted,
                                                                   DECIMALS );
    }

}
