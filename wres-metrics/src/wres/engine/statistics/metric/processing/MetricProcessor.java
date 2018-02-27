package wres.engine.statistics.metric.processing;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
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
 * <li>If the {@link Threshold#hasProbabilityValues()}, the corresponding quantiles are derived from the 
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
     * Instance of a {@link MetricFactory}.
     */

    final MetricFactory metricFactory;

    /**
     * Instance of a {@link DataFactory}.
     */

    final DataFactory dataFactory;

    /**
     * Set of thresholds associated with specific metrics that override {@link #thresholds}
     */

    final EnumMap<MetricConstants, Set<Threshold>> thresholdOverrides;

    /**
     * Set of thresholds that apply each group of metrics.
     */

    final Map<Pair<MetricInputGroup, MetricOutputGroup>, Set<Threshold>> thresholds;

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
        return getSelectedMetrics( metrics, inGroup, outGroup ).length > 0;
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
                     final ExecutorService thresholdExecutor,
                     final ExecutorService metricExecutor,
                     final MetricOutputGroup... mergeList )
            throws MetricConfigurationException, MetricParameterException
    {
        Objects.requireNonNull( config,
                                "Specify a non-null project configuration from which to construct the metric processor." );
        Objects.requireNonNull( dataFactory,
                                "Specify a non-null data factory from which to construct the metric processor." );
        this.dataFactory = dataFactory;
        metrics = MetricConfigHelper.getMetricsFromConfig( config );
        metricFactory = MetricFactory.getInstance( dataFactory );
        //Construct the metrics that are common to more than one type of input pairs

        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCORE ) )
        {
            singleValuedScore =
                    metricFactory.ofSingleValuedScoreCollection( metricExecutor,
                                                                 getSelectedMetrics( metrics,
                                                                                     MetricInputGroup.SINGLE_VALUED,
                                                                                     MetricOutputGroup.SCORE ) );
        }
        else
        {
            singleValuedScore = null;
        }
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR ) )
        {
            singleValuedMultiVector =
                    metricFactory.ofSingleValuedMultiVectorCollection( metricExecutor,
                                                                       getSelectedMetrics( metrics,
                                                                                           MetricInputGroup.SINGLE_VALUED,
                                                                                           MetricOutputGroup.MULTIVECTOR ) );
        }
        else
        {
            singleValuedMultiVector = null;
        }

        //Obtain the thresholds for each metric and store them
        thresholdOverrides = new EnumMap<>( MetricConstants.class );
        thresholds = new HashMap<>();
        setThresholds( config );
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

        //Finally, validate the configuration against the parameters set
        validate( config );
    }

    /**
     * Returns true if the input list of thresholds contains one or more probability thresholds, false otherwise.
     * 
     * @param check the thresholds to check
     * @return true if the input list contains one or more probability thresholds, false otherwise
     */

    boolean hasProbabilityThreshold( Set<Threshold> check )
    {
        return check.stream().anyMatch( Threshold::hasProbabilityValues );
    }

    /**
     * Returns a set of {@link MetricConstants} for a specified {@link MetricInputGroup} and {@link MetricOutputGroup}.
     * If the specified {@link MetricInputGroup} is a {@link MetricInputGroup#ENSEMBLE} and this processor is already
     * computing single-valued metrics, then the {@link MetricConstants#SAMPLE_SIZE} is removed from the returned set,
     * in order to avoid duplication, since the {@link MetricConstants#SAMPLE_SIZE} belongs to both groups.
     * 
     * @param input the input constants
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return a set of {@link MetricConstants} for a specified {@link MetricInputGroup} and {@link MetricOutputGroup}
     *         or an empty array
     */

    MetricConstants[] getSelectedMetrics( Set<MetricConstants> input,
                                          MetricInputGroup inGroup,
                                          MetricOutputGroup outGroup )
    {
        Objects.requireNonNull( input, "Specify a non-null array of metric identifiers from which to select metrics." );
        //Find the matching metrics 
        Set<MetricConstants> metrics = MetricConstants.getMetrics( inGroup, outGroup );
        metrics.removeIf( a -> !input.contains( a ) );
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
     * Sets the quantile values associated with the input threshold if the threshold contains probability values.
     * 
     * @param threshold the input threshold
     * @param sorted a sorted set of values from which to determine the quantiles
     * @return the threshold
     * @throws MetricCalculationException if the sorted array is null and quantiles are required
     */

    Threshold getThreshold( Threshold threshold, double[] sorted )
    {
        Threshold useMe = threshold;
        //Quantile required: need to determine real-value from probability
        if ( threshold.hasProbabilityValues() )
        {
            if ( Objects.isNull( sorted ) )
            {
                throw new MetricCalculationException( "Unable to determine quantile threshold from probability "
                                                      + "threshold: no climatological observations were available in "
                                                      + "the input." );
            }
            useMe = dataFactory.getSlicer().getQuantileFromProbability( useMe,
                                                                        sorted,
                                                                        DECIMALS );
        }
        return useMe;
    }

    /**
     * Returns the thresholds to process for a specific combination of {@link MetricInputGroup} and 
     * {@link MetricOutputGroup}. Returns the union of the global thresholds and any metric-local thresholds 
     * for metrics that belong to the specified group. Also see: 
     * {@link #doNotComputeTheseMetricsForThisThreshold(wres.datamodel.MetricConstants.MetricInputGroup, 
     * wres.datamodel.MetricConstants.MetricOutputGroup, wres.datamodel.Threshold)}
     * 
     * @param inGroup the input group
     * @param outGroup the output group
     * @return the thresholds to process 
     * @throws MetricCalculationException if no thresholds exist
     */

    Set<Threshold> getThresholds( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        Set<Threshold> returnMe = new HashSet<>( thresholds.get( Pair.of( inGroup, outGroup ) ) );
        if ( returnMe.isEmpty() )
        {
            throw new MetricCalculationException( "Could not identify thresholds for '" + inGroup
                                                  + "' and "
                                                  + "'"
                                                  + outGroup
                                                  + "'." );
        }
        //Add any metric-local thresholds for this group
        returnMe.addAll( getThresholdOverrides( inGroup, outGroup ) );
        return returnMe;
    }

    /**
     * Returns a list of metrics for the prescribed {@link MetricInputGroup} and {@link MetricOutputGroup} that 
     * should not be computed for the specified threshold. 
     * 
     * @param inGroup the input group
     * @param outGroup the output group
     * @param threshold the threshold
     * @return the list of metrics within the specified inGroup and outGroup that should not be computed
     */

    Set<MetricConstants> doNotComputeTheseMetricsForThisThreshold( MetricInputGroup inGroup,
                                                                   MetricOutputGroup outGroup,
                                                                   Threshold threshold )
    {
        //Begin by assuming that all metrics within this group will be computed
        Set<MetricConstants> returnMe = new HashSet<>();
        //Find the global thresholds for this group
        Set<Threshold> byGroup = thresholds.get( Pair.of( inGroup, outGroup ) );
        //Are there threshold overrides for this group?
        //Yes: add all metrics within this group to the ignore list unless a metric does not have overrides or 
        //this threshold is defined as an override
        if ( hasThresholdOverrides( inGroup, outGroup ) )
        {
            //Obtain the unconditional set of metrics for this group
            Set<MetricConstants> fullSet =
                    new HashSet<>( Arrays.asList( getSelectedMetrics( metrics, inGroup, outGroup ) ) );
            //Ignore all metrics in this group unless proven otherwise
            Set<MetricConstants> ignoreMe = new HashSet<>( fullSet );
            //If the current threshold is within the list of global thresholds for this group, any metrics within this 
            //group that do not have overrides may be computed. Eliminate them from the ignore list.
            if ( byGroup.contains( threshold ) )
            {
                ignoreMe.removeIf( a -> !thresholdOverrides.containsKey( a ) );
            }
            //Next, handle cases where overrides are defined and this threshold is within the override list
            Set<MetricConstants> overriden = getMetricsWithOverridesForThisThreshold( fullSet, threshold );
            //Remove them from the ignore list if they are overriden
            ignoreMe.removeIf( overriden::contains );
            //Add the rest
            returnMe.addAll( ignoreMe );
        }
        //No: ignore all metrics within the group if the current threshold is not within the global thresholds
        else
        {
            if ( !byGroup.contains( threshold ) )
            {
                returnMe.addAll( Arrays.asList( getSelectedMetrics( metrics, inGroup, outGroup ) ) );
            }
        }
        return returnMe;
    }

    /**
     * Sets the thresholds for each metric in the configuration, including any thresholds that apply globally (to all
     * metrics).
     * 
     * @param config the project configuration
     * @throws MetricConfigurationException if thresholds are configured incorrectly
     */

    private void setThresholds( ProjectConfig config ) throws MetricConfigurationException
    {
        //Validate the configuration
        MetricsConfig metrics = config.getMetrics();
        validateOutputsConfig( metrics );
        //Check for metric-local thresholds and throw an exception if they are defined, as they are currently not supported
        for ( MetricConfig metric : metrics.getMetric() )
        {
            if ( metric.getName() != MetricConfigName.ALL_VALID )
            {
                Set<Threshold> thresholds = new HashSet<>();
                //Add probability thresholds
                if ( Objects.nonNull( metric.getProbabilityThresholds() ) )
                {
                    Operator oper = MetricConfigHelper.from( metric.getProbabilityThresholds().getOperator() );
                    String values = metric.getProbabilityThresholds().getCommaSeparatedValues();
                    thresholds.addAll( getThresholdsFromCommaSeparatedValues( values, oper, true ) );

                }
                //Add real-valued thresholds
                if ( Objects.nonNull( metric.getValueThresholds() ) )
                {
                    Operator oper = MetricConfigHelper.from( metric.getValueThresholds().getOperator() );
                    String values = metric.getValueThresholds().getCommaSeparatedValues();
                    thresholds.addAll( getThresholdsFromCommaSeparatedValues( values, oper, false ) );
                }
                if ( !thresholds.isEmpty() )
                {
                    thresholdOverrides.put( MetricConfigHelper.from( metric.getName() ), thresholds );
                }
            }
        }
        setThresholdsForAllGroups( metrics );
    }

    /**
     * Returns <code>true</code> if one or more metrics in the specified input and output groups have override 
     * thresholds defined, <code>false</code> otherwise.
     * 
     * @param inGroup the input group 
     * @param outGroup the output group
     * @return true if metric-local thresholds are defined for some metrics in the specified groups, false otherwise
     */

    private boolean hasThresholdOverrides( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        return thresholdOverrides.keySet().stream().anyMatch( a -> a.isInGroup( inGroup ) && a.isInGroup( outGroup ) );
    }

    /**
     * Returns the union of thresholds across metrics within the specified group that have override thresholds defined.
     * 
     * @param inGroup the input group 
     * @param outGroup the output group
     * @return the union of override thresholds for the input group
     */

    private Set<Threshold> getThresholdOverrides( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        Set<Threshold> returnMe = new HashSet<>();
        thresholdOverrides.forEach( ( key, value ) -> {
            if ( key.isInGroup( inGroup ) && key.isInGroup( outGroup ) )
            {
                returnMe.addAll( value );
            }
        } );
        return returnMe;
    }

    /**
     * Returns the subset of metrics within the specified group for which the input threshold is an override 
     * threshold.
     * 
     * @param group the group of metrics to search
     * @param override the override threshold
     * @return the subset of metrics from the input group for which the input threshold is an override threshold
     * @throws NullPointerException if either input is null
     */

    private Set<MetricConstants> getMetricsWithOverridesForThisThreshold( Set<MetricConstants> group,
                                                                          Threshold override )
    {
        Objects.requireNonNull( group, "Specify a non-null input group to search for threshold overrides." );
        Objects.requireNonNull( override,
                                "Specify a non-null input threshold to search the list of override thresholds." );
        Set<MetricConstants> returnMe = new HashSet<>();
        thresholdOverrides.forEach( ( key, value ) -> {
            if ( group.contains( key ) && value.contains( override ) )
            {
                returnMe.add( key );
            }
        } );
        return returnMe;
    }

    /**
     * Sets the global thresholds that apply to all metrics within a given group, unless metric-local overrides are 
     * provided.
     * 
     * @param metrics the metric configuration
     * @throws MetricConfigurationException if thresholds are configured incorrectly
     */

    private void setThresholdsForAllGroups( MetricsConfig metrics ) throws MetricConfigurationException
    {

        Set<Threshold> thresholdsWithAllData = new HashSet<>();
        Set<Threshold> thresholdsWithoutAllData = new HashSet<>();
        Set<Threshold> thresholdsWithAllDataOnly = new HashSet<>();

        //Add a threshold for "all data" by default
        Threshold allData = dataFactory.ofThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER );
        thresholdsWithAllData.add( allData );
        thresholdsWithAllDataOnly.add( allData );
        //Add probability thresholds
        if ( Objects.nonNull( metrics.getProbabilityThresholds() ) )
        {
            Operator oper = MetricConfigHelper.from( metrics.getProbabilityThresholds().getOperator() );
            String values = metrics.getProbabilityThresholds().getCommaSeparatedValues();
            Set<Threshold> thresholds = getThresholdsFromCommaSeparatedValues( values, oper, true );
            thresholdsWithoutAllData.addAll( thresholds );
            thresholdsWithAllData.addAll( thresholds );
        }
        //Add real-valued thresholds
        if ( Objects.nonNull( metrics.getValueThresholds() ) )
        {
            Operator oper = MetricConfigHelper.from( metrics.getValueThresholds().getOperator() );
            String values = metrics.getValueThresholds().getCommaSeparatedValues();
            Set<Threshold> thresholds = getThresholdsFromCommaSeparatedValues( values, oper, false );
            thresholdsWithoutAllData.addAll( thresholds );
            thresholdsWithAllData.addAll( thresholds );
        }

        //Set the global thresholds, iterating through the candidate groups
        for ( MetricInputGroup inGroup : MetricInputGroup.values() )
        {
            for ( MetricOutputGroup outGroup : MetricOutputGroup.values() )
            {
                setThresholdsForOneGroup( inGroup,
                                          outGroup,
                                          thresholdsWithAllData,
                                          thresholdsWithoutAllData,
                                          thresholdsWithAllDataOnly );
            }
        }
    }

    /**
     * Sets the global thresholds that apply to all metrics for a specific combination of {@link MetricInputGroup} and
     * {@link MetricOutputGroup}.
     * 
     * @param inGroup the input group
     * @param outGroup the output group
     * @param thresholdsWithAllData the thresholds that include the all data threshold
     * @param thresholdsWithoutAllData the thresholds that do not include the all data threshold
     * @param thresholdsWithAllDataOnly the thresholds that include only the all data threshold
     */

    private void setThresholdsForOneGroup( MetricInputGroup inGroup,
                                           MetricOutputGroup outGroup,
                                           Set<Threshold> thresholdsWithAllData,
                                           Set<Threshold> thresholdsWithoutAllData,
                                           Set<Threshold> thresholdsWithAllDataOnly )
    {
        if ( inGroup == MetricInputGroup.ENSEMBLE )
        {
            //For box plots, only consider "all data"
            if ( outGroup == MetricOutputGroup.BOXPLOT )
            {
                this.thresholds.put( Pair.of( inGroup, outGroup ), thresholdsWithAllDataOnly );
            }
            else
            {
                this.thresholds.put( Pair.of( inGroup, outGroup ), thresholdsWithAllData );
            }
        }
        else if ( inGroup == MetricInputGroup.SINGLE_VALUED )
        {
            //For the QQ diagram, only consider "all data"
            //TODO: override with metric-local thresholds if future metrics within this group are allowed
            //for other thresholds - currently this includes the QQ diagram only
            if ( outGroup == MetricOutputGroup.MULTIVECTOR )
            {
                this.thresholds.put( Pair.of( inGroup, outGroup ), thresholdsWithAllDataOnly );
            }
            else
            {
                this.thresholds.put( Pair.of( inGroup, outGroup ), thresholdsWithAllData );
            }
        }
        else
        {
            this.thresholds.put( Pair.of( inGroup, outGroup ), thresholdsWithoutAllData );
        }
    }

    /**
     * Validates the metrics configuration and throws a {@link MetricConfigurationException} if the validation fails
     * 
     * @param metrics the metrics configuration
     * @throws MetricConfigurationException if thresholds are configured incorrectly
     */

    private void validateOutputsConfig( MetricsConfig metrics )
            throws MetricConfigurationException
    {
        if ( hasThresholdMetrics() && !MetricConfigHelper.hasThresholds( metrics ) )
        {
            throw new MetricConfigurationException( "Thresholds are required by one or more of the configured "
                                                    + "metrics." );
        }
        //Check that probability thresholds are configured for left       
        if ( Objects.nonNull( metrics.getProbabilityThresholds() )
             && metrics.getProbabilityThresholds().getApplyTo() != LeftOrRightOrBaseline.LEFT )
        {
            throw new MetricConfigurationException( "Attempted to apply probability thresholds to '"
                                                    + metrics.getProbabilityThresholds().getApplyTo()
                                                    + "': this is not currently supported. Use '"
                                                    + LeftOrRightOrBaseline.LEFT
                                                    + "' instead." );
        }
        //Check that value thresholds are configured for left  
        if ( Objects.nonNull( metrics.getValueThresholds() )
             && metrics.getValueThresholds().getApplyTo() != LeftOrRightOrBaseline.LEFT )
        {
            throw new MetricConfigurationException( "Attempted to apply value thresholds to '"
                                                    + metrics.getValueThresholds().getApplyTo()
                                                    + "': this is not currently supported. Use '"
                                                    + LeftOrRightOrBaseline.LEFT
                                                    + "' instead." );
        }
    }

    /**
     * Returns a list of {@link Threshold} from a comma-separated string. Specify the type of {@link Threshold}
     * required.
     * 
     * @param inputString the comma-separated input string
     * @param oper the operator
     * @param areProbs is true to generate probability thresholds, false for ordinary thresholds
     * @return the thresholds
     * @throws MetricConfigurationException if the thresholds are configured incorrectly
     */

    private Set<Threshold> getThresholdsFromCommaSeparatedValues( String inputString,
                                                                   Operator oper,
                                                                   boolean areProbs )
            throws MetricConfigurationException
    {
        //Parse the double values
        List<Double> addMe =
                Arrays.stream( inputString.split( "," ) ).map( Double::parseDouble ).collect( Collectors.toList() );
        Set<Threshold> returnMe = new TreeSet<>();
        //Between operator
        if ( oper == Operator.BETWEEN )
        {
            if ( addMe.size() < 2 )
            {
                throw new MetricConfigurationException( "At least two values are required to compose a "
                                                        + "threshold that operates between a lower and an upper bound." );
            }
            for ( int i = 0; i < addMe.size() - 1; i++ )
            {
                if ( areProbs )
                {
                    returnMe.add( dataFactory.ofProbabilityThreshold( addMe.get( i ), addMe.get( i + 1 ), oper ) );
                }
                else
                {
                    returnMe.add( dataFactory.ofThreshold( addMe.get( i ), addMe.get( i + 1 ), oper ) );
                }
            }
        }
        //Other operators
        else
        {
            if ( areProbs )
            {
                addMe.forEach( threshold -> returnMe.add( dataFactory.ofProbabilityThreshold( threshold, oper ) ) );
            }
            else
            {
                addMe.forEach( threshold -> returnMe.add( dataFactory.ofThreshold( threshold, oper ) ) );
            }
        }
        return returnMe;
    }

}
