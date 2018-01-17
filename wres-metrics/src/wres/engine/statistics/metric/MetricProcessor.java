package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProject;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;

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
     * List of thresholds associated with each metric.
     */

    final EnumMap<MetricConstants, List<Threshold>> localThresholds;

    /**
     * List of thresholds that apply to all metrics within a {@link MetricInputGroup}.
     */

    final EnumMap<MetricInputGroup, List<Threshold>> globalThresholds;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     */

    final MetricCollection<SingleValuedPairs, ScalarOutput> singleValuedScalar;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link MultiValuedScoreOutput}.
     */

    final MetricCollection<SingleValuedPairs, MultiValuedScoreOutput> singleValuedVector;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorOutput}.
     */

    final MetricCollection<SingleValuedPairs, MultiVectorOutput> singleValuedMultiVector;

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
     * The minimum sample size to use when computing metrics.
     */

    final int minimumSampleSize;

    /**
     * The number of decimal places to use when rounding.
     */

    private static final int DECIMALS = 5;

    /**
     * Returns a {@link MetricOutputForProject} for the last available results or null if
     * {@link #hasCachedMetricOutput()} returns false.
     * 
     * @return a {@link MetricOutputForProject} or null
     */

    public abstract T getCachedMetricOutput();

    /**
     * Returns true if a prior call led to the caching of metric outputs.
     * 
     * @return true if stored results are available, false otherwise
     */

    public abstract boolean hasCachedMetricOutput();

    /**
     * Validates the configuration and throws a {@link MetricConfigurationException} if the configuration is invalid.
     * 
     * @param config the configuration to validate
     * @throws MetricConfigurationException if the configuration is invalid
     */

    abstract void validate( ProjectConfig config ) throws MetricConfigurationException;

    /**
     * Returns a set of {@link MetricConstants} from a {@link ProjectConfig}. If the {@link ProjectConfig} contains
     * the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics are returned that are consistent
     * with the configuration. 
     * 
     * TODO: consider interpreting configured metrics in combination with {@link MetricConfigName#ALL_VALID} as 
     * overrides to be removed from the {@link MetricConfigName#ALL_VALID} metrics.
     * 
     * @param config the project configuration
     * @return a set of {@link MetricConstants}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public static Set<MetricConstants> getMetricsFromConfig( ProjectConfig config ) throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to generate metrics." );
        //Obtain the list of metrics
        List<MetricConfigName> metricsConfig = config.getOutputs()
                                                     .getMetric()
                                                     .stream()
                                                     .map( MetricConfig::getName )
                                                     .collect( Collectors.toList() );
        Set<MetricConstants> metrics = new TreeSet<>();
        //All valid metrics
        if ( metricsConfig.contains( MetricConfigName.ALL_VALID ) )
        {
            metrics = getAllValidMetricsFromConfig( config );
        }
        //Explicitly configured metrics
        else
        {
            for ( MetricConfigName metric : metricsConfig )
            {
                metrics.add( ConfigMapper.from( metric ) );
            }
        }
        return metrics;
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
     * Returns the metric data input type from the {@link ProjectConfig}.
     * 
     * @param config the {@link ProjectConfig}
     * @return the {@link MetricInputGroup} based on the {@link ProjectConfig}
     * @throws MetricConfigurationException if the input type is not recognized
     */

    static MetricInputGroup getInputType( ProjectConfig config ) throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to generate metrics." );
        DatasourceType type = config.getInputs().getRight().getType();
        switch ( type )
        {
            case ENSEMBLE_FORECASTS:
                return MetricInputGroup.ENSEMBLE;
            case SINGLE_VALUED_FORECASTS:
            case SIMULATIONS:
                return MetricInputGroup.SINGLE_VALUED;
            default:
                throw new MetricConfigurationException( "Unable to interpret the input type '" + type
                                                        + "' when attempting to process the metrics " );
        }
    }

    /**
     * <p>Returns a list of all supported metrics given the input {@link ProjectConfig}. Specifically, checks the 
     * {@link ProjectConfig} for the data type of the right-side and for any thresholds, returning metrics as 
     * follows:</p>
     * <ol>
     * <li>If the right side contains {@link DatasourceType#ENSEMBLE_FORECASTS} and thresholds are defined: returns
     * all metrics that consume {@link MetricInputGroup#ENSEMBLE}, {@link MetricInputGroup#SINGLE_VALUED} and
     * {@link MetricInputGroup#DISCRETE_PROBABILITY}</li>
     * <li>If the right side contains {@link DatasourceType#ENSEMBLE_FORECASTS} and thresholds are not defined: returns
     * all metrics that consume {@link MetricInputGroup#ENSEMBLE} and {@link MetricInputGroup#SINGLE_VALUED}</li>
     * <li>If the right side contains {@link DatasourceType#SINGLE_VALUED_FORECASTS} and thresholds are defined: returns
     * all metrics that consume {@link MetricInputGroup#SINGLE_VALUED} and {@link MetricInputGroup#DICHOTOMOUS}</li>
     * <li>If the right side contains {@link DatasourceType#SINGLE_VALUED_FORECASTS} and thresholds are not defined: 
     * returns all metrics that consume {@link MetricInputGroup#SINGLE_VALUED}.</li>
     * </ol>
     * 
     * TODO: implement multicategory metrics.
     * @param config the {@link ProjectConfig}
     * @return a list of all metrics that are compatible with the project configuration  
     * @throws MetricConfigurationException if the configuration is invalid
     */

    static Set<MetricConstants> getAllValidMetricsFromConfig( ProjectConfig config )
            throws MetricConfigurationException
    {
        Set<MetricConstants> returnMe;
        MetricInputGroup group = getInputType( config );
        switch ( group )
        {
            case ENSEMBLE:
                returnMe = getMetricsForEnsembleInput( config );
                break;
            case SINGLE_VALUED:
                returnMe = getMetricsForSingleValuedInput( config );
                break;
            default:
                throw new MetricConfigurationException( "Unexpected input identifier '" + group + "'." );
        }
        //Remove CRPSS if no baseline is available
        DataSourceConfig baseline = config.getInputs().getBaseline();
        if ( Objects.isNull( baseline ) )
        {
            returnMe.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        }
        //Disallow non-score metrics when pooling window configuration is present, until this 
        //is supported
        PoolingWindowConfig windows = config.getPair().getPoolingWindow();
        if ( Objects.nonNull( windows ) )
        {
            returnMe.removeIf( a -> ! ( a.isInGroup( MetricOutputGroup.SCALAR )
                                        || a.isInGroup( MetricOutputGroup.VECTOR ) ) );
        }
        return returnMe;
    }

    /**
     * Returns true if the input {@link Outputs} has thresholds configured, false otherwise.
     * 
     * @param outputs the {@link Outputs} configuration
     * @return true if the project configuration has thresholds configured, false otherwise
     */

    static boolean hasThresholds( Outputs outputs )
    {
        //Global thresholds
        if ( Objects.nonNull( outputs.getProbabilityThresholds() )
             || Objects.nonNull( outputs.getValueThresholds() ) )
        {
            return true;
        }
        //Local thresholds
        for ( MetricConfig metric : outputs.getMetric() )
        {
            if ( Objects.nonNull( metric.getProbabilityThresholds() )
                 || Objects.nonNull( metric.getValueThresholds() ) )
            {
                return true;
            }
        }
        return false;
    }

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
     */

    MetricProcessor( final DataFactory dataFactory,
                     final ProjectConfig config,
                     final ExecutorService thresholdExecutor,
                     final ExecutorService metricExecutor,
                     final MetricOutputGroup... mergeList )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config,
                                "Specify a non-null project configuration from which to construct the metric processor." );
        Objects.requireNonNull( dataFactory,
                                "Specify a non-null data factory from which to construct the metric processor." );
        this.dataFactory = dataFactory;
        metrics = getMetricsFromConfig( config );
        metricFactory = MetricFactory.getInstance( dataFactory );
        //Construct the metrics that are common to more than one type of input pairs
        try
        {
            if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ) )
            {
                singleValuedScalar =
                        metricFactory.ofSingleValuedScalarCollection( metricExecutor,
                                                                      getSelectedMetrics( metrics,
                                                                                          MetricInputGroup.SINGLE_VALUED,
                                                                                          MetricOutputGroup.SCALAR ) );
            }
            else
            {
                singleValuedScalar = null;
            }
            if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR ) )
            {
                singleValuedVector =
                        metricFactory.ofSingleValuedVectorCollection( metricExecutor,
                                                                      getSelectedMetrics( metrics,
                                                                                          MetricInputGroup.SINGLE_VALUED,
                                                                                          MetricOutputGroup.VECTOR ) );
            }
            else
            {
                singleValuedVector = null;
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
        }
        catch ( MetricParameterException e )
        {
            throw new MetricConfigurationException( "Failed to construct one or more metrics.", e );
        }
        //Obtain the thresholds for each metric and store them
        localThresholds = new EnumMap<>( MetricConstants.class );
        globalThresholds = new EnumMap<>( MetricInputGroup.class );
        setThresholds( dataFactory, config );
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
        //Set the minimum sample size for computing metrics
        minimumSampleSize = 2;
        //Finally, validate the configuration against the parameters set
        validate( config );
    }

    /**
     * Returns true if the input list of thresholds contains one or more probability thresholds, false otherwise.
     * 
     * @param check the thresholds to check
     * @return true if the input list contains one or more probability thresholds, false otherwise
     */

    boolean hasProbabilityThreshold( List<Threshold> check )
    {
        return check.stream().anyMatch( Threshold::hasProbabilityValues );
    }

    /**
     * Returns true if global thresholds are available for a particular {@link MetricInputGroup}, false otherwise.
     * 
     * @param group the {@link MetricInputGroup} to check
     * @return true if global thresholds are available for a {@link MetricInputGroup}, false otherwise
     */

    boolean hasGlobalThresholds( MetricInputGroup group )
    {
        return globalThresholds.containsKey( group );
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

    double[] getSortedClimatology( PairedInput<?> input, List<Threshold> thresholds )
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
     * Validates the inputs and throws a {@link MetricInputSliceException} if the input contains fewer samples than
     * {@link #minimumSampleSize}.
     * 
     * @param subset the data to validate
     * @param threshold the threshold used to localize the error message
     * @throws MetricInputSliceException if the input contains insufficient data for metric calculation 
     * @throws InsufficientDataException if the input contains all missing pairs after slicing
     */

    void checkSlice( PairedInput<?> subset, Threshold threshold ) throws MetricInputSliceException
    {
        if ( subset.getData().size() < minimumSampleSize )
        {
            throw new MetricInputSliceException( "Failed to compute one or more metrics for threshold '"
                                                 + threshold
                                                 + "', as the sample size was less than the prescribed minimum of '"
                                                 + minimumSampleSize
                                                 + "'." );
        }
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#ENSEMBLE}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#ENSEMBLE}
     */

    private static Set<MetricConstants> getMetricsForEnsembleInput( ProjectConfig config )
    {
        Set<MetricConstants> returnMe = new TreeSet<>();
        returnMe.addAll( MetricInputGroup.ENSEMBLE.getMetrics() );
        returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );
        if ( hasThresholds( config.getOutputs() ) )
        {
            returnMe.addAll( MetricInputGroup.DISCRETE_PROBABILITY.getMetrics() );
        }
        return returnMe;
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#SINGLE_VALUED}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#SINGLE_VALUED}
     */

    private static Set<MetricConstants> getMetricsForSingleValuedInput( ProjectConfig config )
    {
        Set<MetricConstants> returnMe = new TreeSet<>();
        returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );
        if ( hasThresholds( config.getOutputs() ) )
        {
            returnMe.addAll( MetricInputGroup.DICHOTOMOUS.getMetrics() );
        }
        return returnMe;
    }

    /**
     * Sets the thresholds for each metric in the configuration, including any thresholds that apply globally (to all
     * metrics).
     * 
     * @param dataFactory a data factory
     * @param config the project configuration
     * @throws MetricConfigurationException if thresholds are configured incorrectly
     */

    private void setThresholds( DataFactory dataFactory, ProjectConfig config ) throws MetricConfigurationException
    {
        //Validate the configuration
        Outputs outputs = config.getOutputs();
        validateOutputsConfig( outputs );
        //Check for metric-local thresholds and throw an exception if they are defined, as they are currently not supported
//        EnumMap<MetricConstants, List<Threshold>> localThresholds = new EnumMap<>( MetricConstants.class );
//      TODO: store metric-local threshold conditions when they are available in the configuration
        for ( MetricConfig metric : outputs.getMetric() )
        {
            if ( metric.getName() != MetricConfigName.ALL_VALID )
            {
//                //Obtain metric-local thresholds here
//                List<Threshold> thresholds = new ArrayList<>();
//                if ( !thresholds.isEmpty() )
//                {
//                    localThresholds.put( name, thresholds );
//                }
            }
        }
//        if ( !localThresholds.isEmpty() )
//        {
//            this.localThresholds.putAll( localThresholds );
//        }
        //Pool together all probability thresholds and real-valued thresholds in the context of the 
        //outputs configuration. 
        List<Threshold> globalThresholds = new ArrayList<>();
        List<Threshold> globalThresholdsAllData = new ArrayList<>();

        //Add a threshold for "all data" by default
        globalThresholdsAllData.add( dataFactory.getThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER ) );
        //Add probability thresholds
        if ( Objects.nonNull( outputs.getProbabilityThresholds() ) )
        {
            Operator oper = ConfigMapper.from( outputs.getProbabilityThresholds().getOperator() );
            String values = outputs.getProbabilityThresholds().getCommaSeparatedValues();
            List<Threshold> thresholds = getThresholdsFromCommaSeparatedValues( values, oper, true );
            globalThresholds.addAll( thresholds );
            globalThresholdsAllData.addAll( thresholds );
        }
        //Add real-valued thresholds
        if ( Objects.nonNull( outputs.getValueThresholds() ) )
        {
            Operator oper = ConfigMapper.from( outputs.getValueThresholds().getOperator() );
            String values = outputs.getValueThresholds().getCommaSeparatedValues();
            List<Threshold> thresholds = getThresholdsFromCommaSeparatedValues( values, oper, false );
            globalThresholds.addAll( thresholds );
            globalThresholdsAllData.addAll( thresholds );
        }

        //Only set the global thresholds if no local ones are available
        if ( localThresholds.isEmpty() )
        {
            for ( MetricInputGroup group : MetricInputGroup.values() )
            {
                if ( group.equals( MetricInputGroup.SINGLE_VALUED ) || group.equals( MetricInputGroup.ENSEMBLE ) )
                {
                    this.globalThresholds.put( group, globalThresholdsAllData );
                }
                else
                {
                    this.globalThresholds.put( group, globalThresholds );
                }
            }
        }
    }

    /**
     * Validates the outputs configuration and throws a {@link MetricConfigurationException} if the validation fails
     * 
     * @param outputs the outputs configuration
     * @throws MetricConfigurationException if thresholds are configured incorrectly
     */

    private void validateOutputsConfig( Outputs outputs ) throws MetricConfigurationException
    {
        if ( hasThresholdMetrics() && !hasThresholds( outputs ) )
        {
            throw new MetricConfigurationException( "Thresholds are required by one or more of the configured "
                                                    + "metrics." );
        }
        //Check that probability thresholds are configured for left       
        if ( Objects.nonNull( outputs.getProbabilityThresholds() )
             && outputs.getProbabilityThresholds().getApplyTo() != LeftOrRightOrBaseline.LEFT )
        {
            throw new MetricConfigurationException( "Attempted to apply probability thresholds to '"
                                                    + outputs.getProbabilityThresholds().getApplyTo()
                                                    + "': this is not currently supported. Use '"
                                                    + LeftOrRightOrBaseline.LEFT
                                                    + "' instead." );
        }
        //Check that value thresholds are configured for left  
        if ( Objects.nonNull( outputs.getValueThresholds() )
             && outputs.getValueThresholds().getApplyTo() != LeftOrRightOrBaseline.LEFT )
        {
            throw new MetricConfigurationException( "Attempted to apply value thresholds to '"
                                                    + outputs.getValueThresholds().getApplyTo()
                                                    + "': this is not currently supported. Use '"
                                                    + LeftOrRightOrBaseline.LEFT
                                                    + "' instead." );
        }
        //Check for metric-local thresholds and throw an exception if they are defined, as they are currently 
        //not supported
        for ( MetricConfig metric : outputs.getMetric() )
        {
            if ( metric.getName() != MetricConfigName.ALL_VALID )
            {
                MetricConstants name = ConfigMapper.from( metric.getName() );
                if ( Objects.nonNull( metric.getProbabilityThresholds() )
                     || Objects.nonNull( metric.getValueThresholds() ) )
                {
                    throw new MetricConfigurationException( "Found metric-local thresholds for '" + name
                                                            + "', which are not "
                                                            + "currently supported." );
                }
            }
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

    private List<Threshold> getThresholdsFromCommaSeparatedValues( String inputString,
                                                                   Operator oper,
                                                                   boolean areProbs )
            throws MetricConfigurationException
    {
        //Parse the double values
        List<Double> addMe =
                Arrays.stream( inputString.split( "," ) ).map( Double::parseDouble ).collect( Collectors.toList() );
        List<Threshold> returnMe = new ArrayList<>();
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
                    returnMe.add( dataFactory.getProbabilityThreshold( addMe.get( i ), addMe.get( i + 1 ), oper ) );
                }
                else
                {
                    returnMe.add( dataFactory.getThreshold( addMe.get( i ), addMe.get( i + 1 ), oper ) );
                }
            }
        }
        //Other operators
        else
        {
            if ( areProbs )
            {
                addMe.forEach( threshold -> returnMe.add( dataFactory.getProbabilityThreshold( threshold, oper ) ) );
            }
            else
            {
                addMe.forEach( threshold -> returnMe.add( dataFactory.getThreshold( threshold, oper ) ) );
            }
        }
        return returnMe;
    }

}
