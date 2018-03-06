package wres.engine.statistics.metric.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.config.generated.AbstractMetricConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProbabilityOrValue;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;

/**
 * A helper class for interpreting and using the {@link ProjectConfig} in the context of verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
 */

public final class MetricConfigHelper
{

    /**
     * Map between names
     */

    private static final EnumMap<MetricConfigName, MetricConstants> NAME_MAP = new EnumMap<>( MetricConfigName.class );

    /**
     * Map between summary statistic names
     */

    private static final EnumMap<SummaryStatisticsName, MetricConstants> STATISTICS_NAME_MAP =
            new EnumMap<>( SummaryStatisticsName.class );

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} or null if the input is
     * {@link MetricConfigName#ALL_VALID}. Throws an exception if no such mapping is available. 
     * 
     * @param configName the name in the {@link MetricConfigName}
     * @return the corresponding name in the {@link MetricConstants}
     * @throws MetricConfigurationException if the configName is not mapped or the input is null
     */

    public static MetricConstants from( MetricConfigName configName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( configName ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a named metric: "
                                                    + "check that the input configuration has been facet-validated against the list of metrics "
                                                    + "supported by the system configuration." );
        }
        buildMap();
        //All valid metrics
        if ( configName.equals( MetricConfigName.ALL_VALID ) )
        {
            return null;
        }
        if ( !NAME_MAP.containsKey( configName ) )
        {
            throw new MetricConfigurationException( " Unable to find a metric with a configured identifier of "
                                                    + "'" + configName + "'." );
        }
        return NAME_MAP.get( configName );
    }

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link SummaryStatisticsName} or null if the input is
     * {@link SummaryStatisticsName#ALL_VALID}. Throws an exception if no such mapping is available. 
     * 
     * @param statsName the name in the {@link SummaryStatisticsName}
     * @return the corresponding name in the {@link MetricConstants}
     * @throws MetricConfigurationException if the statsName is not mapped or the input is null
     */

    public static MetricConstants from( SummaryStatisticsName statsName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( statsName ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a named statistic." );
        }
        buildStatisticsMap();
        //All valid metrics
        if ( statsName.equals( SummaryStatisticsName.ALL_VALID ) )
        {
            return null;
        }
        if ( !STATISTICS_NAME_MAP.containsKey( statsName ) )
        {
            throw new MetricConfigurationException( " Unable to find a summary statistic with a configured identifier "
                                                    + "of '" + statsName + "'." );
        }
        return STATISTICS_NAME_MAP.get( statsName );
    }

    /**
     * Maps between threshold operators in {@link ThresholdOperator} and those in {@link Operator}.
     * 
     * @param configName the input {@link ThresholdOperator}
     * @return the corresponding {@link Operator}.
     * @throws MetricConfigurationException if the configName is not mapped or the input is null
     */

    public static Operator from( ThresholdOperator configName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( configName ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a name operator." );
        }
        switch ( configName )
        {
            case EQUAL_TO:
                return Operator.EQUAL;
            case LESS_THAN:
                return Operator.LESS;
            case GREATER_THAN:
                return Operator.GREATER;
            case LESS_THAN_OR_EQUAL_TO:
                return Operator.LESS_EQUAL;
            case GREATER_THAN_OR_EQUAL_TO:
                return Operator.GREATER_EQUAL;
            default:
                throw new MetricConfigurationException( "Unrecognized threshold operator in project configuration '"
                                                        + configName + "'." );
        }
    }

    /**
     * Returns the metric data input type from the {@link ProjectConfig}.
     * 
     * @param config the {@link ProjectConfig}
     * @return the {@link MetricInputGroup} based on the {@link ProjectConfig}
     * @throws MetricConfigurationException if the input type is not recognized
     */

    public static MetricInputGroup getInputType( ProjectConfig config ) throws MetricConfigurationException
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

    public static Set<MetricConstants> getAllValidMetricsFromConfig( ProjectConfig config )
            throws MetricConfigurationException
    {
        Set<MetricConstants> returnMe;
        MetricInputGroup group = getInputType( config );
        switch ( group )
        {
            case ENSEMBLE:
                returnMe = MetricConfigHelper.getMetricsForEnsembleInput( config );
                break;
            case SINGLE_VALUED:
                returnMe = new TreeSet<>();
                returnMe.addAll( getMetricsForSingleValuedInput( config ) );
                returnMe.addAll( getMetricsForSingleValuedTimeSeriesInput( config ) );
                break;
            default:
                throw new MetricConfigurationException( "Unexpected input type '" + group + "'." );
        }
        //Remove CRPSS if no baseline is available
        DataSourceConfig baseline = config.getInputs().getBaseline();
        if ( Objects.isNull( baseline ) )
        {
            returnMe.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        }
        //Disallow non-score metrics when pooling window configuration is present, until this 
        //is supported
        PoolingWindowConfig windows = config.getPair().getIssuedDatesPoolingWindow();
        if ( Objects.nonNull( windows ) )
        {
            returnMe.removeIf( a -> ! ( a.isInGroup( MetricOutputGroup.DOUBLE_SCORE )
                                        || a.isInGroup( MetricOutputGroup.DURATION_SCORE ) ) );
        }
        return returnMe;
    }

    /**
     * Returns a set of {@link MetricConstants} from a {@link ProjectConfig}. If the {@link ProjectConfig} contains
     * the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics are returned that are consistent
     * with the configuration. 
     * 
     * @param config the project configuration
     * @return a set of {@link MetricConstants}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public static Set<MetricConstants> getMetricsFromConfig( ProjectConfig config ) throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to generate metrics." );
        //Obtain the list of metrics
        List<MetricConfigName> metricsConfig = config.getMetrics()
                                                     .getMetric()
                                                     .stream()
                                                     .map( MetricConfig::getName )
                                                     .collect( Collectors.toList() );
        List<MetricConfigName> timeSeriesConfig = config.getMetrics()
                                                        .getTimeSeriesMetric()
                                                        .stream()
                                                        .map( TimeSeriesMetricConfig::getName )
                                                        .collect( Collectors.toList() );

        Set<MetricConstants> metrics = new TreeSet<>();
        //All valid metrics
        if ( metricsConfig.contains( MetricConfigName.ALL_VALID )
             || timeSeriesConfig.contains( MetricConfigName.ALL_VALID ) )
        {
            metrics = getAllValidMetricsFromConfig( config );
        }
        //Explicitly configured metrics
        else
        {
            //Ordinary metrics
            for ( MetricConfigName metric : metricsConfig )
            {
                metrics.add( from( metric ) );
            }
            //Time-series metrics
            for ( MetricConfigName metric : timeSeriesConfig )
            {
                metrics.add( from( metric ) );
            }
        }
        return metrics;
    }

    /**
     * Returns the {@link MetricConfig} associated with each {@link MetricConstants} in the input {@link ProjectConfig}. 
     * If the {@link ProjectConfig} contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a map of metrics against their configuration
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public static Map<MetricConstants, AbstractMetricConfig> getMetricConfigByMetric( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to obtain the metric configuration." );

        Map<MetricConstants, AbstractMetricConfig> returnMe = new EnumMap<>( MetricConstants.class );

        returnMe.putAll( getMetricConfigByOrdinaryMetric( config ) );
        returnMe.putAll( getMetricConfigByTimeSeriesMetric( config ) );

        return returnMe;
    }

    /**
     * Returns true if the input {@link Outputs} has thresholds configured, false otherwise.
     * 
     * @param metrics the {@link MetricsConfig} configuration
     * @return true if the project configuration has thresholds configured, false otherwise
     */

    public static boolean hasThresholds( MetricsConfig metrics )
    {
        // Global thresholds
        if ( !metrics.getThresholds().isEmpty() )
        {
            return true;
        }
        // Local thresholds
        return metrics.getMetric().stream().anyMatch( nextMetric -> !nextMetric.getThresholds().isEmpty() );
    }
    
    /**
     * Transforms a list of {@link ThresholdsConfig} to a set of {@link Threshold}.
     * 
     * @param thresholds the thresholds configuration
     * @param dataFactory the data factory with which to build thresholds
     * @return a set of thresholds (possibly empty)
     * @throws MetricConfigurationException if the metric configuration is invalid
     * @throws NullPointerException if either input is null
     * @throws MetricConfigurationException if the configuration is invalid
     */

    public static Set<Threshold> fromInternalThresholdsConfig( List<ThresholdsConfig> thresholds,
                                                               DataFactory dataFactory )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( thresholds, "Cannot obtain thresholds from null configuration." );

        Objects.requireNonNull( dataFactory, "Cannot obtain thresholds without a data factory." );

        Set<Threshold> returnMe = new HashSet<>();

        // Iterate and transform
        for ( ThresholdsConfig next : thresholds )
        {
            Operator operator = Operator.GREATER;
            // Operator specified
            if( Objects.nonNull( next.getOperator() ) )
            {
                operator = from( next.getOperator() );
            }
            // Must be internally sourced: thresholds with global scope should be provided directly 
            Object values = next.getCommaSeparatedValuesOrSource();
            if ( values instanceof String )
            {
                // Default to ProbabilityOrValue.PROBABILITY if null
                returnMe.addAll( getThresholdsFromCommaSeparatedValues( dataFactory,
                                                                        values.toString(),
                                                                        operator,
                                                                        next.getType() != ProbabilityOrValue.VALUE ) );
            }
        }
        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns true if the specified project configuration contains a metric of the specified type for which summary
     * statistics are defined, false otherwise.
     * 
     * @param config the project configuration
     * @param metric the metric to check
     * @return true if the configuration contains the specified type of metric, false otherwise
     */

    public static boolean hasSummaryStatisticsFor( ProjectConfig config, MetricConfigName metric )
    {
        Objects.requireNonNull( config, "Specify a non-null project configuration to check for summary statistics" );
        Objects.requireNonNull( metric, "Specify a non null metric name to check for summary statistics." );
        List<TimeSeriesMetricConfig> tsMetrics = config.getMetrics().getTimeSeriesMetric();
        for ( TimeSeriesMetricConfig next : tsMetrics )
        {
            if ( next.getName() == metric && Objects.nonNull( next.getSummaryStatistics() ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a list of summary statistics associated with the named metric.
     * 
     * @param config the project configuration
     * @param metric the metric whose summary statistics are required
     * @return the summary statistics associated with the named metric
     * @throws MetricConfigurationException if the project contains an unmapped summary statistic
     */

    public static MetricConstants[] getSummaryStatisticsFor( ProjectConfig config, MetricConfigName metric )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project configuration to check for summary statistics" );
        Objects.requireNonNull( metric, "Specify a non null metric name to check for summary statistics." );
        List<TimeSeriesMetricConfig> tsMetrics = config.getMetrics().getTimeSeriesMetric();
        Set<MetricConstants> allStats = new HashSet<>();
        for ( TimeSeriesMetricConfig next : tsMetrics )
        {
            if ( next.getName() == metric && Objects.nonNull( next.getSummaryStatistics() ) )
            {
                for ( SummaryStatisticsName nextStat : next.getSummaryStatistics().getName() )
                {
                    allStats.add( from( nextStat ) );
                }
            }
        }
        return allStats.toArray( new MetricConstants[allStats.size()] );
    }

    /**
     * Returns <code>true</code> if the input configuration requires any outputs of the specified type  where the 
     * {@link OutputTypeSelection} is {@link OutputTypeSelection#THRESHOLD_LEAD} for any or all metrics.
     * 
     * @param projectConfig the project configuration
     * @param outGroup the output group to test
     * @return true if the input configuration requires outputs of the {@link MetricOutputGroup#MULTIVECTOR} 
     *            type whose output type is {@link OutputTypeSelection#THRESHOLD_LEAD}, false otherwise
     * @throws NullPointerException if the input is null
     * @throws MetricConfigurationException if the configuration is invalid
     */

    public static boolean hasTheseOutputsByThresholdLead( ProjectConfig projectConfig, MetricOutputGroup outGroup )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration." );
        // Does the configuration contain any multivector types?        
        boolean hasSpecifiedType = MetricConfigHelper.getMetricsFromConfig( projectConfig )
                                                     .stream()
                                                     .anyMatch( a -> a.isInGroup( outGroup ) );

        // If there is a metric-local override for ALL_VALID, and this is *not* THRESHOLD_LEAD, return false
        // immediately because the metric-local override covers all metrics and the local type is canonical       
        if ( projectConfig.getMetrics()
                          .getMetric()
                          .stream()
                          .anyMatch( next -> MetricConfigName.ALL_VALID == next.getName()
                                             && Objects.nonNull( next.getOutputType() )
                                             && OutputTypeSelection.THRESHOLD_LEAD != next.getOutputType() ) )
        {
            return false;
        }

        // Does it contain any metric-local THRESHOLD_LEAD types?
        boolean hasThresholdLeadType = projectConfig.getMetrics()
                                                    .getMetric()
                                                    .stream()
                                                    .anyMatch( next -> OutputTypeSelection.THRESHOLD_LEAD == next.getOutputType() );

        // No local types, but does it contain any metric-global THRESHOLD_LEAD types?
        if ( !hasThresholdLeadType && Objects.nonNull( projectConfig.getOutputs() ) )
        {
            hasThresholdLeadType =
                    projectConfig.getOutputs()
                                 .getDestination()
                                 .stream()
                                 .anyMatch( next -> OutputTypeSelection.THRESHOLD_LEAD == next.getOutputType() );
        }
        return hasSpecifiedType && hasThresholdLeadType;
    }

    /**
     * Returns a list of {@link Threshold} from a comma-separated string. Specify the type of {@link Threshold}
     * required.
     * 
     * @param dataFactory a factory for building thresholds
     * @param inputString the comma-separated input string
     * @param oper the operator
     * @param areProbs is true to generate probability thresholds, false for ordinary thresholds
     * @return the thresholds
     * @throws MetricConfigurationException if the thresholds are configured incorrectly
     */

    public static Set<Threshold> getThresholdsFromCommaSeparatedValues( DataFactory dataFactory,
                                                                        String inputString,
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
        if ( hasThresholds( config.getMetrics() ) )
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
        List<MetricConfig> metrics = config.getMetrics().getMetric();
        //Add ordinary metrics
        if ( !metrics.isEmpty() )
        {
            returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );
            if ( hasThresholds( config.getMetrics() ) )
            {
                returnMe.addAll( MetricInputGroup.DICHOTOMOUS.getMetrics() );
            }
        }
        return returnMe;
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#SINGLE_VALUED_TIME_SERIES}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#SINGLE_VALUED_TIME_SERIES}
     */

    private static Set<MetricConstants> getMetricsForSingleValuedTimeSeriesInput( ProjectConfig config )
    {
        Set<MetricConstants> returnMe = new TreeSet<>();
        List<TimeSeriesMetricConfig> tsMetrics = config.getMetrics().getTimeSeriesMetric();
        //Add time-series metrics
        if ( !tsMetrics.isEmpty() )
        {
            returnMe.addAll( MetricInputGroup.SINGLE_VALUED_TIME_SERIES.getMetrics() );
        }
        return returnMe;
    }

    /**
     * Returns the {@link MetricConfig} associated with each {@link MetricConstants} in the input {@link ProjectConfig}. 
     * If the {@link ProjectConfig} contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a map of metrics against their configuration
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public static Map<MetricConstants, MetricConfig> getMetricConfigByOrdinaryMetric( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to obtain the ordinary "
                                        + "metric configuration." );

        Map<MetricConstants, MetricConfig> returnMe = new EnumMap<>( MetricConstants.class );

        // Iterate through the ordinary metrics and populate the map
        for ( MetricConfig next : config.getMetrics().getMetric() )
        {
            if ( next.getName() == MetricConfigName.ALL_VALID )
            {
                // All valid metrics for the input group
                Set<MetricConstants> allValid = null;
                MetricInputGroup inGroup = getInputType( config );
                if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                {
                    allValid = getMetricsForSingleValuedInput( config );
                }
                else if ( inGroup == MetricInputGroup.ENSEMBLE )
                {
                    allValid = getMetricsForEnsembleInput( config );
                }
                else
                {
                    throw new MetricConfigurationException( "Unexpected input type for metrics '" + inGroup + "'." );
                }
                allValid.forEach( metric -> returnMe.put( metric, next ) );
            }
            else
            {
                returnMe.put( from( next.getName() ), next );
            }
        }

        return returnMe;
    }

    /**
     * Returns the {@link TimeSeriesMetricConfig} associated with each {@link MetricConstants} in the input 
     * {@link ProjectConfig}. If the {@link ProjectConfig} contains the identifier {@link MetricConfigName#ALL_VALID}, 
     * all supported metrics are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a map of metrics against their configuration
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public static Map<MetricConstants, TimeSeriesMetricConfig> getMetricConfigByTimeSeriesMetric( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to obtain the time-series metric "
                                        + "configuration." );

        Map<MetricConstants, TimeSeriesMetricConfig> returnMe = new EnumMap<>( MetricConstants.class );

        // Iterate through the time-series metrics and populate the map
        for ( TimeSeriesMetricConfig next : config.getMetrics().getTimeSeriesMetric() )
        {
            if ( next.getName() == MetricConfigName.ALL_VALID )
            {
                // All valid metrics for the input group
                Set<MetricConstants> allValid = null;
                MetricInputGroup inGroup = getInputType( config );
                if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                {
                    allValid = getMetricsForSingleValuedTimeSeriesInput( config );
                }
                else
                {
                    throw new MetricConfigurationException( "Unexpected input type for time-series metrics '" +
                                                            inGroup
                                                            + "'." );
                }
                allValid.forEach( metric -> returnMe.put( metric, next ) );
            }
            else
            {
                returnMe.put( from( next.getName() ), next );
            }
        }

        return returnMe;
    }

    /**
     * Builds the mapping between the {@link MetricConstants} and the {@link MetricConfigName} 
     */

    private static void buildMap()
    {
        //Lazy population
        if ( NAME_MAP.isEmpty() )
        {
            //Match on name
            for ( MetricConfigName nextConfig : MetricConfigName.values() )
            {
                for ( MetricConstants nextMetric : MetricConstants.values() )
                {
                    if ( nextConfig.name().equals( nextMetric.name() ) )
                    {
                        NAME_MAP.put( nextConfig, nextMetric );
                        break;
                    }
                }
            }
        }
    }

    /**
     * Builds the mapping between the {@link MetricConstants} and the {@link SummaryStatisticsName} 
     */

    private static void buildStatisticsMap()
    {
        //Lazy population
        if ( STATISTICS_NAME_MAP.isEmpty() )
        {
            //Match on name
            for ( SummaryStatisticsName nextStat : SummaryStatisticsName.values() )
            {
                // Use one of type ScoreOutputGroup.UNIVARIATE_STATISTIC to find the others
                for ( MetricConstants nextSystemStat : MetricConstants.MEAN.getAllComponents() )
                {
                    if ( nextSystemStat.name().equals( nextStat.name() ) )
                    {
                        STATISTICS_NAME_MAP.put( nextStat, nextSystemStat );
                        break;
                    }
                }
            }
        }
    }

    /**
     * Hidden constructor.
     */

    private MetricConfigHelper()
    {
    }

}
