package wres.engine.statistics.metric.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.ThresholdsByType;

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
     * Null configuration error.
     */

    public static final String NULL_CONFIGURATION_ERROR = "Specify non-null project configuration.";


    /**
     * Null data factory error.
     */

    public static final String NULL_DATA_FACTORY_ERROR = "Specify a non-null data factory.";

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
            throw new MetricConfigurationException( "Unable to map a null input identifier to a named metric." );
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
     * Returns the {@link MetricConfigName} that corresponds to the {@link MetricConstants}. Cannot map the
     * {@link MetricConfigName#ALL_VALID}. Throws an exception if no mapping is available. 
     * 
     * @param metricName the metric name
     * @return the corresponding name in the {@link MetricConfigName}
     * @throws MetricConfigurationException if the metricName is not mapped or the input is null
     */

    public static MetricConfigName from( MetricConstants metricName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( metricName ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a named metric." );
        }

        buildMap();

        for ( Entry<MetricConfigName, MetricConstants> next : NAME_MAP.entrySet() )
        {
            if ( next.getValue() == metricName )
            {
                return next.getKey();
            }
        }

        throw new MetricConfigurationException( " Unable to find a configured metric that corresponds to an "
                                                + "identifier of '" + metricName + "'." );
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
        Set<MetricConstants> returnMe = new TreeSet<>();
        ;
        MetricInputGroup group = MetricConfigHelper.getInputType( config );
        switch ( group )
        {
            case ENSEMBLE:
                returnMe.addAll( MetricConfigHelper.getValidMetricsForEnsembleInput( config ) );
                break;
            case SINGLE_VALUED:
                returnMe.addAll( MetricConfigHelper.getValidMetricsForSingleValuedInput( config ) );
                returnMe.addAll( MetricConfigHelper.getValidMetricsForSingleValuedTimeSeriesInput( config ) );
                break;
            default:
                throw new MetricConfigurationException( "Unexpected input type '" + group + "'." );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * <p>Returns a list of all supported time-series metrics given the input {@link ProjectConfig}. 
     * 
     * @param config the {@link ProjectConfig}
     * @return a list of all time-series metrics that are compatible with the project configuration  
     * @throws MetricConfigurationException if the configuration is invalid
     */

    public static Set<MetricConstants> getAllValidTimeSeriesMetricsFromConfig( ProjectConfig config )
            throws MetricConfigurationException
    {
        MetricInputGroup group = MetricConfigHelper.getInputType( config );

        // Only single-valued time-series metrics valid in this context
        if ( group == MetricInputGroup.SINGLE_VALUED )
        {
            return MetricConfigHelper.getValidMetricsForSingleValuedTimeSeriesInput( config );
        }

        return Collections.emptySet();
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

        return Collections.unmodifiableSet( MetricConfigHelper.getMetricConfigByMetric( config ).keySet() );
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

    public static Map<MetricConstants, MetricConfig> getMetricConfigByMetric( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project from which to obtain the metric configuration." );

        Map<MetricConstants, MetricConfig> returnMe = new EnumMap<>( MetricConstants.class );

        returnMe.putAll( MetricConfigHelper.getMetricConfigByOrdinaryMetric( config ) );
        
        returnMe.putAll( MetricConfigHelper.getMetricConfigByTimeSeriesMetric( config ) );

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Transforms a list of {@link ThresholdsConfig} to a set of {@link Threshold}.
     * 
     * @param thresholds the thresholds configuration
     * @param units optional units for non-probability thresholds
     * @param dataFactory the data factory with which to build thresholds
     * @param types an optional list of threshold types to read
     * @return a set of thresholds (possibly empty)
     * @throws MetricConfigurationException if the metric configuration is invalid
     * @throws NullPointerException if either input is null
     * @throws MetricConfigurationException if the configuration is invalid
     */

    public static Set<Threshold> fromInternalThresholdsConfig( List<ThresholdsConfig> thresholds,
                                                               Dimension units,
                                                               DataFactory dataFactory,
                                                               ThresholdType... types )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( thresholds, "Cannot obtain thresholds from null configuration." );

        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        Set<Threshold> returnMe = new HashSet<>();

        // Iterate and transform
        for ( ThresholdsConfig next : thresholds )
        {
            Operator operator = Operator.GREATER;

            // Operator specified
            if ( Objects.nonNull( next.getOperator() ) )
            {
                operator = MetricConfigHelper.from( next.getOperator() );
            }

            // Must be internally sourced: thresholds with global scope should be provided directly 
            Object values = next.getCommaSeparatedValuesOrSource();

            // Default to ThresholdType.PROBABILITY
            ThresholdType type = ThresholdType.PROBABILITY;
            if ( Objects.nonNull( next.getType() ) )
            {
                type = next.getType();
            }

            // String = internal sourced
            if ( values instanceof String
                 && ( types.length == 0
                      || Arrays.asList( types ).contains( type ) ) )
            {
                returnMe.addAll( MetricConfigHelper.getThresholdsFromCommaSeparatedValues( dataFactory,
                                                                                           values.toString(),
                                                                                           operator,
                                                                                           type != ThresholdType.VALUE,
                                                                                           units ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a set of thresholds used to classify probabilities to binary outcomes. If external thresholds are
     * defined, these thresholds are appended. Only returns metrics to which these thresholds apply, namely
     * metrics that consume {@link MetricInputGroup#DICHOTOMOUS}.
     * 
     * @param config the project configuration
     * @param dataFactory the data factory with which to build thresholds
     * @param external the optional external thresholds, may be null
     * @throws MetricConfigurationException if the canonical thresholds are inappropriate
     * @throws NullPointerException if any required input is null
     * @return the probability classifiers
     */

    public static Map<MetricConstants, Set<Threshold>> getProbabilityClassifiers( ProjectConfig config,
                                                                                  DataFactory dataFactory,
                                                                                  Map<MetricConfigName, ThresholdsByType> external )
            throws MetricConfigurationException
    {

        // Validate
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        Map<MetricConstants, Set<Threshold>> returnMe = new EnumMap<>( MetricConstants.class );

        // Iterate through the metric configuration groups, adding thresholds
        for ( MetricsConfig nextGroup : config.getMetrics() )
        {
            MetricConfigHelper.addProbabilityClassifiers( config,
                                                          returnMe,
                                                          nextGroup,
                                                          dataFactory,
                                                          external );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Returns true if the specified project configuration contains a metric of the specified type for which summary
     * statistics are defined, false otherwise.
     * 
     * @param config the project configuration
     * @param metric the metric to check
     * @return true if the configuration contains the specified type of metric, false otherwise
     * @throws MetricConfigurationException if the configuration is invalid
     */

    public static boolean hasSummaryStatisticsFor( ProjectConfig config, MetricConfigName metric )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project configuration to check for summary statistics" );

        Objects.requireNonNull( metric, "Specify a non null metric name to check for summary statistics." );

        Map<MetricConstants, TimeSeriesMetricConfig> configs =
                MetricConfigHelper.getMetricConfigByTimeSeriesMetric( config );

        return configs.values()
                      .stream()
                      .anyMatch( next -> next.getName() == metric && Objects.nonNull( next.getSummaryStatistics() ) );
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

        Map<MetricConstants, TimeSeriesMetricConfig> configs =
                MetricConfigHelper.getMetricConfigByTimeSeriesMetric( config );

        Set<MetricConstants> allStats = new HashSet<>();
        for ( TimeSeriesMetricConfig next : configs.values() )
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
     * @throws MetricConfigurationException if the configuration is invalid
     * @throws NullPointerException if the input is null
     */

    public static boolean hasTheseOutputsByThresholdLead( ProjectConfig projectConfig, MetricOutputGroup outGroup )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        // Does the configuration contain the requested type?        
        boolean hasSpecifiedType = MetricConfigHelper.getMetricsFromConfig( projectConfig )
                                                     .stream()
                                                     .anyMatch( a -> a.isInGroup( outGroup ) );

        // Does it contain any THRESHOLD_LEAD types?
        boolean hasThresholdLeadType = false;

        if ( Objects.nonNull( projectConfig.getOutputs() ) )
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
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     * @throws MetricConfigurationException if the thresholds are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    public static Set<Threshold> getThresholdsFromCommaSeparatedValues( DataFactory dataFactory,
                                                                        String inputString,
                                                                        Operator oper,
                                                                        boolean areProbs,
                                                                        Dimension units )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        Objects.requireNonNull( inputString, "Specify a non-null input string." );

        Objects.requireNonNull( oper, "Specify a non-null operator." );

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
                    returnMe.add( dataFactory.ofProbabilityThreshold( dataFactory.ofOneOrTwoDoubles( addMe.get( i ),
                                                                                                     addMe.get( i
                                                                                                                + 1 ) ),
                                                                      oper,
                                                                      units ) );
                }
                else
                {
                    returnMe.add( dataFactory.ofThreshold( dataFactory.ofOneOrTwoDoubles( addMe.get( i ),
                                                                                          addMe.get( i + 1 ) ),
                                                           oper,
                                                           units ) );
                }
            }
        }
        //Other operators
        else
        {
            if ( areProbs )
            {
                addMe.forEach( threshold -> returnMe.add( dataFactory.ofProbabilityThreshold( dataFactory.ofOneOrTwoDoubles( threshold ),
                                                                                              oper,
                                                                                              units ) ) );
            }
            else
            {
                addMe.forEach( threshold -> returnMe.add( dataFactory.ofThreshold( dataFactory.ofOneOrTwoDoubles( threshold ),
                                                                                   oper,
                                                                                   units ) ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns <code>true</code> if the project configuration contains thresholds, <code>false</code> otherwise.
     * 
     * @param config the project configuration
     * @return true if the configuration contains thresholds, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasThresholds( ProjectConfig config )
    {
        return MetricConfigHelper.hasThresholds( config, null );
    }

    /**
     * Returns <code>true</code> if the project configuration contains thresholds of the specified type or for 
     * all types (null), <code>false</code> otherwise.
     * 
     * @param config the project configuration
     * @param type the optional threshold type, may be null for all types
     * @return true if the configuration contains thresholds, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasThresholds( ProjectConfig config, ThresholdType type )
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        return config.getMetrics().stream().anyMatch( nextGroup -> hasThresholds( nextGroup, type ) );
    }

    /**
     * Returns <code>true</code> if the project configuration contains thresholds, <code>false</code> otherwise.
     * 
     * @param config the project configuration
     * @param type an optional threshold type, may be null
     * @return true if the configuration contains thresholds, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasThresholds( MetricsConfig config, ThresholdType type )
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        // No type condition
        if ( Objects.isNull( type ) )
        {
            // Has some thresholds
            return !config.getThresholds().isEmpty();
        }

        // Has some thresholds of a specified type
        return config.getThresholds().stream().anyMatch( testType -> testType.getType() == type );
    }

    /**
     * Returns the {@link MetricConfig} associated with each {@link MetricConstants} in the input {@link ProjectConfig}. 
     * If the {@link ProjectConfig} contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a map of metrics against their configuration
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    public static Map<MetricConstants, MetricConfig> getMetricConfigByOrdinaryMetric( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Map<MetricConstants, MetricConfig> returnMe = new EnumMap<>( MetricConstants.class );

        // Iterate through the ordinary metrics and populate the map
        for ( MetricsConfig metrics : config.getMetrics() )
        {
            for ( MetricConfig next : metrics.getMetric() )
            {
                // All valid metrics
                if ( next.getName() == MetricConfigName.ALL_VALID )
                {
                    Set<MetricConstants> allValid = null;
                    MetricInputGroup inGroup = MetricConfigHelper.getInputType( config );

                    // Single-valued metrics
                    if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                    {
                        allValid = MetricConfigHelper.getValidMetricsForSingleValuedInput( config );
                    }
                    // Ensemble metrics
                    else if ( inGroup == MetricInputGroup.ENSEMBLE )
                    {
                        allValid = MetricConfigHelper.getValidMetricsForEnsembleInput( config );
                    }
                    // Unrecognized type
                    else
                    {
                        throw new MetricConfigurationException( "Unexpected input type for metrics '" + inGroup
                                                                + "'." );
                    }
                    allValid.forEach( metric -> returnMe.put( metric, next ) );

                    // Return immediately                    
                    return Collections.unmodifiableMap( returnMe );
                }
                else
                {
                    returnMe.put( from( next.getName() ), next );
                }
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Returns the {@link TimeSeriesMetricConfig} associated with each {@link MetricConstants} in the input 
     * {@link ProjectConfig}. If the {@link ProjectConfig} contains the identifier {@link MetricConfigName#ALL_VALID}, 
     * all supported metrics are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a map of metrics against their configuration
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    public static Map<MetricConstants, TimeSeriesMetricConfig> getMetricConfigByTimeSeriesMetric( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Map<MetricConstants, TimeSeriesMetricConfig> returnMe = new EnumMap<>( MetricConstants.class );

        // Iterate through the ordinary metrics and populate the map
        for ( MetricsConfig metrics : config.getMetrics() )
        {
            for ( TimeSeriesMetricConfig next : metrics.getTimeSeriesMetric() )
            {
                // All valid metrics
                if ( next.getName() == MetricConfigName.ALL_VALID )
                {
                    Set<MetricConstants> allValid = null;
                    MetricInputGroup inGroup = MetricConfigHelper.getInputType( config );

                    // Single-valued metrics
                    if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                    {
                        allValid = MetricConfigHelper.getValidMetricsForSingleValuedInput( config );
                    }
                    // Unrecognized type
                    else
                    {
                        throw new MetricConfigurationException( "Unexpected input type for time-series metrics '"
                                                                + inGroup
                                                                + "'." );
                    }
                    allValid.forEach( metric -> returnMe.put( metric, next ) );

                    // Return immediately                    
                    return Collections.unmodifiableMap( returnMe );
                }
                else
                {
                    returnMe.put( from( next.getName() ), next );
                }
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Returns <code>true</code> if the input configuration has time-series metrics, otherwise <code>false</code>.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration has time-series metrics, otherwise false
     */

    public static boolean hasTimeSeriesMetrics( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        return projectConfig.getMetrics().stream().anyMatch( next -> !next.getTimeSeriesMetric().isEmpty() );
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#ENSEMBLE}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#ENSEMBLE}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getValidMetricsForEnsembleInput( ProjectConfig config )
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();
        returnMe.addAll( MetricInputGroup.ENSEMBLE.getMetrics() );
        returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );

        if ( MetricConfigHelper.hasThresholds( config ) )
        {
            returnMe.addAll( MetricInputGroup.DISCRETE_PROBABILITY.getMetrics() );
        }

        // Allow dichotomous metrics when probability classifiers are defined
        if ( MetricConfigHelper.hasThresholds( config, ThresholdType.PROBABILITY_CLASSIFIER ) )
        {
            returnMe.addAll( MetricInputGroup.DICHOTOMOUS.getMetrics() );
        }

        return removeMetricsDisallowedByOtherConfig( config, returnMe );
    }

    /**
     * Returns valid ordinary metrics for {@link MetricInputGroup#SINGLE_VALUED}. Also see:
     * {@link #getValidMetricsForSingleValuedTimeSeriesInput(ProjectConfig)}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#SINGLE_VALUED}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getValidMetricsForSingleValuedInput( ProjectConfig config )
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        //Add ordinary metrics if required
        if ( config.getMetrics().stream().anyMatch( next -> !next.getMetric().isEmpty() ) )
        {
            returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );
            if ( hasThresholds( config ) )
            {
                returnMe.addAll( MetricInputGroup.DICHOTOMOUS.getMetrics() );
            }
        }

        return removeMetricsDisallowedByOtherConfig( config, returnMe );
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#SINGLE_VALUED_TIME_SERIES}
     * 
     * @param config the project configuration
     * @return the valid metrics for {@link MetricInputGroup#SINGLE_VALUED_TIME_SERIES}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getValidMetricsForSingleValuedTimeSeriesInput( ProjectConfig config )
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        //Add time-series metrics if required
        if ( config.getMetrics().stream().anyMatch( next -> !next.getTimeSeriesMetric().isEmpty() ) )
        {
            returnMe.addAll( MetricInputGroup.SINGLE_VALUED_TIME_SERIES.getMetrics() );
        }

        return removeMetricsDisallowedByOtherConfig( config, returnMe );
    }

    /**
     * Adjusts a list of metrics, removing any that are not supported because the non-metric project configuration 
     * disallows them. For example, metrics that require a baseline cannot be computed when a baseline is not present.
     * 
     * @param config the project configuration
     * @param metrics the unconditional set of metrics
     * @return the adjusted set of metrics
     */

    private static Set<MetricConstants> removeMetricsDisallowedByOtherConfig( ProjectConfig config,
                                                                              Set<MetricConstants> metrics )
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( metrics, "Specify a non-null set of metrics to adjust." );

        Set<MetricConstants> returnMe = new HashSet<>( metrics );

        //Disallow non-score metrics when pooling window configuration is present, until this 
        //is supported
        PoolingWindowConfig windows = config.getPair().getIssuedDatesPoolingWindow();
        if ( Objects.nonNull( windows ) )
        {
            returnMe.removeIf( a -> ! ( a.isInGroup( MetricOutputGroup.DOUBLE_SCORE )
                                        || a.isInGroup( MetricOutputGroup.DURATION_SCORE ) ) );
        }

        //Remove CRPSS if no baseline is available
        DataSourceConfig baseline = config.getInputs().getBaseline();
        if ( Objects.isNull( baseline ) )
        {
            returnMe.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Mutates the input map of thresholds, adding probability classifiers for the specified metric configuration.
     * 
     * @param config the project configuration
     * @param returnMe the map of thresholds to mutate
     * @param metrics the metric configuration to use in mutating the input
     * @param dataFactory the data factory
     * @param external the optional external thresholds, may be null
     * @throws MetricConfigurationException if the configuration is invalid
     * @throws NullPointerException if any required input is null 
     */

    private static void addProbabilityClassifiers( ProjectConfig config,
                                                   Map<MetricConstants, Set<Threshold>> returnMe,
                                                   MetricsConfig metrics,
                                                   DataFactory dataFactory,
                                                   Map<MetricConfigName, ThresholdsByType> external )
            throws MetricConfigurationException
    {

        // Validate
        Objects.requireNonNull( returnMe, "Specify non-null input to mutate." );

        Objects.requireNonNull( metrics, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        Set<Threshold> thresholds = new HashSet<>();

        thresholds.addAll( fromInternalThresholdsConfig( metrics.getThresholds(),
                                                         null,
                                                         dataFactory,
                                                         ThresholdType.PROBABILITY_CLASSIFIER ) );

        // Filter by input type
        Set<MetricConstants> metricsToAdd = new HashSet<>();

        // Add ALL_VALID metrics
        if ( metrics.getMetric().stream().anyMatch( name -> name.getName() == MetricConfigName.ALL_VALID ) )
        {
            metricsToAdd.addAll( MetricConfigHelper.getAllValidMetricsFromConfig( config ) );
        }
        // Add named metrics
        else
        {
            for ( MetricConfig nextMetric : metrics.getMetric() )
            {
                metricsToAdd.add( MetricConfigHelper.from( nextMetric.getName() ) );
            }
        }

        // Filter everything except dichotomous
        metricsToAdd.removeIf( a -> !a.isInGroup( MetricInputGroup.DICHOTOMOUS ) );

        // Add or append
        for ( MetricConstants nextMetric : metricsToAdd )
        {
            if ( returnMe.containsKey( nextMetric ) )
            {
                returnMe.get( nextMetric ).addAll( thresholds );
            }
            else
            {
                returnMe.put( nextMetric, thresholds );
            }
            // Add external thresholds
            MetricConfigName name = from( nextMetric );
            if ( Objects.nonNull( external ) && external.containsKey( name ) )
            {
                returnMe.get( nextMetric )
                        .addAll( external.get( name )
                                         .getThresholdsByType( ThresholdsByType.ThresholdType.PROBABILITY_CLASSIFIER ) );
            }
        }

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
