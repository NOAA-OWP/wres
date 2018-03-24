package wres.engine.statistics.metric.config;

import java.util.Arrays;
import java.util.Collection;
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
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.DataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdConstants;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.ThresholdsByMetric;
import wres.datamodel.ThresholdsByMetric.ThresholdsByMetricBuilder;
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
     * Error for null metric input identifier.
     */

    private static final String NULL_METRIC_INPUT_IDENTIFIER_ERROR =
            "Unable to map a null input identifier to a named metric.";

    /**
     * Map between {@link MetricConfigName} and {@link MetricConstants}.
     */

    private static final EnumMap<MetricConfigName, MetricConstants> NAME_MAP = new EnumMap<>( MetricConfigName.class );

    /**
     * Map between {@link TimeSeriesMetricConfigName} and {@link MetricConstants}.
     */

    private static final EnumMap<TimeSeriesMetricConfigName, MetricConstants> TIME_SERIES_NAME_MAP =
            new EnumMap<>( TimeSeriesMetricConfigName.class );

    /**
     * Map between {@link SummaryStatisticsName} and {@link MetricConstants}
     */

    private static final EnumMap<SummaryStatisticsName, MetricConstants> STATISTICS_NAME_MAP =
            new EnumMap<>( SummaryStatisticsName.class );

    /**
     * Map between {@link ThresholdDataType} and {@link ThresholdsByMetric.ThresholdDataType}.
     */

    private static final EnumMap<ThresholdDataType, ThresholdConstants.ThresholdDataType> THRESHOLD_APPLICATION_TYPE_MAP =
            new EnumMap<>( ThresholdDataType.class );

    /**
     * Map between {@link ThresholdType} and {@link ThresholdsByType.ThresholdGroup}.
     */

    private static final EnumMap<ThresholdType, ThresholdConstants.ThresholdGroup> THRESHOLD_TYPE_MAP =
            new EnumMap<>( ThresholdType.class );

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} or null if the input is
     * {@link MetricConfigName#ALL_VALID}. Throws an exception if no such mapping is available. 
     * 
     * @param configName the metric name
     * @return the corresponding name in the {@link MetricConstants}
     * @throws MetricConfigurationException if the configName is not mapped or the input is null
     */

    public static MetricConstants from( MetricConfigName configName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( configName ) )
        {
            throw new MetricConfigurationException( NULL_METRIC_INPUT_IDENTIFIER_ERROR );
        }

        buildMap();

        //All valid metrics
        if ( configName == MetricConfigName.ALL_VALID )
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
     * Returns the {@link MetricConstants} that corresponds to the {@link TimeSeriesMetricConfigName} or null if the 
     * input is {@link MetricConfigName#ALL_VALID}. Throws an exception if no such mapping is available. 
     * 
     * @param configName the time-series metric name
     * @return the corresponding name in the {@link MetricConstants}
     * @throws MetricConfigurationException if the configName is not mapped or the input is null
     */

    public static MetricConstants from( TimeSeriesMetricConfigName configName ) throws MetricConfigurationException
    {
        if ( Objects.isNull( configName ) )
        {
            throw new MetricConfigurationException( NULL_METRIC_INPUT_IDENTIFIER_ERROR );
        }

        buildTimeSeriesMap();

        //All valid metrics
        if ( configName == TimeSeriesMetricConfigName.ALL_VALID )
        {
            return null;
        }
        if ( !TIME_SERIES_NAME_MAP.containsKey( configName ) )
        {
            throw new MetricConfigurationException( " Unable to find a metric with a configured identifier of "
                                                    + "'" + configName + "'." );
        }
        return TIME_SERIES_NAME_MAP.get( configName );
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
            throw new MetricConfigurationException( NULL_METRIC_INPUT_IDENTIFIER_ERROR );
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
     * @param statsName the name of the summary statistic
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
     * Returns the {@link ThresholdConstants.ThresholdDataType} that corresponds to the {@link ThresholdDataType}. 
     * Throws an exception if no such mapping is available. 
     * 
     * @param type the threshold data type
     * @return the corresponding {@link ThresholdConstants.ThresholdDataType}
     * @throws MetricConfigurationException if the type is not mapped or the input is null
     */

    public static ThresholdConstants.ThresholdDataType from( ThresholdDataType type )
            throws MetricConfigurationException
    {
        if ( Objects.isNull( type ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a threshold applicaton "
                                                    + "type." );
        }

        buildThresholdDataTypeMap();

        if ( !THRESHOLD_APPLICATION_TYPE_MAP.containsKey( type ) )
        {
            throw new MetricConfigurationException( " Unable to find a threshold application type with a configured "
                                                    + "identifier of '" + type + "'." );
        }
        return THRESHOLD_APPLICATION_TYPE_MAP.get( type );
    }

    /**
     * Returns the {@link ThresholdConstants.ThresholdGroup} that corresponds to the {@link ThresholdType}. 
     * Throws an exception if no such mapping is available. 
     * 
     * @param type the threshold type
     * @return the corresponding {@link ThresholdConstants.ThresholdGroup}
     * @throws MetricConfigurationException if the type is not mapped or the input is null
     */

    public static ThresholdConstants.ThresholdGroup from( ThresholdType type ) throws MetricConfigurationException
    {
        if ( Objects.isNull( type ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a threshold type." );
        }

        buildThresholdTypeMap();

        if ( !THRESHOLD_TYPE_MAP.containsKey( type ) )
        {
            throw new MetricConfigurationException( " Unable to find a threshold type with a configured identifier "
                                                    + "of '" + type + "'." );
        }
        return THRESHOLD_TYPE_MAP.get( type );
    }

    /**
     * Maps between threshold operators in {@link ThresholdOperator} and those in {@link Operator}.
     * 
     * @param operator the threshold operator
     * @return the corresponding {@link Operator}.
     * @throws MetricConfigurationException if the configName is not mapped or the input is null
     */

    public static Operator from( ThresholdOperator operator ) throws MetricConfigurationException
    {
        if ( Objects.isNull( operator ) )
        {
            throw new MetricConfigurationException( "Unable to map a null input identifier to a name operator." );
        }
        switch ( operator )
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
                                                        + operator + "'." );
        }
    }

    /**
     * Returns the metric data input type from the {@link DatasourceType}.
     * 
     * @param type the data source type
     * @return the {@link MetricInputGroup} based on the {@link ProjectConfig}
     * @throws MetricConfigurationException if the input type is not recognized
     */

    public static MetricInputGroup from( DatasourceType type ) throws MetricConfigurationException
    {
        Objects.requireNonNull( type, NULL_CONFIGURATION_ERROR );

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
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        returnMe.addAll( MetricConfigHelper.getOrdinaryMetricsFromConfig( config ) );

        returnMe.addAll( MetricConfigHelper.getTimeSeriesMetricsFromConfig( config ) );

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a set of {@link MetricConstants} from a particular metric group in the {@link ProjectConfig}. 
     * If the {@link ProjectConfig} contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @param metrics the metric configuration
     * @return a set of {@link MetricConstants}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws NullPointerException if either input is null
     */

    public static Set<MetricConstants> getMetricsFromConfig( ProjectConfig config, MetricsConfig metrics )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( metrics, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        returnMe.addAll( MetricConfigHelper.getOrdinaryMetricsFromConfig( config, metrics ) );

        returnMe.addAll( MetricConfigHelper.getTimeSeriesMetricsFromConfig( config, metrics ) );

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Reads the internally configured thresholds, combines them with any supplied, external, thresholds, and 
     * returns the union of all thresholds.
     * 
     * @param projectConfig the project configuration with internally configured thresholds
     * @param dataFactory the data factory from which to build thresholds
     * @param external an optional source of external thresholds, may be null
     * @return the union of internal and external thresholds
     * @throws MetricConfigurationException if the metric configuration is invalid
     * @throws NullPointerException if the projectConfig or dataFactory is null
     */

    public static ThresholdsByMetric getThresholdsFromConfig( ProjectConfig projectConfig,
                                                              DataFactory dataFactory,
                                                              Map<MetricConfigName, ThresholdsByType> external )
            throws MetricConfigurationException
    {
        if ( Objects.nonNull( external ) )
        {
            return MetricConfigHelper.getThresholdsFromConfig( projectConfig, dataFactory, Arrays.asList( external ) );
        }

        return MetricConfigHelper.getThresholdsFromConfig( projectConfig,
                                                           dataFactory,
                                                           (Collection<Map<MetricConfigName, ThresholdsByType>>) null );
    }

    /**
     * Reads the internally configured thresholds, combines them with any supplied, external, thresholds, and 
     * returns the union of all thresholds.
     * 
     * @param projectConfig the project configuration with internally configured thresholds
     * @param dataFactory the data factory from which to build thresholds
     * @param external an optional source of external thresholds, may be null
     * @return the union of internal and external thresholds
     * @throws MetricConfigurationException if the metric configuration is invalid
     * @throws NullPointerException if the projectConfig or dataFactory is null
     */

    public static ThresholdsByMetric getThresholdsFromConfig( ProjectConfig projectConfig,
                                                              DataFactory dataFactory,
                                                              Collection<Map<MetricConfigName, ThresholdsByType>> external )
            throws MetricConfigurationException
    {

        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        // Find the units associated with the pairs
        Dimension units = null;
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            units = dataFactory.getMetadataFactory().getDimension( projectConfig.getPair().getUnit() );
        }

        // Builder 
        ThresholdsByMetricBuilder builder = dataFactory.ofThresholdsByMetricBuilder();

        // Iterate through the metric groups
        for ( MetricsConfig nextGroup : projectConfig.getMetrics() )
        {

            MetricConfigHelper.addThresholdsToBuilderForOneMetricGroup( projectConfig,
                                                                        nextGroup,
                                                                        builder,
                                                                        units,
                                                                        dataFactory );
        }

        return MetricConfigHelper.getUnionOfInternalAndExternalThresholds( builder.build(), external, dataFactory );
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
     * @deprecated
     */

    @Deprecated
    public static Set<Threshold> fromInternalThresholdsConfig( List<ThresholdsConfig> thresholds,
                                                               Dimension units,
                                                               DataFactory dataFactory,
                                                               ThresholdType... types )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( thresholds, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        Set<Threshold> returnMe = new HashSet<>();

        // Iterate and transform
        for ( ThresholdsConfig next : thresholds )
        {
            returnMe.addAll( MetricConfigHelper.fromInternalThresholdsConfig( next, units, dataFactory, types ) );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Transforms a {@link ThresholdsConfig} to a set of {@link Threshold}.
     * 
     * @param thresholds the thresholds configuration
     * @param units optional units for non-probability thresholds
     * @param dataFactory the data factory with which to build thresholds
     * @param types an optional list of threshold types to read
     * @return a set of thresholds (possibly empty)
     * @throws MetricConfigurationException if the metric configuration is invalid
     * @throws NullPointerException if either input is null
     * @deprecated
     */

    @Deprecated
    private static Set<Threshold> fromInternalThresholdsConfig( ThresholdsConfig thresholds,
                                                                Dimension units,
                                                                DataFactory dataFactory,
                                                                ThresholdType... types )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( thresholds, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        Set<Threshold> returnMe = new HashSet<>();

        Operator operator = Operator.GREATER;

        // Operator specified
        if ( Objects.nonNull( thresholds.getOperator() ) )
        {
            operator = MetricConfigHelper.from( thresholds.getOperator() );
        }
        
        ThresholdConstants.ThresholdDataType dataType = ThresholdConstants.ThresholdDataType.LEFT;

        // Operator specified
        if ( Objects.nonNull( thresholds.getApplyTo() ) )
        {
            dataType = MetricConfigHelper.from( thresholds.getApplyTo() );
        }

        // Must be internally sourced: thresholds with global scope should be provided directly 
        Object values = thresholds.getCommaSeparatedValuesOrSource();

        // Default to ThresholdType.PROBABILITY
        ThresholdType type = ThresholdType.PROBABILITY;
        if ( Objects.nonNull( thresholds.getType() ) )
        {
            type = thresholds.getType();
        }

        // String = internal sourced
        if ( values instanceof String
             && ( types.length == 0
                  || Arrays.asList( types ).contains( type ) ) )
        {
            returnMe.addAll( MetricConfigHelper.getThresholdsFromCommaSeparatedValues( dataFactory,
                                                                                       values.toString(),
                                                                                       operator,
                                                                                       dataType,
                                                                                       type != ThresholdType.VALUE,
                                                                                       units ) );
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
     * @throws MetricConfigurationException if the configuration is invalid
     */

    public static boolean hasSummaryStatisticsFor( ProjectConfig config, TimeSeriesMetricConfigName metric )
            throws MetricConfigurationException
    {
        return MetricConfigHelper.getSummaryStatisticsFor( config, metric ).length > 0;
    }

    /**
     * Returns a list of summary statistics associated with the named metric.
     * 
     * @param config the project configuration
     * @param metric the metric whose summary statistics are required
     * @return the summary statistics associated with the named metric
     * @throws MetricConfigurationException if the project contains an unmapped summary statistic
     */

    public static MetricConstants[] getSummaryStatisticsFor( ProjectConfig config, TimeSeriesMetricConfigName metric )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, "Specify a non-null project configuration to check for summary statistics" );

        Objects.requireNonNull( metric, "Specify a non null metric name to check for summary statistics." );

        Set<MetricConstants> allStats = new HashSet<>();

        // Iterate the metric groups
        for ( MetricsConfig nextGroup : config.getMetrics() )
        {
            // Iterate the time-series metrics
            for ( TimeSeriesMetricConfig next : nextGroup.getTimeSeriesMetric() )
            {
                // Match the name
                if ( next.getName() == metric && Objects.nonNull( next.getSummaryStatistics() ) )
                {
                    // Return the summary statistics
                    for ( SummaryStatisticsName nextStat : next.getSummaryStatistics().getName() )
                    {
                        allStats.add( from( nextStat ) );
                    }
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
     * Returns <code>true</code> if the project configuration contains thresholds, <code>false</code> otherwise.
     * 
     * @param config the project configuration
     * @param type an optional threshold type, may be null
     * @return true if the configuration contains thresholds, otherwise false
     * @throws NullPointerException if the input is null
     */

    private static boolean hasThresholds( MetricsConfig config, ThresholdType type )
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        // No type condition
        if ( Objects.isNull( type ) )
        {
            // Has some thresholds
            return !config.getThresholds().isEmpty();
        }

        // No explicit type condition, defaults to ThresholdType.PROBABILITY
        ThresholdType defaultType = ThresholdType.PROBABILITY;

        // Has some thresholds of a specified type
        return config.getThresholds()
                     .stream()
                     .anyMatch( testType -> testType.getType() == type
                                            || ( Objects.isNull( testType.getType() ) && type == defaultType ) );
    }

    /**
     * Returns a list of {@link Threshold} from a comma-separated string. Specify the type of {@link Threshold}
     * required.
     * 
     * @param dataFactory a factory for building thresholds
     * @param inputString the comma-separated input string
     * @param oper the operator
     * @param dataType the data type to which the threshold applies
     * @param areProbs is true to generate probability thresholds, false for ordinary thresholds
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     * @throws MetricConfigurationException if the thresholds are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<Threshold> getThresholdsFromCommaSeparatedValues( DataFactory dataFactory,
                                                                         String inputString,
                                                                         Operator oper,
                                                                         ThresholdConstants.ThresholdDataType dataType,
                                                                         boolean areProbs,
                                                                         Dimension units )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        Objects.requireNonNull( inputString, "Specify a non-null input string." );

        Objects.requireNonNull( oper, "Specify a non-null operator." );

        Objects.requireNonNull( dataType, "Specify a non-null data type." );

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
                                                                      dataType,
                                                                      units ) );
                }
                else
                {
                    returnMe.add( dataFactory.ofThreshold( dataFactory.ofOneOrTwoDoubles( addMe.get( i ),
                                                                                          addMe.get( i + 1 ) ),
                                                           oper,
                                                           dataType,
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
                                                                                              dataType,
                                                                                              units ) ) );
            }
            else
            {
                addMe.forEach( threshold -> returnMe.add( dataFactory.ofThreshold( dataFactory.ofOneOrTwoDoubles( threshold ),
                                                                                   oper,
                                                                                   dataType,
                                                                                   units ) ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns an adjusted set of thresholds for the input thresholds. Adjustments are made for the specific metric.
     * Notably, an "all data" threshold is added for metrics that support this, and thresholds are removed for metrics
     * that do not support them.
     * 
     * @param metric the metric
     * @param thresholds the raw thresholds
     * @param type the threshold type
     * @param dataFactory a data factory
     * @return the adjusted thresholds
     * @throws NullPointerException if either input is null
     */

    private static Set<Threshold> getAdjustedThresholds( MetricConstants metric,
                                                         Set<Threshold> thresholds,
                                                         ThresholdConstants.ThresholdGroup type,
                                                         DataFactory dataFactory )
    {
        Objects.requireNonNull( metric, "Specify a non-null metric." );

        Objects.requireNonNull( thresholds, "Specify non-null thresholds." );

        Objects.requireNonNull( thresholds, "Specify a non-null threshold type." );

        Objects.requireNonNull( thresholds, NULL_DATA_FACTORY_ERROR );

        if ( type == ThresholdConstants.ThresholdGroup.PROBABILITY_CLASSIFIER )
        {
            return thresholds;
        }

        Set<Threshold> returnMe = new HashSet<>();

        Threshold allData =
                dataFactory.ofThreshold( dataFactory.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                         Operator.GREATER,
                                         ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT );

        // All data only
        if ( metric.getMetricOutputGroup() == MetricOutputGroup.BOXPLOT
             || metric == MetricConstants.QUANTILE_QUANTILE_DIAGRAM )
        {
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( allData ) ) );
        }

        // Otherwise, add the input thresholds
        returnMe.addAll( thresholds );

        // Add all data for appropriate types
        if ( metric.isInGroup( MetricInputGroup.ENSEMBLE ) || metric.isInGroup( MetricInputGroup.SINGLE_VALUED )
             || metric.isInGroup( MetricInputGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            returnMe.add( allData );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns {@link MetricConstants} from the input configuration for metrics that are not time-series. 
     * If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a set of metrics
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getOrdinaryMetricsFromConfig( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        // Iterate through the ordinary metrics and populate the map
        for ( MetricsConfig metrics : config.getMetrics() )
        {
            returnMe.addAll( MetricConfigHelper.getOrdinaryMetricsFromConfig( config, metrics ) );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns {@link MetricConstants} from the input configuration for metrics that are not time-series. 
     * If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @param metrics the metrics configuration
     * @return a set of metrics
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getOrdinaryMetricsFromConfig( ProjectConfig config, MetricsConfig metrics )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( MetricConfig next : metrics.getMetric() )
        {
            // All valid metrics
            if ( next.getName() == MetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                MetricInputGroup inGroup = MetricConfigHelper.from( config.getInputs().getRight().getType() );

                // Single-valued metrics
                if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                {
                    allValid = MetricConfigHelper.getAllValidMetricsForSingleValuedInput( config, metrics );
                }
                // Ensemble metrics
                else if ( inGroup == MetricInputGroup.ENSEMBLE )
                {
                    allValid = MetricConfigHelper.getAllValidMetricsForEnsembleInput( config, metrics );
                }
                // Unrecognized type
                else
                {
                    throw new MetricConfigurationException( "Unexpected input type for metrics '" + inGroup
                                                            + "'." );
                }

                returnMe.addAll( allValid );

                // Cannot be defined more than once in one metric group
                break;
            }
            else
            {
                returnMe.add( from( next.getName() ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a set of{@link MetricConstants} associated with time-series metrics in the input configuration. 
     * If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics are 
     * returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a set of metrics
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getTimeSeriesMetricsFromConfig( ProjectConfig config )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        // Iterate through the metric groups
        for ( MetricsConfig metrics : config.getMetrics() )
        {
            returnMe.addAll( MetricConfigHelper.getTimeSeriesMetricsFromConfig( config, metrics ) );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a set of{@link MetricConstants} associated with time-series metrics in the input configuration. 
     * If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics are 
     * returned that are consistent with the configuration. 
     * 
     * @param config the project configuration
     * @return a set of metrics
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getTimeSeriesMetricsFromConfig( ProjectConfig config, MetricsConfig metrics )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( TimeSeriesMetricConfig next : metrics.getTimeSeriesMetric() )
        {
            // All valid metrics
            if ( next.getName() == TimeSeriesMetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                MetricInputGroup inGroup = MetricConfigHelper.from( config.getInputs().getRight().getType() );

                // Single-valued input source
                if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                {
                    allValid = MetricConfigHelper.getAllValidMetricsForSingleValuedTimeSeriesInput( config, metrics );
                }
                // Unrecognized type
                else
                {
                    throw new MetricConfigurationException( "Unexpected input type for time-series metrics '"
                                                            + inGroup
                                                            + "'." );
                }

                returnMe.addAll( allValid );

                // Cannot be defined more than once in one metric group
                break;
            }
            else
            {
                returnMe.add( from( next.getName() ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#ENSEMBLE}
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @return the valid metrics for {@link MetricInputGroup#ENSEMBLE}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getAllValidMetricsForEnsembleInput( ProjectConfig projectConfig,
                                                                            MetricsConfig metricsConfig )
    {
        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        if ( !metricsConfig.getMetric().isEmpty() )
        {
            returnMe.addAll( MetricInputGroup.ENSEMBLE.getMetrics() );
            returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );

            if ( MetricConfigHelper.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || MetricConfigHelper.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
            {
                returnMe.addAll( MetricInputGroup.DISCRETE_PROBABILITY.getMetrics() );
            }

            // Allow dichotomous metrics when probability classifiers are defined
            if ( MetricConfigHelper.hasThresholds( metricsConfig, ThresholdType.PROBABILITY_CLASSIFIER ) )
            {
                returnMe.addAll( MetricInputGroup.DICHOTOMOUS.getMetrics() );
            }
        }

        return removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
    }

    /**
     * Returns valid ordinary metrics for {@link MetricInputGroup#SINGLE_VALUED}. Also see:
     * {@link #getValidMetricsForSingleValuedInput(ProjectConfig, MetricsConfig)}
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @return the valid metrics for {@link MetricInputGroup#SINGLE_VALUED}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getAllValidMetricsForSingleValuedInput( ProjectConfig projectConfig,
                                                                                MetricsConfig metricsConfig )
    {
        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        if ( !metricsConfig.getMetric().isEmpty() )
        {
            returnMe.addAll( MetricInputGroup.SINGLE_VALUED.getMetrics() );

            if ( MetricConfigHelper.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || MetricConfigHelper.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
            {
                returnMe.addAll( MetricInputGroup.DICHOTOMOUS.getMetrics() );
            }
        }

        return removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
    }

    /**
     * Returns valid metrics for {@link MetricInputGroup#SINGLE_VALUED_TIME_SERIES}
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @return the valid metrics for {@link MetricInputGroup#SINGLE_VALUED_TIME_SERIES}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getAllValidMetricsForSingleValuedTimeSeriesInput( ProjectConfig projectConfig,
                                                                                          MetricsConfig metricsConfig )
    {
        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        //Add time-series metrics if required
        if ( !metricsConfig.getTimeSeriesMetric().isEmpty() )
        {
            returnMe.addAll( MetricInputGroup.SINGLE_VALUED_TIME_SERIES.getMetrics() );
        }

        return removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
    }

    /**
     * Adjusts a list of metrics, removing any that are not supported because the non-metric project configuration 
     * disallows them. For example, metrics that require a baseline cannot be computed when a baseline is not present.
     * 
     * @param projectConfig the project configuration
     * @param metrics the unconditional set of metrics
     * @return the adjusted set of metrics
     */

    private static Set<MetricConstants> removeMetricsDisallowedByOtherConfig( ProjectConfig projectConfig,
                                                                              Set<MetricConstants> metrics )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( metrics, "Specify a non-null set of metrics to adjust." );

        Set<MetricConstants> returnMe = new HashSet<>( metrics );

        //Disallow non-score metrics when pooling window configuration is present, until this 
        //is supported
        PoolingWindowConfig windows = projectConfig.getPair().getIssuedDatesPoolingWindow();
        if ( Objects.nonNull( windows ) )
        {
            returnMe.removeIf( a -> ! ( a.isInGroup( MetricOutputGroup.DOUBLE_SCORE )
                                        || a.isInGroup( MetricOutputGroup.DURATION_SCORE ) ) );
        }

        //Remove CRPSS if no baseline is available
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();
        if ( Objects.isNull( baseline ) )
        {
            returnMe.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Adds thresholds to the input builder for one metric group.
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @param builder the builder
     * @param units the optional units for value thresholds
     * @param dataFactory the data factory
     * @throws MetricConfigurationException if the threshold configuration is incorrect
     */

    private static void addThresholdsToBuilderForOneMetricGroup( ProjectConfig projectConfig,
                                                                 MetricsConfig metricsConfig,
                                                                 ThresholdsByMetricBuilder builder,
                                                                 Dimension units,
                                                                 DataFactory dataFactory )
            throws MetricConfigurationException
    {

        // Find the metrics
        Set<MetricConstants> metrics = MetricConfigHelper.getMetricsFromConfig( projectConfig, metricsConfig );

        // No explicit thresholds, add an "all data" threshold
        if ( metricsConfig.getThresholds().isEmpty() )
        {
            for ( MetricConstants next : metrics )
            {
                Set<Threshold> allData = MetricConfigHelper.getAdjustedThresholds( next,
                                                                                   Collections.emptySet(),
                                                                                   ThresholdConstants.ThresholdGroup.VALUE,
                                                                                   dataFactory );

                builder.addThresholds( Collections.singletonMap( next, allData ),
                                       ThresholdConstants.ThresholdGroup.VALUE );
            }
        }

        // Iterate through any explicit thresholds
        for ( ThresholdsConfig nextThresholds : metricsConfig.getThresholds() )
        {
            // Thresholds
            Set<Threshold> thresholds =
                    MetricConfigHelper.fromInternalThresholdsConfig( nextThresholds, units, dataFactory );

            // Build the thresholds map per metric
            Map<MetricConstants, Set<Threshold>> thresholdsMap = new EnumMap<>( MetricConstants.class );

            // Type of thresholds
            ThresholdConstants.ThresholdGroup thresholdType = ThresholdConstants.ThresholdGroup.PROBABILITY;
            if ( Objects.nonNull( nextThresholds.getType() ) )
            {
                thresholdType = MetricConfigHelper.from( nextThresholds.getType() );
            }

            // Adjust the thresholds, adding "all data" where required, then append
            for ( MetricConstants next : metrics )
            {
                thresholdsMap.put( next,
                                   MetricConfigHelper.getAdjustedThresholds( next,
                                                                             thresholds,
                                                                             thresholdType,
                                                                             dataFactory ) );
            }

            // Add the thresholds
            builder.addThresholds( thresholdsMap, thresholdType );
        }
    }

    /**
     * Returns the union of the internal and external thresholds. 
     * 
     * @param internalThresholds the configured thresholds
     * @param externalThresholds the supplied thresholds
     * @param dataFactory the data factory
     * @return the union of the internal and any external thresholds
     * @throws MetricConfigurationException if the metric configuration is invalid
     * @throws NullPointerException if either the internal thresholds or data factory is null
     */

    private static ThresholdsByMetric
            getUnionOfInternalAndExternalThresholds( ThresholdsByMetric internalThresholds,
                                                     Collection<Map<MetricConfigName, ThresholdsByType>> externalThresholds,
                                                     DataFactory dataFactory )
                    throws MetricConfigurationException
    {
        Objects.requireNonNull( internalThresholds, "Specify non-null internal thresholds." );

        Objects.requireNonNull( dataFactory, NULL_DATA_FACTORY_ERROR );

        if ( Objects.isNull( externalThresholds ) )
        {
            return internalThresholds;
        }

        ThresholdsByMetricBuilder builder = dataFactory.ofThresholdsByMetricBuilder();

        Set<MetricConstants> existing = internalThresholds.hasThresholdsForTheseMetrics();

        for ( Map<MetricConfigName, ThresholdsByType> nextMap : externalThresholds )
        {
            for ( Entry<MetricConfigName, ThresholdsByType> nextEntry : nextMap.entrySet() )
            {
                MetricConfigName nextName = nextEntry.getKey();

                if ( nextName == MetricConfigName.ALL_VALID )
                {
                    for ( MetricConstants nextMetric : existing )
                    {
                        MetricConfigHelper.addExternalThresholdsToThisStore( builder,
                                                                             nextMetric,
                                                                             nextEntry.getValue() );
                    }
                }
                else
                {
                    MetricConfigHelper.addExternalThresholdsToThisStore( builder,
                                                                         MetricConfigHelper.from( nextName ),
                                                                         nextEntry.getValue() );
                }
            }
        }

        return builder.build().unionWithThisStore( internalThresholds );
    }

    /**
     * Adds a set of external thresholds to the specified builder.
     * 
     * @param builder the builder
     * @param metric the metric to which the thresholds refer
     * @param thresholds the external thresholds
     */

    private static void addExternalThresholdsToThisStore( ThresholdsByMetricBuilder builder,
                                                          MetricConstants metric,
                                                          ThresholdsByType external )
    {
        Objects.requireNonNull( builder, "Specify a non-null builder." );

        Objects.requireNonNull( builder, "Specify a non-null metric." );

        Objects.requireNonNull( builder, "Specify a non-null source of external thresholds." );

        for ( ThresholdConstants.ThresholdGroup nextType : external.getAllThresholdTypes() )
        {
            Map<MetricConstants, Set<Threshold>> input =
                    Collections.singletonMap( metric, external.getThresholdsByType( nextType ) );
            builder.addThresholds( input, nextType );
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
     * Builds the mapping between the {@link MetricConstants} and the {@link TimeSeriesMetricConfigName} 
     */

    private static void buildTimeSeriesMap()
    {
        //Lazy population
        if ( TIME_SERIES_NAME_MAP.isEmpty() )
        {
            //Match on name
            for ( TimeSeriesMetricConfigName nextConfig : TimeSeriesMetricConfigName.values() )
            {
                for ( MetricConstants nextMetric : MetricConstants.values() )
                {
                    if ( nextConfig.name().equals( nextMetric.name() ) )
                    {
                        TIME_SERIES_NAME_MAP.put( nextConfig, nextMetric );
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
     * Builds the mapping between the {@link ThresholdsByMetric.ThresholdDataType} and the {@link ThresholdDataType} 
     */

    private static void buildThresholdDataTypeMap()
    {
        // Lazy population
        if ( THRESHOLD_APPLICATION_TYPE_MAP.isEmpty() )
        {
            // Iterate the external types
            for ( ThresholdDataType nextExternalType : ThresholdDataType.values() )
            {
                // Iterate the internal types
                for ( ThresholdConstants.ThresholdDataType nextInternalType : ThresholdConstants.ThresholdDataType.values() )
                {
                    if ( nextExternalType.name().equals( nextInternalType.name() ) )
                    {
                        THRESHOLD_APPLICATION_TYPE_MAP.put( nextExternalType, nextInternalType );
                        break;
                    }
                }
            }
        }
    }

    /**
     * Builds the mapping between the {@link ThresholdsByType.ThresholdGroup} and the {@link ThresholdType} 
     */

    private static void buildThresholdTypeMap()
    {
        // Lazy population
        if ( THRESHOLD_TYPE_MAP.isEmpty() )
        {
            // Iterate the external types
            for ( ThresholdType nextExternalType : ThresholdType.values() )
            {
                // Iterate the internal types
                for ( ThresholdConstants.ThresholdGroup nextInternalType : ThresholdConstants.ThresholdGroup.values() )
                {
                    if ( nextExternalType.name().equals( nextInternalType.name() ) )
                    {
                        THRESHOLD_TYPE_MAP.put( nextExternalType, nextInternalType );
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
