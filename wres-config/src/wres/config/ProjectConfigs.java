package wres.config;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.ThresholdConstants;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.ThresholdsByMetric;
import wres.datamodel.ThresholdsByType;

/**
 * Provides static methods that help with ProjectConfig and its children.
 */

public class ProjectConfigs
{

    /**
     * Null configuration error.
     */

    private static final String NULL_CONFIGURATION_ERROR = "Specify non-null project configuration.";

    /**
     * Null mapping error.
     */

    private static final String NULL_CONFIGURATION_NAME_ERROR = "Specify input configuration with a "
                                                                + "non-null identifier to map.";

    /**
     * Map between {@link MetricConfigName} and {@link MetricConstants}.
     */

    private static final EnumMap<MetricConfigName, MetricConstants> METRIC_CONFIG_NAME_MAP =
            new EnumMap<>( MetricConfigName.class );

    /**
     * Map between {@link TimeSeriesMetricConfigName} and {@link MetricConstants}.
     */

    private static final EnumMap<TimeSeriesMetricConfigName, MetricConstants> TIME_SERIES_METRIC_CONFIG_NAME_MAP =
            new EnumMap<>( TimeSeriesMetricConfigName.class );

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
     * Returns a set of {@link MetricConstants} for a specific group of metrics contained in a {@link MetricsConfig}. 
     * If the {@link MetricsConfig} contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param metricsConfig the metric configuration
     * @param projectConfig the project configuration used to assist in identifying valid metrics
     * @return a set of metrics
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws NullPointerException if either input is null or no metrics are configured
     */

    public static Set<MetricConstants> getMetricsFromMetricsConfig( MetricsConfig metricsConfig,
                                                                    ProjectConfig projectConfig )
            throws MetricConfigException
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        returnMe.addAll( ProjectConfigs.getOrdinaryMetricsFromConfig( metricsConfig, projectConfig ) );

        returnMe.addAll( ProjectConfigs.getTimeSeriesMetricsFromConfig( metricsConfig, projectConfig ) );

        // Validate
        if ( returnMe.isEmpty() )
        {
            throw new MetricConfigException( projectConfig,
                                             "The project configuration does not contain any valid metrics." );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a set of time-series metrics within a particular group of metrics contained in a 
     * {@link MetricsConfig}. If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, 
     * all supported metrics are returned that are consistent with the configuration. 
     * 
     * @param metricsConfig the metrics configuration
     * @param projectConfig the project configuration used to assist in identifying valid metrics
     * @return a set of metrics
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    public static Set<MetricConstants> getTimeSeriesMetricsFromConfig( MetricsConfig metricsConfig,
                                                                       ProjectConfig projectConfig )
            throws MetricConfigException
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( TimeSeriesMetricConfig next : metricsConfig.getTimeSeriesMetric() )
        {
            // All valid metrics
            if ( next.getName() == TimeSeriesMetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                MetricInputGroup inGroup = ProjectConfigs.getMetricInputGroup( projectConfig.getInputs().getRight() );

                // Single-valued input source
                if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                {
                    allValid = ProjectConfigs.getAllValidMetricsForSingleValuedTimeSeriesInput( projectConfig,
                                                                                                metricsConfig );
                }
                // Unrecognized type
                else
                {
                    throw new MetricConfigException( next, "Unexpected input type for time-series metrics '"
                                                           + inGroup
                                                           + "'." );
                }

                returnMe.addAll( allValid );

                // Cannot be defined more than once in one metric group
                break;
            }
            else
            {
                returnMe.add( ProjectConfigs.getMetricName( next ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a set of non-time-series metrics within a particular group of metrics contained in a 
     * {@link MetricsConfig}. If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, 
     * all supported metrics are returned that are consistent with the configuration. 
     * 
     * @param metricsConfig the metrics configuration
     * @param projectConfig the project configuration used to assist in identifying valid metrics
     * @return a set of metrics
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    public static Set<MetricConstants> getOrdinaryMetricsFromConfig( MetricsConfig metricsConfig,
                                                                      ProjectConfig projectConfig )
            throws MetricConfigException
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( MetricConfig next : metricsConfig.getMetric() )
        {
            // All valid metrics
            if ( next.getName() == MetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                MetricInputGroup inGroup = ProjectConfigs.getMetricInputGroup( projectConfig.getInputs().getRight() );

                // Single-valued metrics
                if ( inGroup == MetricInputGroup.SINGLE_VALUED )
                {
                    allValid =
                            ProjectConfigs.getAllValidMetricsForSingleValuedInput( projectConfig, metricsConfig );
                }
                // Ensemble metrics
                else if ( inGroup == MetricInputGroup.ENSEMBLE )
                {
                    allValid = ProjectConfigs.getAllValidMetricsForEnsembleInput( projectConfig, metricsConfig );
                }
                // Unrecognized type
                else
                {
                    throw new MetricConfigException( next, "Unexpected input type for metrics '" + inGroup
                                                           + "'." );
                }

                returnMe.addAll( allValid );

                // Cannot be defined more than once in one metric group
                break;
            }
            else
            {
                returnMe.add( ProjectConfigs.getMetricName( next ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} in the input 
     * configuration or null if the input is {@link MetricConfigName#ALL_VALID}.
     * 
     * @param metricConfig the metric configuration
     * @return the mapped name
     * @throws MetricConfigException if the input name is not mapped
     * @throws NullPointerException if the input is null or the input name is null
     */

    public static MetricConstants getMetricName( MetricConfig metricConfig ) throws MetricConfigException
    {
        Objects.requireNonNull( metricConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( metricConfig.getName(), NULL_CONFIGURATION_NAME_ERROR );

        //All valid metrics
        if ( metricConfig.getName() == MetricConfigName.ALL_VALID )
        {
            return null;
        }

        // Lazy build the mapping
        buildMetricConfigNameMap();

        if ( !METRIC_CONFIG_NAME_MAP.containsKey( metricConfig.getName() ) )
        {
            throw new MetricConfigException( metricConfig, " Unable to find a metric with a configured identifier of "
                                                           + "'" + metricConfig.getName() + "'." );
        }

        return METRIC_CONFIG_NAME_MAP.get( metricConfig.getName() );
    }

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} in the input 
     * configuration or null if the input is {@link MetricConfigName#ALL_VALID}.
     * 
     * @param timeSeriesMetricConfig the metric configuration
     * @return the mapped name
     * @throws MetricConfigException if the input name is not mapped
     * @throws NullPointerException if the input is null or the input name is null
     */

    public static MetricConstants getMetricName( TimeSeriesMetricConfig timeSeriesMetricConfig )
            throws MetricConfigException
    {
        Objects.requireNonNull( timeSeriesMetricConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( timeSeriesMetricConfig.getName(), NULL_CONFIGURATION_NAME_ERROR );

        //All valid metrics
        if ( timeSeriesMetricConfig.getName() == TimeSeriesMetricConfigName.ALL_VALID )
        {
            return null;
        }

        // Lazy build the mapping
        buildTimeSeriesMetricConfigNameMap();

        if ( !TIME_SERIES_METRIC_CONFIG_NAME_MAP.containsKey( timeSeriesMetricConfig.getName() ) )
        {
            throw new MetricConfigException( timeSeriesMetricConfig, " Unable to find a metric with a configured "
                                                                     + "identifier of '"
                                                                     + timeSeriesMetricConfig.getName()
                                                                     + "'." );
        }

        return TIME_SERIES_METRIC_CONFIG_NAME_MAP.get( timeSeriesMetricConfig.getName() );
    }

    /**
     * Returns the {@link ThresholdConstants.ThresholdDataType} that corresponds to the {@link ThresholdDataType}
     * associated with the input configuration.
     * 
     * @param thresholdsConfig the thresholds configuration
     * @return the mapped threshold data type
     * @throws MetricConfigException if the data type is not mapped
     * @throws NullPointerException if the input is null or the {@link ThresholdsConfig#getApplyTo()} returns null
     */

    public static ThresholdConstants.ThresholdDataType getThresholdDataType( ThresholdsConfig thresholdsConfig )
            throws MetricConfigException
    {
        Objects.requireNonNull( thresholdsConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( thresholdsConfig.getApplyTo(), NULL_CONFIGURATION_NAME_ERROR );

        buildThresholdDataTypeMap();

        if ( !THRESHOLD_APPLICATION_TYPE_MAP.containsKey( thresholdsConfig.getApplyTo() ) )
        {
            throw new MetricConfigException( thresholdsConfig,
                                             " Unable to find a threshold application type with a configured "
                                                               + "identifier of '"
                                                               + thresholdsConfig.getApplyTo()
                                                               + "'." );
        }
        return THRESHOLD_APPLICATION_TYPE_MAP.get( thresholdsConfig.getApplyTo() );
    }

    /**
     * Returns the {@link ThresholdConstants.ThresholdGroup} that corresponds to the {@link ThresholdType}
     * associated with the input configuration. 
     * 
     * @param thresholdsConfig the thresholds configuration
     * @return the mapped threshold group
     * @throws MetricConfigException if the threshold group is not mapped
     * @throws NullPointerException if the input is null or the {@link ThresholdsConfig#getType()} returns null
     */

    public static ThresholdConstants.ThresholdGroup getThresholdGroup( ThresholdsConfig thresholdsConfig )
            throws MetricConfigException
    {
        Objects.requireNonNull( thresholdsConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( thresholdsConfig.getType(), NULL_CONFIGURATION_NAME_ERROR );

        buildThresholdTypeMap();

        if ( !THRESHOLD_TYPE_MAP.containsKey( thresholdsConfig.getType() ) )
        {
            throw new MetricConfigException( thresholdsConfig,
                                             " Unable to find a threshold type with a configured identifier "
                                                               + "of '" + thresholdsConfig.getType() + "'." );
        }
        return THRESHOLD_TYPE_MAP.get( thresholdsConfig.getType() );
    }

    /**
     * Maps between threshold operators in {@link ThresholdOperator} and those in {@link Operator}.
     * 
     * @param thresholdsConfig the threshold configuration
     * @return the mapped operator
     * @throws MetricConfigException if the operator is not mapped
     * @throws NullPointerException if the input is null or the {@link ThresholdsConfig#getOperator()} returns null
     */

    public static Operator getThresholdOperator( ThresholdsConfig thresholdsConfig ) throws MetricConfigException
    {
        Objects.requireNonNull( thresholdsConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( thresholdsConfig.getOperator(), NULL_CONFIGURATION_NAME_ERROR );

        switch ( thresholdsConfig.getOperator() )
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
                throw new MetricConfigException( thresholdsConfig,
                                                 "Unrecognized threshold operator in project configuration '"
                                                                   + thresholdsConfig.getOperator() + "'." );
        }
    }

    /**
     * Returns the metric data input type from the {@link DatasourceType}.
     * 
     * @param dataSourceConfig the data source configuration
     * @return the metric input group
     * @throws MetricConfigException if the input type is not mapped
     * @throws NullPointerException if the input is null or the {@link DataSourceConfig#getType()} returns null 
     */

    public static MetricInputGroup getMetricInputGroup( DataSourceConfig dataSourceConfig )
            throws MetricConfigException
    {
        Objects.requireNonNull( dataSourceConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataSourceConfig.getType(), NULL_CONFIGURATION_NAME_ERROR );

        switch ( dataSourceConfig.getType() )
        {
            case ENSEMBLE_FORECASTS:
                return MetricInputGroup.ENSEMBLE;
            case SINGLE_VALUED_FORECASTS:
            case SIMULATIONS:
                return MetricInputGroup.SINGLE_VALUED;
            default:
                throw new MetricConfigException( dataSourceConfig,
                                                 "Unable to interpret the input type '" + dataSourceConfig.getType()
                                                                   + "'." );
        }
    }

    /**
     * Returns <code>true</code> if the input configuration has time-series metrics, otherwise <code>false</code>.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration has time-series metrics, otherwise false
     * @throws NullPointerException if the input is null
     */

    public static boolean hasTimeSeriesMetrics( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        return projectConfig.getMetrics().stream().anyMatch( next -> !next.getTimeSeriesMetric().isEmpty() );
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

            if ( ProjectConfigs.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || ProjectConfigs.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
            {
                returnMe.addAll( MetricInputGroup.DISCRETE_PROBABILITY.getMetrics() );
            }

            // Allow dichotomous metrics when probability classifiers are defined
            if ( ProjectConfigs.hasThresholds( metricsConfig, ThresholdType.PROBABILITY_CLASSIFIER ) )
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

            if ( ProjectConfigs.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || ProjectConfigs.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
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
     * Builds the mapping between the {@link MetricConstants} and the {@link MetricConfigName} 
     */

    private static void buildMetricConfigNameMap()
    {
        //Lazy population
        if ( METRIC_CONFIG_NAME_MAP.isEmpty() )
        {
            //Match on enumerated name
            for ( MetricConfigName nextConfig : MetricConfigName.values() )
            {
                for ( MetricConstants nextMetric : MetricConstants.values() )
                {
                    if ( nextConfig.name().equals( nextMetric.name() ) )
                    {
                        METRIC_CONFIG_NAME_MAP.put( nextConfig, nextMetric );
                        break;
                    }
                }
            }
        }
    }

    /**
     * Builds the mapping between the {@link MetricConstants} and the {@link TimeSeriesMetricConfigName}.
     */

    private static void buildTimeSeriesMetricConfigNameMap()
    {
        //Lazy population
        if ( TIME_SERIES_METRIC_CONFIG_NAME_MAP.isEmpty() )
        {
            //Match on name
            for ( TimeSeriesMetricConfigName nextConfig : TimeSeriesMetricConfigName.values() )
            {
                for ( MetricConstants nextMetric : MetricConstants.values() )
                {
                    if ( nextConfig.name().equals( nextMetric.name() ) )
                    {
                        TIME_SERIES_METRIC_CONFIG_NAME_MAP.put( nextConfig, nextMetric );
                        break;
                    }
                }
            }
        }
    }

    /**
     * Builds the mapping between the {@link ThresholdsByMetric.ThresholdDataType} and the {@link ThresholdDataType}. 
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
     * Builds the mapping between the {@link ThresholdsByType.ThresholdGroup} and the {@link ThresholdType}.
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

    private ProjectConfigs()
    {
        // Prevent construction, this is a static helper class.
    }

}

