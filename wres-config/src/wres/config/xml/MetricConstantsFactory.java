package wres.config.xml;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdType;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.config.MetricConstants.MetricGroup;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;

/**
 * Factory class for creating {@link MetricConstants} from project declaration.
 * 
 * @author James Brown
 */

public class MetricConstantsFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger( MetricConstantsFactory.class );

    /** Map between {@link MetricConfigName} and {@link MetricConstants}. */
    private static final EnumMap<MetricConfigName, MetricConstants> NAME_MAP = new EnumMap<>( MetricConfigName.class );

    /** Map between {@link SummaryStatisticsName} and {@link MetricConstants}. */
    private static final EnumMap<SummaryStatisticsName, MetricConstants> STATISTICS_NAME_MAP =
            new EnumMap<>( SummaryStatisticsName.class );

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

    public static Set<MetricConstants> getMetricsFromConfig( MetricsConfig metricsConfig,
                                                             ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        Objects.requireNonNull( metricsConfig );

        Set<MetricConstants> returnMe = new HashSet<>();

        returnMe.addAll( MetricConstantsFactory.getOrdinaryMetricsFromConfig( metricsConfig, projectConfig ) );

        returnMe.addAll( MetricConstantsFactory.getTimeSeriesMetricsFromConfig( metricsConfig, projectConfig ) );

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
    {
        Objects.requireNonNull( projectConfig );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( TimeSeriesMetricConfig next : metricsConfig.getTimeSeriesMetric() )
        {
            // All valid metrics
            if ( next.getName() == TimeSeriesMetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                SampleDataGroup inGroup =
                        MetricConstantsFactory.getMetricInputGroup( projectConfig.getInputs().getRight() );

                // Single-valued input source
                if ( inGroup == SampleDataGroup.SINGLE_VALUED )
                {
                    allValid = MetricConstantsFactory.getAllValidMetricsForSingleValuedTimeSeriesInput( projectConfig,
                                                                                                        metricsConfig );
                }
                // Unrecognized type
                else
                {
                    throw new MetricConfigException( next,
                                                     "Unexpected input type for time-series metrics '"
                                                           + inGroup
                                                           + "'." );
                }

                returnMe.addAll( allValid );

                // Cannot be defined more than once in one metric group
                break;
            }
            else
            {
                MetricConstants metricName = MetricConstantsFactory.getMetricName( next.getName() );
                returnMe.add( metricName );
                Set<MetricConstants> summaryStatistics = MetricConstantsFactory.getSummaryStatisticsFor( next );
                returnMe.addAll( summaryStatistics );
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
    {
        Objects.requireNonNull( projectConfig );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( MetricConfig next : metricsConfig.getMetric() )
        {
            // All valid metrics
            if ( next.getName() == MetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                SampleDataGroup inGroup =
                        MetricConstantsFactory.getMetricInputGroup( projectConfig.getInputs().getRight() );

                // Single-valued metrics
                if ( inGroup == SampleDataGroup.SINGLE_VALUED )
                {
                    allValid =
                            MetricConstantsFactory.getAllValidMetricsForSingleValuedInput( projectConfig,
                                                                                           metricsConfig );
                }
                // Ensemble metrics
                else if ( inGroup == SampleDataGroup.ENSEMBLE )
                {
                    allValid =
                            MetricConstantsFactory.getAllValidMetricsForEnsembleInput( projectConfig, metricsConfig );
                }
                // Unrecognized type
                else
                {
                    throw new MetricConfigException( next,
                                                     "Unexpected input type for metrics '" + inGroup
                                                           + "'." );
                }

                returnMe.addAll( allValid );

                // Cannot be defined more than once in one metric group
                break;
            }
            else
            {
                MetricConstants metricName = MetricConstantsFactory.getMetricName( next.getName() );
                returnMe.add( metricName );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} in the input 
     * configuration or null if the input is {@link MetricConfigName#ALL_VALID}. Matches the enumerations by 
     * {@link Enum#name()}.
     * 
     * @param metricConfigName the metric name
     * @return the mapped name
     * @throws IllegalArgumentException if the input name is not mapped
     * @throws NullPointerException if the input is null
     */

    public static MetricConstants getMetricName( MetricConfigName metricConfigName )
    {
        Objects.requireNonNull( metricConfigName );

        //All valid metrics
        if ( metricConfigName == MetricConfigName.ALL_VALID )
        {
            return null;
        }

        return MetricConstants.valueOf( metricConfigName.name() );
    }

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} in the input 
     * configuration or null if the input is {@link MetricConfigName#ALL_VALID}. Matches the enumerations by 
     * {@link Enum#name()}.
     * 
     * @param timeSeriesMetricConfigName the metric name
     * @return the mapped name
     * @throws IllegalArgumentException if the input name is not mapped
     * @throws NullPointerException if the input name is null
     */

    public static MetricConstants getMetricName( TimeSeriesMetricConfigName timeSeriesMetricConfigName )
    {
        Objects.requireNonNull( timeSeriesMetricConfigName );

        //All valid metrics
        if ( timeSeriesMetricConfigName == TimeSeriesMetricConfigName.ALL_VALID )
        {
            return null;
        }

        return MetricConstants.valueOf( timeSeriesMetricConfigName.name() );
    }

    /**
     * <p>Returns the metric data input type from the {@link DatasourceType}.
     * 
     * <p>TODO: make these enumerations match on name to reduce brittleness.
     * 
     * @param dataSourceConfig the data source configuration
     * @return the metric input group
     * @throws MetricConfigException if the input type is not mapped
     * @throws NullPointerException if the input is null or the {@link DataSourceConfig#getType()} returns null 
     */

    public static SampleDataGroup getMetricInputGroup( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );
        Objects.requireNonNull( dataSourceConfig.getType() );

        return switch ( dataSourceConfig.getType() )
                {
                    case ENSEMBLE_FORECASTS -> SampleDataGroup.ENSEMBLE;
                    case SINGLE_VALUED_FORECASTS, SIMULATIONS -> SampleDataGroup.SINGLE_VALUED;
                    default -> throw new MetricConfigException( dataSourceConfig,
                                                                "Unable to interpret the input type '"
                                                                + dataSourceConfig.getType()
                                                                + "'." );
                };
    }

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link MetricConfigName} or null if the input is
     * {@link MetricConfigName#ALL_VALID}. Throws an exception if no such mapping is available. 
     * 
     * @param metricConfigName the metric configuration name
     * @return the corresponding name in the {@link MetricConstants}
     * @throws MetricConfigException if the input name is not mapped
     * @throws NullPointerException if the input is null
     */

    public static MetricConstants from( MetricConfigName metricConfigName )
    {

        Objects.requireNonNull( metricConfigName );

        //All valid metrics
        if ( metricConfigName == MetricConfigName.ALL_VALID )
        {
            return null;
        }

        // Lazy build the mapping
        MetricConstantsFactory.buildMetricConfigNameMap();

        return NAME_MAP.get( metricConfigName );
    }

    /**
     * Returns the {@link MetricConstants} that corresponds to the {@link SummaryStatisticsName} or null if the input is
     * {@link SummaryStatisticsName#ALL_VALID}. Throws an exception if no such mapping is available. 
     * 
     * @param statsName the summary statistic name
     * @return the mapped name
     * @throws MetricConfigException if the input configuration is not mapped
     * @throws NullPointerException if the input is null
     */

    public static MetricConstants from( SummaryStatisticsName statsName )
    {

        Objects.requireNonNull( statsName );

        //All valid metrics
        if ( statsName.equals( SummaryStatisticsName.ALL_VALID ) )
        {
            return null;
        }

        // Lazy build the name map
        MetricConstantsFactory.buildSummaryStatisticsNameMap();

        return STATISTICS_NAME_MAP.get( statsName );
    }

    /**
     * Returns a set of {@link MetricConstants} from a {@link ProjectConfig}. If the {@link ProjectConfig} contains
     * the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics are returned that are consistent
     * with the configuration. 
     * 
     * @param config the project configuration
     * @return a set of metrics
     * @throws MetricConfigException if the metrics are configured incorrectly
     */

    public static Set<MetricConstants> getMetricsFromConfig( ProjectConfig config )
    {
        Objects.requireNonNull( config );

        Set<MetricConstants> returnMe = new HashSet<>();

        returnMe.addAll( MetricConstantsFactory.getOrdinaryMetricsFromConfig( config ) );

        returnMe.addAll( MetricConstantsFactory.getTimeSeriesMetricsFromConfig( config ) );

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns <code>true</code> if the input configuration requires any outputs of the specified type  where the 
     * {@link OutputTypeSelection} is {@link OutputTypeSelection#THRESHOLD_LEAD} for any or all metrics.
     * 
     * @param projectConfig the project configuration
     * @param outGroup the output group to test
     * @return true if the input configuration requires outputs of the {@link StatisticType#DIAGRAM} 
     *            type whose output type is {@link OutputTypeSelection#THRESHOLD_LEAD}, false otherwise
     * @throws MetricConfigException if the configuration is invalid
     * @throws NullPointerException if the input is null
     */

    public static boolean hasTheseOutputsByThresholdLead( ProjectConfig projectConfig, StatisticType outGroup )
    {
        Objects.requireNonNull( projectConfig );

        // Does the configuration contain the requested type?        
        boolean hasSpecifiedType = MetricConstantsFactory.getMetricsFromConfig( projectConfig )
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
     * Returns valid metrics for {@link SampleDataGroup#ENSEMBLE}
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @return the valid metrics for {@link SampleDataGroup#ENSEMBLE}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getAllValidMetricsForEnsembleInput( ProjectConfig projectConfig,
                                                                            MetricsConfig metricsConfig )
    {
        Objects.requireNonNull( metricsConfig );

        Set<MetricConstants> returnMe = new TreeSet<>();

        if ( !metricsConfig.getMetric().isEmpty() )
        {
            returnMe.addAll( SampleDataGroup.ENSEMBLE.getMetrics() );
            returnMe.addAll( SampleDataGroup.SINGLE_VALUED.getMetrics() );

            if ( MetricConstantsFactory.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || MetricConstantsFactory.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
            {
                returnMe.addAll( SampleDataGroup.DISCRETE_PROBABILITY.getMetrics() );
            }

            // Allow dichotomous metrics when probability classifiers are defined
            if ( MetricConstantsFactory.hasThresholds( metricsConfig, ThresholdType.PROBABILITY_CLASSIFIER ) )
            {
                Set<MetricConstants> metrics = MetricConstantsFactory.getDichotomousMetrics();
                returnMe.addAll( metrics );
            }
        }

        return MetricConstantsFactory.removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
    }

    /**
     * Returns a set of summary statistic for the input declaration.
     * 
     * @param config the statistic name
     * @return a set of summary statistics
     * @throws MetricConfigException if the named statistic could not be mapped
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getSummaryStatisticsFor( TimeSeriesMetricConfig config )
    {
        if ( Objects.isNull( config ) || Objects.isNull( config.getSummaryStatistics() ) )
        {
            LOGGER.debug( "No summary statistics found for the input declaration." );

            return Set.of();
        }

        List<SummaryStatisticsName> summaryStats = config.getSummaryStatistics()
                                                         .getName();

        String outerNameString = config.getName().name();

        if ( summaryStats.contains( SummaryStatisticsName.ALL_VALID ) )
        {
            MetricConstants outerName = MetricConstants.valueOf( outerNameString );
            return outerName.getChildren();
        }

        Set<MetricConstants> returnMe = new HashSet<>();

        // Find the summary statistic
        for ( SummaryStatisticsName nextName : summaryStats )
        {
            String innerNameString = outerNameString + "_" + nextName.name();
            MetricConstants innerName = MetricConstants.valueOf( innerNameString );
            returnMe.add( innerName );
        }

        return Collections.unmodifiableSet( returnMe );
    }


    /**
     * Returns valid ordinary metrics for {@link SampleDataGroup#SINGLE_VALUED}.
     *
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @return the valid metrics for {@link SampleDataGroup#SINGLE_VALUED}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getAllValidMetricsForSingleValuedInput( ProjectConfig projectConfig,
                                                                                MetricsConfig metricsConfig )
    {
        Objects.requireNonNull( metricsConfig );

        Set<MetricConstants> returnMe = new TreeSet<>();

        if ( !metricsConfig.getMetric().isEmpty() )
        {
            returnMe.addAll( SampleDataGroup.SINGLE_VALUED.getMetrics() );

            if ( MetricConstantsFactory.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || MetricConstantsFactory.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
            {
                Set<MetricConstants> metrics = MetricConstantsFactory.getDichotomousMetrics();
                returnMe.addAll( metrics );
            }
        }

        return removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
    }

    /**
     * Returns the set of available dichotomous metrics, not including the component elements of the 
     * {@link MetricConstants#CONTINGENCY_TABLE}, only the {@link MetricConstants#CONTINGENCY_TABLE} itself.
     * 
     * @return the dichotomous metrics
     */

    private static Set<MetricConstants> getDichotomousMetrics()
    {
        Set<MetricConstants> metrics = new HashSet<>( SampleDataGroup.DICHOTOMOUS.getMetrics() );

        Set<MetricConstants> contingencyTableComponents = MetricGroup.CONTINGENCY_TABLE.getAllComponents();
        metrics.removeAll( contingencyTableComponents );

        return Collections.unmodifiableSet( metrics );
    }

    /**
     * Returns valid metrics for {@link SampleDataGroup#SINGLE_VALUED_TIME_SERIES}
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @return the valid metrics for {@link SampleDataGroup#SINGLE_VALUED_TIME_SERIES}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getAllValidMetricsForSingleValuedTimeSeriesInput( ProjectConfig projectConfig,
                                                                                          MetricsConfig metricsConfig )
    {
        Objects.requireNonNull( metricsConfig );

        Set<MetricConstants> returnMe = new TreeSet<>();

        //Add time-series metrics if required
        if ( !metricsConfig.getTimeSeriesMetric().isEmpty() )
        {
            returnMe.addAll( SampleDataGroup.SINGLE_VALUED_TIME_SERIES.getMetrics() );
        }

        // Remove any general purpose timing error metrics that do not map to specific metric functions
        returnMe.remove( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC );
        returnMe.remove( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC );

        return MetricConstantsFactory.removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
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
        Objects.requireNonNull( projectConfig );

        Objects.requireNonNull( metrics, "Specify a non-null set of metrics to adjust." );

        Set<MetricConstants> returnMe = new HashSet<>( metrics );

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
        Objects.requireNonNull( config );

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
     * Returns {@link MetricConstants} from the input configuration for metrics that are not time-series. 
     * If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics 
     * are returned that are consistent with the configuration. 
     * 
     * @param projectConfig the project configuration
     * @return a set of metrics
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getOrdinaryMetricsFromConfig( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        Set<MetricConstants> returnMe = new HashSet<>();

        // Iterate through the ordinary metrics and populate the map
        for ( MetricsConfig metrics : projectConfig.getMetrics() )
        {
            returnMe.addAll( MetricConstantsFactory.getOrdinaryMetricsFromConfig( metrics, projectConfig ) );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a set of{@link MetricConstants} associated with time-series metrics in the input configuration. 
     * If the configuration contains the identifier {@link MetricConfigName#ALL_VALID}, all supported metrics are 
     * returned that are consistent with the configuration. 
     * 
     * @param projectConfig the project configuration
     * @return a set of metrics
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getTimeSeriesMetricsFromConfig( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        Set<MetricConstants> returnMe = new HashSet<>();

        // Iterate through the metric groups
        for ( MetricsConfig metrics : projectConfig.getMetrics() )
        {
            returnMe.addAll( MetricConstantsFactory.getTimeSeriesMetricsFromConfig( metrics, projectConfig ) );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Builds the mapping between the {@link MetricConstants} and the {@link MetricConfigName} 
     */

    private static void buildMetricConfigNameMap()
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

    private static void buildSummaryStatisticsNameMap()
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
     * Do not construct.
     */
    private MetricConstantsFactory()
    {
    }
}
