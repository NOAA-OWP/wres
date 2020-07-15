package wres.engine.statistics.metric.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import wres.config.MetricConfigException;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsGenerator;

/**
 * A helper class for interpreting and using the {@link ProjectConfig} in the context of verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetricConfigHelper
{

    /**
     * Null configuration error.
     */

    public static final String NULL_CONFIGURATION_ERROR = "Specify non-null project configuration.";

    /**
     * Null mapping error.
     */

    public static final String NULL_CONFIGURATION_NAME_ERROR = "Specify input configuration with a "
                                                               + "non-null name to map.";

    /**
     * Map between {@link MetricConfigName} and {@link MetricConstants}.
     */

    private static final EnumMap<MetricConfigName, MetricConstants> NAME_MAP = new EnumMap<>( MetricConfigName.class );

    /**
     * Map between {@link SummaryStatisticsName} and {@link MetricConstants}
     */

    private static final EnumMap<SummaryStatisticsName, MetricConstants> STATISTICS_NAME_MAP =
            new EnumMap<>( SummaryStatisticsName.class );

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

        Objects.requireNonNull( metricConfigName, NULL_CONFIGURATION_NAME_ERROR );

        //All valid metrics
        if ( metricConfigName == MetricConfigName.ALL_VALID )
        {
            return null;
        }

        // Lazy build the mapping
        buildMetricConfigNameMap();

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

        Objects.requireNonNull( statsName, NULL_CONFIGURATION_NAME_ERROR );

        //All valid metrics
        if ( statsName.equals( SummaryStatisticsName.ALL_VALID ) )
        {
            return null;
        }

        // Lazy build the name map
        buildSummaryStatisticsNameMap();

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
        Objects.requireNonNull( config, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        returnMe.addAll( MetricConfigHelper.getOrdinaryMetricsFromConfig( config ) );

        returnMe.addAll( MetricConfigHelper.getTimeSeriesMetricsFromConfig( config ) );

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Reads the internally configured thresholds, combines them with any supplied, external, thresholds, and 
     * returns the union of all thresholds.
     *
     * @param external an optional source of external thresholds, may be null
     * @return the union of internal and external thresholds
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if the projectConfig or dataFactory is null
     */

    public static ThresholdsByMetric getThresholdsFromConfig( ThresholdsByMetric external )
    {
        if ( Objects.nonNull( external ) )
        {
            return MetricConfigHelper.getThresholdsFromConfig( Arrays.asList( external ) );
        }

        return null;
    }

    /**
     * Reads the internally configured thresholds, combines them with any supplied, external, thresholds, and 
     * returns the union of all thresholds.
     *
     * @param externalThresholds an optional source of external thresholds, may be null
     * @return the union of internal and external thresholds
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if the projectConfig or dataFactory is null
     */

    public static ThresholdsByMetric getThresholdsFromConfig( Collection<ThresholdsByMetric> externalThresholds )
    {
        ThresholdsByMetric allThresholds = null;

        for (ThresholdsByMetric thresholds : externalThresholds) {
            if (allThresholds == null) {
                allThresholds = thresholds;
            }
            else {
                allThresholds = allThresholds.unionWithThisStore(thresholds);
            }
        }

        return allThresholds;
    }

    /**
     * Returns true if the specified project configuration contains a metric of the specified type for which summary
     * statistics are defined, false otherwise. The predicate must test for a named metric, not 
     * {@link TimeSeriesMetricConfigName#ALL_VALID}. 
     * 
     * @param config the project configuration
     * @param metric the predicate to find a metric whose summary statistics are required
     * @return true if the configuration contains the specified type of metric, false otherwise
     * @throws MetricConfigException if the configuration is invalid
     * @throws IllegalArgumentException if the predicate tests for {@link TimeSeriesMetricConfigName#ALL_VALID}
     */

    public static boolean hasSummaryStatisticsFor( ProjectConfig config, Predicate<TimeSeriesMetricConfigName> metric )
    {
        return !MetricConfigHelper.getSummaryStatisticsFor( config, metric ).isEmpty();
    }

    /**
     * Returns a list of summary statistics associated with the predicate for a named metric. The predicate must test 
     * for a named metric, not {@link TimeSeriesMetricConfigName#ALL_VALID}. However, configuration that contains
     * {@link TimeSeriesMetricConfigName#ALL_VALID} will be tested separately and an appropriate list of summary 
     * statistics returned for that configuration.
     * 
     * @param config the project configuration
     * @param metric the predicate to find a metric whose summary statistics are required
     * @return the summary statistics associated with the named metric
     * @throws MetricConfigException if the project contains an unmapped summary statistic
     * @throws IllegalArgumentException if the predicate tests for {@link TimeSeriesMetricConfigName#ALL_VALID}
     */

    public static Set<MetricConstants> getSummaryStatisticsFor( ProjectConfig config,
                                                                Predicate<TimeSeriesMetricConfigName> metric )
    {
        Objects.requireNonNull( config, "Specify a non-null project configuration to check for summary statistics" );

        Objects.requireNonNull( metric, "Specify a non null metric to check for summary statistics." );

        if ( metric.test( TimeSeriesMetricConfigName.ALL_VALID ) )
        {
            throw new IllegalArgumentException( "Cannot obtain summary statistics for the general type 'all valid' "
                                                + "when a specific type is required: instead, provide a time-series "
                                                + "metric that is specific." );
        }

        Set<MetricConstants> allStats = new HashSet<>();

        // Iterate the metric groups
        for ( MetricsConfig nextGroup : config.getMetrics() )
        {
            // Iterate the time-series metrics
            for ( TimeSeriesMetricConfig next : nextGroup.getTimeSeriesMetric() )
            {
                // Match the name
                if ( ( next.getName() == TimeSeriesMetricConfigName.ALL_VALID || metric.test( next.getName() ) )
                     && Objects.nonNull( next.getSummaryStatistics() ) )
                {
                    // Return the summary statistics
                    for ( SummaryStatisticsName nextStat : next.getSummaryStatistics().getName() )
                    {
                        allStats.addAll( MetricConfigHelper.getSummaryStatisticsFor( nextStat ) );
                    }
                }
            }
        }

        return Collections.unmodifiableSet( allStats );
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
     * Helper that interprets the input configuration and returns a list of {@link StatisticType} whose results 
     * should be cached when computing metrics incrementally.
     * 
     * @param projectConfig the project configuration
     * @return a list of output types that should be cached
     * @throws MetricConfigException if the configuration is invalid
     * @throws NullPointerException if the input is null
     */

    public static Set<StatisticType> getCacheListFromProjectConfig( ProjectConfig projectConfig )
    {
        // Always cache ordinary scores and paired output for timing error metrics
        Set<StatisticType> returnMe = new TreeSet<>();
        returnMe.add( StatisticType.DOUBLE_SCORE );
        returnMe.add( StatisticType.DURATION_DIAGRAM );
        
        // Always cache box plot outputs for pooled predictions
        returnMe.add( StatisticType.BOXPLOT_PER_POOL );

        // Cache other outputs as required
        StatisticType[] options = StatisticType.values();
        for ( StatisticType next : options )
        {
            if ( !returnMe.contains( next )
                 && MetricConfigHelper.hasTheseOutputsByThresholdLead( projectConfig, next ) )
            {
                returnMe.add( next );
            }
        }

        // Never cache box plot output for individual pairs
        returnMe.remove( StatisticType.BOXPLOT_PER_PAIR );

        // Never cache duration score output as timing error summary statistics are computed once all data 
        // is available
        returnMe.remove( StatisticType.DURATION_SCORE );

        return Collections.unmodifiableSet( returnMe );
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
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        // Iterate through the ordinary metrics and populate the map
        for ( MetricsConfig metrics : projectConfig.getMetrics() )
        {
            returnMe.addAll( DataFactory.getOrdinaryMetricsFromConfig( metrics, projectConfig ) );
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
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        // Iterate through the metric groups
        for ( MetricsConfig metrics : projectConfig.getMetrics() )
        {
            returnMe.addAll( DataFactory.getTimeSeriesMetricsFromConfig( metrics, projectConfig ) );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a a summary statistic for the named input and all valid statistics if the input is 
     * {@link SummaryStatisticName#ALL_VALID}.
     * 
     * @param name the statistic name
     * @return a set of summary statistics
     * @throws MetricConfigException if the named statistic could not be mapped
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getSummaryStatisticsFor( SummaryStatisticsName name )
    {
        Objects.requireNonNull( name, "Specify a non-null summary statistic name." );

        // Lazy build
        buildSummaryStatisticsNameMap();

        Set<MetricConstants> returnMe = new HashSet<>();

        if ( name == SummaryStatisticsName.ALL_VALID )
        {
            returnMe.addAll( STATISTICS_NAME_MAP.values() );
        }
        else
        {
            returnMe.add( MetricConfigHelper.from( name ) );
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
     * Hidden constructor.
     */

    private MetricConfigHelper()
    {
    }

}
