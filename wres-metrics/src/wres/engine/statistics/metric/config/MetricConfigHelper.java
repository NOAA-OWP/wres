package wres.engine.statistics.metric.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import wres.config.MetricConfigException;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.config.generated.SummaryStatisticsName;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;

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
     * @param projectConfig the project configuration with internally configured thresholds
     * @param external an optional source of external thresholds, may be null
     * @return the union of internal and external thresholds
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if the projectConfig or dataFactory is null
     */

    public static ThresholdsByMetric getThresholdsFromConfig( ProjectConfig projectConfig,
                                                              ThresholdsByMetric external )
    {
        if ( Objects.nonNull( external ) )
        {
            return MetricConfigHelper.getThresholdsFromConfig( projectConfig, Arrays.asList( external ) );
        }

        return MetricConfigHelper.getThresholdsFromConfig( projectConfig,
                                                           (Collection<ThresholdsByMetric>) null );
    }

    /**
     * Reads the internally configured thresholds, combines them with any supplied, external, thresholds, and 
     * returns the union of all thresholds.
     * 
     * @param projectConfig the project configuration with internally configured thresholds
     * @param externalThresholds an optional source of external thresholds, may be null
     * @return the union of internal and external thresholds
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if the projectConfig or dataFactory is null
     */

    public static ThresholdsByMetric getThresholdsFromConfig( ProjectConfig projectConfig,
                                                              Collection<ThresholdsByMetric> externalThresholds )
    {

        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        // Find the units associated with the pairs
        MeasurementUnit units = null;
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            units = MeasurementUnit.of( projectConfig.getPair().getUnit() );
        }

        // Builder 
        ThresholdsByMetricBuilder builder = new ThresholdsByMetricBuilder();

        // Iterate through the metric groups
        for ( MetricsConfig nextGroup : projectConfig.getMetrics() )
        {

            MetricConfigHelper.addThresholdsToBuilderForOneMetricGroup( projectConfig,
                                                                        nextGroup,
                                                                        builder,
                                                                        units );
        }

        ThresholdsByMetric returnMe = builder.build();

        // Find the union with any external thresholds
        if ( Objects.nonNull( externalThresholds ) )
        {
            for ( ThresholdsByMetric nextStore : externalThresholds )
            {
                returnMe = returnMe.unionWithThisStore( nextStore );
            }
        }

        return returnMe;
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
     * @return true if the input configuration requires outputs of the {@link StatisticGroup#MULTIVECTOR} 
     *            type whose output type is {@link OutputTypeSelection#THRESHOLD_LEAD}, false otherwise
     * @throws MetricConfigException if the configuration is invalid
     * @throws NullPointerException if the input is null
     */

    public static boolean hasTheseOutputsByThresholdLead( ProjectConfig projectConfig, StatisticGroup outGroup )
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
     * Helper that interprets the input configuration and returns a list of {@link StatisticGroup} whose results 
     * should be cached when computing metrics incrementally.
     * 
     * @param projectConfig the project configuration
     * @return a list of output types that should be cached
     * @throws MetricConfigException if the configuration is invalid
     * @throws NullPointerException if the input is null
     */

    public static Set<StatisticGroup> getCacheListFromProjectConfig( ProjectConfig projectConfig )
    {
        // Always cache ordinary scores and paired output for timing error metrics
        Set<StatisticGroup> returnMe = new TreeSet<>();
        returnMe.add( StatisticGroup.DOUBLE_SCORE );
        returnMe.add( StatisticGroup.PAIRED );
        
        // Always cache box plot outputs for pooled predictions
        returnMe.add( StatisticGroup.BOXPLOT_PER_POOL );

        // Cache other outputs as required
        StatisticGroup[] options = StatisticGroup.values();
        for ( StatisticGroup next : options )
        {
            if ( !returnMe.contains( next )
                 && MetricConfigHelper.hasTheseOutputsByThresholdLead( projectConfig, next ) )
            {
                returnMe.add( next );
            }
        }

        // Never cache box plot output for individual pairs
        returnMe.remove( StatisticGroup.BOXPLOT_PER_PAIR );

        // Never cache duration score output as timing error summary statistics are computed once all data 
        // is available
        returnMe.remove( StatisticGroup.DURATION_SCORE );

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns a list of {@link Threshold} from a comma-separated string. Specify the type of {@link Threshold}
     * required.
     * 
     * @param inputString the comma-separated input string
     * @param oper the operator
     * @param dataType the data type to which the threshold applies
     * @param areProbs is true to generate probability thresholds, false for ordinary thresholds
     * @param units the optional units in which non-probability thresholds are expressed
     * @return the thresholds
     * @throws MetricConfigException if the thresholds are configured incorrectly
     * @throws NullPointerException if the input is null
     */

    private static Set<Threshold> getThresholdsFromCommaSeparatedValues( String inputString,
                                                                         Operator oper,
                                                                         ThresholdConstants.ThresholdDataType dataType,
                                                                         boolean areProbs,
                                                                         MeasurementUnit units )
    {
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
                throw new MetricConfigException( "At least two values are required to compose a "
                                                 + "threshold that operates between a lower and an upper bound." );
            }
            for ( int i = 0; i < addMe.size() - 1; i++ )
            {
                if ( areProbs )
                {
                    returnMe.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( addMe.get( i ),
                                                                                          addMe.get( i
                                                                                                     + 1 ) ),
                                                                      oper,
                                                                      dataType,
                                                                      units ) );
                }
                else
                {
                    returnMe.add( Threshold.of( OneOrTwoDoubles.of( addMe.get( i ),
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
                addMe.forEach( threshold -> returnMe.add( Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( threshold ),
                                                                                              oper,
                                                                                              dataType,
                                                                                              units ) ) );
            }
            else
            {
                addMe.forEach( threshold -> returnMe.add( Threshold.of( OneOrTwoDoubles.of( threshold ),
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
     * @return the adjusted thresholds
     * @throws NullPointerException if either input is null
     */

    private static Set<Threshold> getAdjustedThresholds( MetricConstants metric,
                                                         Set<Threshold> thresholds,
                                                         ThresholdConstants.ThresholdGroup type )
    {
        Objects.requireNonNull( metric, "Specify a non-null metric." );

        Objects.requireNonNull( thresholds, "Specify non-null thresholds." );

        Objects.requireNonNull( type, "Specify a non-null threshold type." );

        if ( type == ThresholdConstants.ThresholdGroup.PROBABILITY_CLASSIFIER )
        {
            return thresholds;
        }

        Set<Threshold> returnMe = new HashSet<>();

        Threshold allData =
                Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                         Operator.GREATER,
                                         ThresholdConstants.ThresholdDataType.LEFT_AND_RIGHT );

        // All data only
        if ( metric.getMetricOutputGroup() == StatisticGroup.BOXPLOT_PER_PAIR
             || metric.getMetricOutputGroup() == StatisticGroup.BOXPLOT_PER_POOL
             || metric == MetricConstants.QUANTILE_QUANTILE_DIAGRAM )
        {
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( allData ) ) );
        }

        // Otherwise, add the input thresholds
        returnMe.addAll( thresholds );

        // Add all data for appropriate types
        if ( metric.isInGroup( SampleDataGroup.ENSEMBLE ) || metric.isInGroup( SampleDataGroup.SINGLE_VALUED )
             || metric.isInGroup( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
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
     * Adds thresholds to the input builder for one metric group.
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @param builder the builder
     * @param units the optional units for value thresholds
     * @throws MetricConfigException if the threshold configuration is incorrect
     */

    private static void addThresholdsToBuilderForOneMetricGroup( ProjectConfig projectConfig,
                                                                 MetricsConfig metricsConfig,
                                                                 ThresholdsByMetricBuilder builder,
                                                                 MeasurementUnit units )
    {

        // Find the metrics
        Set<MetricConstants> metrics = DataFactory.getMetricsFromMetricsConfig( metricsConfig, projectConfig );

        // No explicit thresholds, add an "all data" threshold
        if ( metricsConfig.getThresholds().isEmpty() )
        {
            for ( MetricConstants next : metrics )
            {
                Set<Threshold> allData = MetricConfigHelper.getAdjustedThresholds( next,
                                                                                   Collections.emptySet(),
                                                                                   ThresholdConstants.ThresholdGroup.VALUE );

                builder.addThresholds( Collections.singletonMap( next, allData ),
                                       ThresholdConstants.ThresholdGroup.VALUE );
            }
        }

        // Iterate through any explicit thresholds
        for ( ThresholdsConfig nextThresholds : metricsConfig.getThresholds() )
        {
            // Thresholds
            Set<Threshold> thresholds =
                    MetricConfigHelper.getInternalThresholdsFromThresholdsConfig( nextThresholds, units );

            // Build the thresholds map per metric
            Map<MetricConstants, Set<Threshold>> thresholdsMap = new EnumMap<>( MetricConstants.class );

            // Type of thresholds
            ThresholdConstants.ThresholdGroup thresholdType = ThresholdConstants.ThresholdGroup.PROBABILITY;
            if ( Objects.nonNull( nextThresholds.getType() ) )
            {
                thresholdType = DataFactory.getThresholdGroup( nextThresholds.getType() );
            }

            // Adjust the thresholds, adding "all data" where required, then append
            for ( MetricConstants next : metrics )
            {
                thresholdsMap.put( next,
                                   MetricConfigHelper.getAdjustedThresholds( next,
                                                                             thresholds,
                                                                             thresholdType ) );
            }

            // Add the thresholds
            builder.addThresholds( thresholdsMap, thresholdType );
        }
    }

    /**
     * Obtains a set of {@link Threshold} from a {@link ThresholdsConfig} for thresholds that are configured internally
     * and not sourced externally. In other words, {@link ThresholdsConfig#getCommaSeparatedValuesOrSource()} must 
     * return a string of thresholds and not a {@link ThresholdsConfig.Source}.
     * 
     * @param thresholds the thresholds configuration
     * @param units optional units for non-probability thresholds
     * @return a set of thresholds (possibly empty)
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if either input is null
     */

    private static Set<Threshold> getInternalThresholdsFromThresholdsConfig( ThresholdsConfig thresholds,
                                                                             MeasurementUnit units )
    {
        Objects.requireNonNull( thresholds, NULL_CONFIGURATION_ERROR );

        Set<Threshold> returnMe = new HashSet<>();

        Operator operator = Operator.GREATER;

        // Operator specified
        if ( Objects.nonNull( thresholds.getOperator() ) )
        {
            operator = DataFactory.getThresholdOperator( thresholds );
        }

        ThresholdConstants.ThresholdDataType dataType = ThresholdConstants.ThresholdDataType.LEFT;

        // Operator specified
        if ( Objects.nonNull( thresholds.getApplyTo() ) )
        {
            dataType = DataFactory.getThresholdDataType( thresholds.getApplyTo() );
        }

        // Must be internally sourced: thresholds with global scope should be provided directly 
        Object values = thresholds.getCommaSeparatedValuesOrSource();

        // Default to ThresholdType.PROBABILITY
        ThresholdType type = ThresholdType.PROBABILITY;
        if ( Objects.nonNull( thresholds.getType() ) )
        {
            type = thresholds.getType();
        }

        // String = internal source
        if ( values instanceof String )
        {
            returnMe.addAll( MetricConfigHelper.getThresholdsFromCommaSeparatedValues( values.toString(),
                                                                                       operator,
                                                                                       dataType,
                                                                                       type != ThresholdType.VALUE,
                                                                                       units ) );
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
