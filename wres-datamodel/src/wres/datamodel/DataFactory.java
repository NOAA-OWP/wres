package wres.datamodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConfigException;
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
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.StatisticsForProjectBuilder;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.time.TimeWindow;

/**
 * A factory class for producing datasets associated with verification metrics.
 * 
 * TODO: improve unit test coverage.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class DataFactory
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
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        returnMe.addAll( DataFactory.getOrdinaryMetricsFromConfig( metricsConfig, projectConfig ) );

        returnMe.addAll( DataFactory.getTimeSeriesMetricsFromConfig( metricsConfig, projectConfig ) );

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
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( TimeSeriesMetricConfig next : metricsConfig.getTimeSeriesMetric() )
        {
            // All valid metrics
            if ( next.getName() == TimeSeriesMetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                SampleDataGroup inGroup = DataFactory.getMetricInputGroup( projectConfig.getInputs().getRight() );

                // Single-valued input source
                if ( inGroup == SampleDataGroup.SINGLE_VALUED )
                {
                    allValid = DataFactory.getAllValidMetricsForSingleValuedTimeSeriesInput( projectConfig,
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
                returnMe.add( DataFactory.getMetricName( next.getName() ) );
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
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new HashSet<>();

        for ( MetricConfig next : metricsConfig.getMetric() )
        {
            // All valid metrics
            if ( next.getName() == MetricConfigName.ALL_VALID )
            {
                Set<MetricConstants> allValid = null;
                SampleDataGroup inGroup = DataFactory.getMetricInputGroup( projectConfig.getInputs().getRight() );

                // Single-valued metrics
                if ( inGroup == SampleDataGroup.SINGLE_VALUED )
                {
                    allValid =
                            DataFactory.getAllValidMetricsForSingleValuedInput( projectConfig, metricsConfig );
                }
                // Ensemble metrics
                else if ( inGroup == SampleDataGroup.ENSEMBLE )
                {
                    allValid = DataFactory.getAllValidMetricsForEnsembleInput( projectConfig, metricsConfig );
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
                returnMe.add( DataFactory.getMetricName( next.getName() ) );
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
        Objects.requireNonNull( metricConfigName, NULL_CONFIGURATION_NAME_ERROR );

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
        Objects.requireNonNull( timeSeriesMetricConfigName, NULL_CONFIGURATION_NAME_ERROR );

        //All valid metrics
        if ( timeSeriesMetricConfigName == TimeSeriesMetricConfigName.ALL_VALID )
        {
            return null;
        }

        return MetricConstants.valueOf( timeSeriesMetricConfigName.name() );
    }

    /**
     * Returns the {@link ThresholdConstants.ThresholdDataType} that corresponds to the {@link ThresholdDataType}
     * associated with the input configuration. Matches the enumerations by {@link Enum#name()}.
     * 
     * @param thresholdDataType the threshold data type
     * @return the mapped threshold data type
     * @throws IllegalArgumentException if the data type is not mapped
     * @throws NullPointerException if the input is null
     */

    public static ThresholdConstants.ThresholdDataType getThresholdDataType( ThresholdDataType thresholdDataType )
    {
        Objects.requireNonNull( thresholdDataType, NULL_CONFIGURATION_NAME_ERROR );

        return ThresholdConstants.ThresholdDataType.valueOf( thresholdDataType.name() );
    }

    /**
     * Returns the {@link ThresholdConstants.ThresholdGroup} that corresponds to the {@link ThresholdType}
     * associated with the input configuration. Matches the enumerations by {@link Enum#name()}.
     * 
     * @param thresholdType the threshold type
     * @return the mapped threshold group
     * @throws IllegalArgumentException if the threshold group is not mapped
     * @throws NullPointerException if the input is null
     */

    public static ThresholdConstants.ThresholdGroup getThresholdGroup( ThresholdType thresholdType )
    {
        Objects.requireNonNull( thresholdType, NULL_CONFIGURATION_NAME_ERROR );

        return ThresholdConstants.ThresholdGroup.valueOf( thresholdType.name() );
    }

    /**
     * Maps between threshold operators in {@link ThresholdOperator} and those in {@link Operator}.
     * 
     * TODO: make these enumerations match on name to reduce brittleness.
     * 
     * @param thresholdsConfig the threshold configuration
     * @return the mapped operator
     * @throws MetricConfigException if the operator is not mapped
     * @throws NullPointerException if the input is null or the {@link ThresholdsConfig#getOperator()} returns null
     */

    public static Operator getThresholdOperator( ThresholdsConfig thresholdsConfig )
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
                                                                   + thresholdsConfig.getOperator()
                                                                   + "'." );
        }
    }

    /**
     * Returns the metric data input type from the {@link DatasourceType}.
     * 
     * TODO: make these enumerations match on name to reduce brittleness.
     * 
     * @param dataSourceConfig the data source configuration
     * @return the metric input group
     * @throws MetricConfigException if the input type is not mapped
     * @throws NullPointerException if the input is null or the {@link DataSourceConfig#getType()} returns null 
     */

    public static SampleDataGroup getMetricInputGroup( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( dataSourceConfig.getType(), NULL_CONFIGURATION_NAME_ERROR );

        switch ( dataSourceConfig.getType() )
        {
            case ENSEMBLE_FORECASTS:
                return SampleDataGroup.ENSEMBLE;
            case SINGLE_VALUED_FORECASTS:
            case SIMULATIONS:
                return SampleDataGroup.SINGLE_VALUED;
            default:
                throw new MetricConfigException( dataSourceConfig,
                                                 "Unable to interpret the input type '" + dataSourceConfig.getType()
                                                                   + "'." );
        }
    }

    /**
     * Forms the union of the {@link PairedStatistic}, returning a {@link PairedStatistic} that contains all of the 
     * pairs in the inputs.
     * 
     * @param <S> the left side of the paired output
     * @param <T> the right side of the paired output
     * @param collection the list of inputs
     * @return a combined {@link PairedStatistic}
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input is empty
     */

    public static <S, T> PairedStatistic<S, T> unionOf( Collection<PairedStatistic<S, T>> collection )
    {
        Objects.requireNonNull( collection );
        
        if( collection.isEmpty() )
        {
            throw new IllegalArgumentException( "Specify one or more sets of pairs to combine." );
        }
        
        List<Pair<S, T>> combined = new ArrayList<>();
        List<TimeWindow> combinedWindows = new ArrayList<>();
        StatisticMetadata sourceMeta = null;
        for ( PairedStatistic<S, T> next : collection )
        {
            combined.addAll( next.getData() );
            if ( Objects.isNull( sourceMeta ) )
            {
                sourceMeta = next.getMetadata();
            }
            combinedWindows.add( next.getMetadata().getSampleMetadata().getTimeWindow() );
        }
        
        if( Objects.isNull( sourceMeta ) )
        {
            throw new IllegalArgumentException( "Cannot find the union of input whose metadata is missing." );
        }
        
        TimeWindow unionWindow = null;
        if ( !combinedWindows.isEmpty() )
        {
            unionWindow = TimeWindow.unionOf( combinedWindows );
        }

        StatisticMetadata combinedMeta =
                StatisticMetadata.of( SampleMetadata.of( sourceMeta.getSampleMetadata(), unionWindow ),
                                      combined.size(),
                                      sourceMeta.getMeasurementUnit(),
                                      sourceMeta.getMetricID(),
                                      sourceMeta.getMetricComponentID() );
        
        return PairedStatistic.of( combined, combinedMeta );
    }

    /**
     * Return a {@link Pair} from two double vectors.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    public static Pair<VectorOfDoubles, VectorOfDoubles> pairOf( double[] left, double[] right )
    {
        return new Pair<VectorOfDoubles, VectorOfDoubles>()
        {

            private static final long serialVersionUID = -1498961647587422087L;

            @Override
            public VectorOfDoubles setValue( VectorOfDoubles vectorOfDoubles )
            {
                throw new UnsupportedOperationException( "Cannot set on this entry." );
            }

            @Override
            public VectorOfDoubles getLeft()
            {
                return VectorOfDoubles.of( left );
            }

            @Override
            public VectorOfDoubles getRight()
            {
                return VectorOfDoubles.of( right );
            }
        };
    }

    /**
     * Returns a builder for a {@link StatisticsForProject}.
     * 
     * @return a {@link StatisticsForProjectBuilder} for a map of metric outputs by time window and
     *         threshold
     */

    public static StatisticsForProjectBuilder ofMetricOutputForProjectByTimeAndThreshold()
    {
        return new StatisticsForProject.StatisticsForProjectBuilder();
    }

    /**
     * Helper that checks for the equality of two double values using a prescribed number of significant digits.
     * 
     * @param first the first double
     * @param second the second double
     * @param digits the number of significant digits
     * @return true if the first and second are equal to the number of significant digits
     */

    public static boolean doubleEquals( double first, double second, int digits )
    {
        return Math.abs( first - second ) < 1.0 / digits;
    }

    /**
     * Consistent comparison of double arrays, first checks count of elements,
     * next goes through values.
     *
     * If first has fewer values, return -1, if first has more values, return 1.
     *
     * If value count is equal, go through in order until an element is less
     * or greater than another. If all values are equal, return 0.
     *
     * @param first the first array
     * @param second the second array
     * @return -1 if first is less than second, 0 if equal, 1 otherwise.
     */
    public static int compareDoubleArray( final double[] first,
                                          final double[] second )
    {
        // this one has fewer elements
        if ( first.length < second.length )
        {
            return -1;
        }
        // this one has more elements
        else if ( first.length > second.length )
        {
            return 1;
        }
        // compare values until we diverge
        else // assumption here is lengths are equal
        {
            for ( int i = 0; i < first.length; i++ )
            {
                int safeComparisonResult = Double.compare( first[i], second[i] );
                if ( safeComparisonResult != 0 )
                {
                    return safeComparisonResult;
                }
            }
            // all values were equal
            return 0;
        }
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
        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        if ( !metricsConfig.getMetric().isEmpty() )
        {
            returnMe.addAll( SampleDataGroup.ENSEMBLE.getMetrics() );
            returnMe.addAll( SampleDataGroup.SINGLE_VALUED.getMetrics() );

            if ( DataFactory.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || DataFactory.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
            {
                returnMe.addAll( SampleDataGroup.DISCRETE_PROBABILITY.getMetrics() );
            }

            // Allow dichotomous metrics when probability classifiers are defined
            if ( DataFactory.hasThresholds( metricsConfig, ThresholdType.PROBABILITY_CLASSIFIER ) )
            {
                returnMe.addAll( SampleDataGroup.DICHOTOMOUS.getMetrics() );
            }
        }

        return removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
    }

    /**
     * Returns valid ordinary metrics for {@link SampleDataGroup#SINGLE_VALUED}. Also see:
     * {@link #getValidMetricsForSingleValuedInput(ProjectConfig, MetricsConfig)}
     * 
     * @param projectConfig the project configuration
     * @param metricsConfig the metrics configuration
     * @return the valid metrics for {@link SampleDataGroup#SINGLE_VALUED}
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricConstants> getAllValidMetricsForSingleValuedInput( ProjectConfig projectConfig,
                                                                                MetricsConfig metricsConfig )
    {
        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        if ( !metricsConfig.getMetric().isEmpty() )
        {
            returnMe.addAll( SampleDataGroup.SINGLE_VALUED.getMetrics() );

            if ( DataFactory.hasThresholds( metricsConfig, ThresholdType.PROBABILITY )
                 || DataFactory.hasThresholds( metricsConfig, ThresholdType.VALUE ) )
            {
                returnMe.addAll( SampleDataGroup.DICHOTOMOUS.getMetrics() );
            }
        }
        
        // Disallowed temporarily: #69567
        returnMe.remove( MetricConstants.ROOT_MEAN_SQUARE_ERROR_NORMALIZED );
        returnMe.remove( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED );
        
        return removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
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
        Objects.requireNonNull( metricsConfig, NULL_CONFIGURATION_ERROR );

        Set<MetricConstants> returnMe = new TreeSet<>();

        //Add time-series metrics if required
        if ( !metricsConfig.getTimeSeriesMetric().isEmpty() )
        {
            returnMe.addAll( SampleDataGroup.SINGLE_VALUED_TIME_SERIES.getMetrics() );
        }

        return DataFactory.removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
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
            returnMe.removeIf( a -> ! ( a.isInGroup( StatisticGroup.DOUBLE_SCORE )
                                        || a.isInGroup( StatisticGroup.DURATION_SCORE ) ) );
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
     * Prevent construction.
     */

    private DataFactory()
    {
    }

}
