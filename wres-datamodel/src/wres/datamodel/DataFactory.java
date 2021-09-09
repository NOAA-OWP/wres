package wres.datamodel;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.LeftOrRightOrBaseline;
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
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.Builder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;

/**
 * A factory class for producing datasets associated with verification metrics.
 * 
 * TODO: improve unit test coverage.
 * 
 * @author James Brown
 */

public final class DataFactory
{

    private static final Logger LOGGER = LoggerFactory.getLogger( DataFactory.class );

    /**
     * Null configuration error.
     */

    private static final String NULL_CONFIGURATION_ERROR = "Specify non-null project configuration.";

    /**
     * Null mapping error.
     */

    private static final String NULL_CONFIGURATION_NAME_ERROR = "Specify input configuration with a "
                                                                + "non-null identifier to map.";

    private static final String ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING =
            "Enter non-null metadata to establish a path for writing.";

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
     * Forms the union of the {@link DurationDiagramStatisticOuter}, returning a {@link DurationDiagramStatisticOuter} that contains all of the 
     * pairs in the inputs.
     * 
     * @param collection the list of inputs
     * @return a combined {@link DurationDiagramStatisticOuter}
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input is empty
     */

    public static DurationDiagramStatisticOuter unionOf( Collection<DurationDiagramStatisticOuter> collection )
    {
        Objects.requireNonNull( collection );

        if ( collection.isEmpty() )
        {
            throw new IllegalArgumentException( "Specify one or more sets of pairs to combine." );
        }

        DurationDiagramStatistic.Builder builder = DurationDiagramStatistic.newBuilder();
        builder.setMetric( collection.iterator().next().getData().getMetric() );
        Set<TimeWindowOuter> combinedWindows = new HashSet<>();
        PoolMetadata sourceMeta = null;

        for ( DurationDiagramStatisticOuter next : collection )
        {
            builder.addAllStatistics( next.getData().getStatisticsList() );

            if ( Objects.isNull( sourceMeta ) )
            {
                sourceMeta = next.getMetadata();
            }
            combinedWindows.add( next.getMetadata().getTimeWindow() );
        }

        if ( Objects.isNull( sourceMeta ) )
        {
            throw new IllegalArgumentException( "Cannot find the union of input whose metadata is missing." );
        }

        TimeWindowOuter unionWindow = null;
        if ( !combinedWindows.isEmpty() )
        {
            unionWindow = TimeWindowOuter.unionOf( combinedWindows );
        }

        DurationDiagramStatistic statistic = builder.build();

        return DurationDiagramStatisticOuter.of( statistic, PoolMetadata.of( sourceMeta, unionWindow ) );
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
     * @return a {@link Builder} for a map of metric outputs by time window and
     *         threshold
     */

    public static Builder ofMetricOutputForProjectByTimeAndThreshold()
    {
        return new StatisticsForProject.Builder();
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
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link PoolMetadata}
     * associated with the results and a {@link TimeWindowOuter}.
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getPathFromSampleMetadata( Path outputDirectory,
                                                  PoolMetadata meta,
                                                  TimeWindowOuter timeWindow,
                                                  ChronoUnit leadUnits,
                                                  MetricConstants metricName,
                                                  MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( meta, DataFactory.ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );

        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );

        return DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                      meta,
                                                      DataFactory.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                                       leadUnits )
                                                            + "_"
                                                            + leadUnits.name().toUpperCase(),
                                                      metricName,
                                                      metricComponentName );
    }

    /**
     * Returns a path to write from a combination of the destination configuration, the input metadata and any 
     * additional string that should be appended to the path (e.g. lead time or threshold). 
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param append an optional string to append to the end of the path, may be null
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated 
     *            with the sample metadata
     * @throws IOException if the path cannot be produced
     * @throws ProjectConfigException when the destination configuration is invalid
     */

    public static Path getPathFromSampleMetadata( Path outputDirectory,
                                                  PoolMetadata meta,
                                                  String append,
                                                  MetricConstants metricName,
                                                  MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( metricName, "Specify a non-null metric name." );

        Pool pool = meta.getPool();

        // Build the path 
        StringJoiner joinElements = new StringJoiner( "_" );

        Evaluation evaluation = meta.getEvaluation();

        // Geographic name
        String geoName = DataFactory.getGeographicName( pool );
        joinElements.add( geoName );
        
        // Dataset name
        String dataName = DataFactory.getDatasetName( evaluation, pool );

        if ( !dataName.isBlank() )
        {
            joinElements.add( dataName );
        }

        // Add the metric name
        joinElements.add( metricName.name() );

        // Add a non-default component name
        if ( Objects.nonNull( metricComponentName ) && MetricConstants.MAIN != metricComponentName )
        {
            joinElements.add( metricComponentName.name() );
        }

        // Add optional append
        if ( Objects.nonNull( append ) )
        {
            joinElements.add( append );
        }

        // Derive a sanitized name
        String safeName = URLEncoder.encode( joinElements.toString().replace( " ", "_" ), "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link PoolMetadata}
     * associated with the results and a {@link OneOrTwoThresholds}.
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param threshold the threshold
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getPathFromSampleMetadata( Path outputDirectory,
                                                  PoolMetadata meta,
                                                  OneOrTwoThresholds threshold,
                                                  MetricConstants metricName,
                                                  MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( threshold, "Enter non-null threshold to establish a path for writing." );

        return getPathFromSampleMetadata( outputDirectory,
                                          meta,
                                          threshold.toStringSafe(),
                                          metricName,
                                          metricComponentName );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig} and the {@link PoolMetadata}.
     *
     * @param outputDirectory the directory into which to write
     * @param meta the metadata
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write, without a file type extension
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getPathFromSampleMetadata( Path outputDirectory,
                                                  PoolMetadata meta,
                                                  MetricConstants metricName,
                                                  MetricConstants metricComponentName )
            throws IOException
    {
        return DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                      meta,
                                                      (String) null,
                                                      metricName,
                                                      metricComponentName );
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
                Set<MetricConstants> metrics = DataFactory.getDichotomousMetrics();
                returnMe.addAll( metrics );
            }
        }

        return removeMetricsDisallowedByOtherConfig( projectConfig, returnMe );
    }

    /**
     * Returns a geographic name for the pool.
     * @param pool the pool
     * @return a graphic name
     * @throws IllegalArgumentException if the pool has zero feature tuples or many tuples and no group name
     */

    private static String getGeographicName( Pool pool )
    {
        int geoCount = pool.getGeometryTuplesCount();
        if ( geoCount == 0 )
        {
            throw new IllegalArgumentException( "Expected metadata with at least one feature tuple, but found none." );
        }

        if ( geoCount > 1 )
        {
            if ( "".equals( pool.getRegionName() ) )
            {
                throw new IllegalArgumentException( "Discovered a pool with " + geoCount
                                                    + " features, but no region "
                                                    + "name that describes them, which is not allowed in this "
                                                    + "context." );
            }

            return pool.getRegionName();
        }

        // Exactly one tuple
        GeometryTuple firstTuple = pool.getGeometryTuples( 0 );

        StringJoiner joiner = new StringJoiner( "_" );

        // Work-around to figure out if this is gridded data and if so to use
        // something other than the feature name, use the description.
        // When you make gridded benchmarks congruent, remove this.
        if ( firstTuple.getRight()
                       .getName()
                       .matches( "^-?[0-9]+\\.[0-9]+ -?[0-9]+\\.[0-9]+$" ) )
        {
            LOGGER.debug( "Using ugly workaround for ugly gridded benchmarks: {}",
                          firstTuple );
            joiner.add( firstTuple.getRight()
                                  .getDescription() );
        }
        else
        {
            LOGGER.debug( "Creating a geographic name from the single feature tuple, {}.", firstTuple );

            // Region name?
            if ( !"".equals( pool.getRegionName() ) )
            {
                joiner.add( pool.getRegionName().replace( "-", "_" ) );
            }
            // No, use the first tuple instead
            else
            {
                joiner.add( firstTuple.getLeft()
                                      .getName() );
                joiner.add( firstTuple.getRight()
                                      .getName() );

                if ( firstTuple.hasBaseline() )
                {
                    joiner.add( firstTuple.getBaseline()
                                          .getName() );
                }
            }
        }

        return joiner.toString();
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
                Set<MetricConstants> metrics = DataFactory.getDichotomousMetrics();
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
            returnMe.removeIf( a -> ! ( a.isInGroup( StatisticType.DOUBLE_SCORE )
                                        || a.isInGroup( StatisticType.DURATION_SCORE ) ) );
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
     * Retrieves the specified number of time units from the input duration. Accepted units include:
     * 
     * <ol>
     * <li>{@link ChronoUnit#DAYS}</li>
     * <li>{@link ChronoUnit#HOURS}</li>
     * <li>{@link ChronoUnit#MINUTES}</li>
     * <li>{@link ChronoUnit#SECONDS}</li>
     * <li>{@link ChronoUnit#MILLIS}</li>
     * </ol>
     * 
     * @param duration Retrieves the duration
     * @param durationUnits the time units required
     * @return The length of the duration in terms of the project's lead resolution
     * @throws IllegalArgumentException if the durationUnits is not one of the accepted units
     */
    private static long durationToLongUnits( Duration duration, ChronoUnit durationUnits )
    {
        switch ( durationUnits )
        {
            case DAYS:
                return duration.toDays();
            case HOURS:
                return duration.toHours();
            case MINUTES:
                return duration.toMinutes();
            case SECONDS:
                return duration.getSeconds();
            case MILLIS:
                return duration.toMillis();
            default:
                throw new IllegalArgumentException( "The input time units '" + durationUnits
                                                    + "' are not supported "
                                                    + "in this context." );
        }
    }

    /**
     * Gets the name of a dataset from the evaluation and pool.
     * 
     * @param evaluation the evaluation
     * @param pool the pool
     * @return the dataset name
     */

    private static String getDatasetName( Evaluation evaluation, Pool pool )
    {
        String name = null;

        // Try to use the baseline data name if this pool is a baseline pool
        if ( pool.getIsBaselinePool() && !evaluation.getBaselineDataName().isBlank() )
        {
            name = evaluation.getBaselineDataName();

        }

        // Use the right name, which may be blank
        if ( Objects.isNull( name ) )
        {
            name = evaluation.getRightDataName();
        }

        // If both right and baseline have the same non-blank names, resolve this
        if ( evaluation.getBaselineDataName().equals( evaluation.getRightDataName() ) &&
             !evaluation.getRightDataName().isBlank() )
        {
            if ( pool.getIsBaselinePool() )
            {
                name = LeftOrRightOrBaseline.BASELINE.toString();
            }
            else
            {
                name = LeftOrRightOrBaseline.RIGHT.toString();
            }
        }

        return name;
    }

    /**
     * Prevent construction.
     */

    private DataFactory()
    {
    }

}
