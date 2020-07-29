package wres.io.config;

import static wres.config.generated.SourceTransformationType.PERSISTENCE;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.*;
import wres.config.generated.ProjectConfig.Outputs;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.utilities.NoDataException;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;
import wres.util.TimeHelper;

/**
 * The purpose of io's ConfigHelper is to help the io module translate raw
 * user-specified configuration elements into a reduced form, a more
 * actionable or meaningful form such as a SQL script, or to extract specific
 * elements from a particular config element, or other purposes that are common
 * to the io module.
 *
 * The general form of a helper method appropriate for ConfigHelper has a
 * ProjectConfig as the first argument and some other element(s) or hint(s) as
 * additional args. These are not hard-and-fast-rules. But the original purpose
 * was to help the io module avoid tedious repetition of common interpretations
 * of the raw user-specified configuration.
 *
 * Candidates for removal to a wres-config helper are those that purely operate
 * on, use, and return objects of classes that are specified in the wres-config
 * or JDK.
 *
 * Candidates that should stay are those returning SQL statements or are
 * currently useful only to the wres-io module.
 */

public class ConfigHelper
{
    private static final String ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING =
            "Enter non-null metadata to establish a path for writing.";

    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigHelper.class );

    /**
     * Default exception message when a destination cannot be established.
     */

    public static final String OUTPUT_CLAUSE_BOILERPLATE = "Please include valid numeric output clause(s) in"
                                                           + " the project configuration. Example: <destination>"
                                                           + "<path>c:/Users/myname/wres_output/</path>"
                                                           + "</destination>";

    /**
     * String for null configuration error.
     */

    private static final String NULL_CONFIGURATION_ERROR = "The project configuration cannot be null.";

    private ConfigHelper()
    {
        // prevent construction
    }

    // TODO: Move to wres-config
    public static boolean usesNetCDFData( ProjectConfig projectConfig )
    {
        boolean usesNetcdf = wres.util.Collections.exists(
                                                           projectConfig.getInputs().getLeft().getSource(),
                                                           source -> source.getFormat() != null &&
                                                                     source.getFormat().equals( Format.NET_CDF ) );

        usesNetcdf = usesNetcdf || wres.util.Collections.exists(
                                                                 projectConfig.getInputs().getRight().getSource(),
                                                                 source -> source.getFormat() != null &&
                                                                           source.getFormat()
                                                                                 .equals( Format.NET_CDF ) );

        if ( !usesNetcdf && projectConfig.getInputs().getBaseline() != null )
        {
            usesNetcdf = wres.util.Collections.exists(
                                                       projectConfig.getInputs().getBaseline().getSource(),
                                                       source -> source.getFormat() != null &&
                                                                 source.getFormat().equals( Format.NET_CDF ) );
        }

        return usesNetcdf;
    }

    /**
     * Creates a hash for the indicated project configuration based on its
     * specifications and the data it has ingested
     *
     * TODO: introduce wres.Dataset table, hash sorted hashes of left, right,
     * baseline separately, treat each as a dataset. Link dataset to project.
     *
     * TODO: store less collision-prone value, e.g. 128bit hash instead of int.
     * @param projectConfig The configuration for the project
     * @param leftHashesIngested A collection of the hashes for the left sided
     *                           source data
     * @param rightHashesIngested A collection of the hashes for the right sided
     *                            source data
     * @param baselineHashesIngested A collection of hashes representing the baseline
     *                               source data
     * @return A unique hash code for the project's circumstances
     */
    public static Integer hashProject( final ProjectConfig projectConfig,
                                       final List<String> leftHashesIngested,
                                       final List<String> rightHashesIngested,
                                       final List<String> baselineHashesIngested )
    {
        StringBuilder hashBuilder = new StringBuilder();

        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        hashBuilder.append( left.getType().value() );

        for ( EnsembleCondition ensembleCondition : left.getEnsemble() )
        {
            hashBuilder.append( ensembleCondition.getName() );
            hashBuilder.append( ensembleCondition.getMemberId() );
            hashBuilder.append( ensembleCondition.getQualifier() );
        }

        // Sort for deterministic hash result for same list of ingested
        Collection<String> sortedLeftHashes =
                wres.util.Collections.copyAndSort( leftHashesIngested );

        for ( String leftHash : sortedLeftHashes )
        {
            hashBuilder.append( leftHash );
        }

        hashBuilder.append( left.getVariable().getValue() );

        hashBuilder.append( right.getType().value() );

        for ( EnsembleCondition ensembleCondition : right.getEnsemble() )
        {
            hashBuilder.append( ensembleCondition.getName() );
            hashBuilder.append( ensembleCondition.getMemberId() );
            hashBuilder.append( ensembleCondition.getQualifier() );
        }

        // Sort for deterministic hash result for same list of ingested
        Collection<String> sortedRightHashes =
                wres.util.Collections.copyAndSort( rightHashesIngested );

        for ( String rightHash : sortedRightHashes )
        {
            hashBuilder.append( rightHash );
        }

        hashBuilder.append( right.getVariable().getValue() );

        if ( baseline != null )
        {

            hashBuilder.append( baseline.getType().value() );

            for ( EnsembleCondition ensembleCondition : baseline.getEnsemble() )
            {
                hashBuilder.append( ensembleCondition.getName() );
                hashBuilder.append( ensembleCondition.getMemberId() );
                hashBuilder.append( ensembleCondition.getQualifier() );
            }


            // Sort for deterministic hash result for same list of ingested
            Collection<String> sortedBaselineHashes =
                    wres.util.Collections.copyAndSort( baselineHashesIngested );

            for ( String baselineHash : sortedBaselineHashes )
            {
                hashBuilder.append( baselineHash );
            }

            hashBuilder.append( baseline.getVariable().getValue() );
        }

        for ( Feature feature : projectConfig.getPair()
                                             .getFeature() )
        {
            hashBuilder.append( feature.getLeft() );
            hashBuilder.append( feature.getRight() );
            hashBuilder.append( feature.getBaseline() );
        }

        return hashBuilder.toString().hashCode();
    }

    /**
     * Returns whether the declared {@link DatasourceType} matches one of the forecast types, currently 
     * {@link DatasourceType#SINGLE_VALUED_FORECASTS} and {@link DatasourceType#ENSEMBLE_FORECASTS}.
     * @param dataSourceConfig the configuration
     * @return true when the type of data is a forecast type
     */
    public static boolean isForecast( DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( dataSourceConfig );

        return dataSourceConfig.getType() == DatasourceType.SINGLE_VALUED_FORECASTS
               || dataSourceConfig.getType() == DatasourceType.ENSEMBLE_FORECASTS;
    }


    public static ProjectConfig read( final String path ) throws IOException
    {
        Path actualPath = Paths.get( path );
        ProjectConfigPlus configPlus = ProjectConfigPlus.from( actualPath );
        return configPlus.getProjectConfig();
    }


    /**
     * Get all the destinations from a configuration for a particular type.
     * @param config the config to search through
     * @param type the type to look for
     * @return a list of destinations with the type specified
     * @throws NullPointerException when config or type is null
     */

    public static List<DestinationConfig> getDestinationsOfType( ProjectConfig config,
                                                                 DestinationType type )
    {
        Objects.requireNonNull( config, "Config must not be null." );
        Objects.requireNonNull( type, "Type must not be null." );

        List<DestinationConfig> result = new ArrayList<>();

        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return java.util.Collections.unmodifiableList( result );
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            if ( d.getType() == type )
            {
                result.add( d );
            }
        }

        return java.util.Collections.unmodifiableList( result );
    }

    /**
     * Get all the graphical destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of graphical destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getGraphicalDestinations( ProjectConfig config )
    {
        return getDestinationsOfType( config, DestinationType.GRAPHIC );
    }

    /**
     * Get all the numerical destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of numerical destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getNumericalDestinations( ProjectConfig config )
    {
        return getDestinationsOfType( config, DestinationType.NUMERIC );
    }

    /**
     * Get the time zone offset from a datasource config or null if not found.
     * @param sourceConfig the configuration element to retrieve for
     * @return the time zone offset or null if not specified in dataSourceConfig
     * @throws ProjectConfigException when the date time could not be parsed
     */

    public static ZoneOffset getZoneOffset( DataSourceConfig.Source sourceConfig )
    {
        ZoneOffset result = null;

        if ( sourceConfig != null
             && sourceConfig.getZoneOffset() != null )
        {
            String configuredOffset = sourceConfig.getZoneOffset();

            // Look for CONUS-ish names like "EDT" and convert to offset.
            for ( ConusZoneId id : ConusZoneId.values() )
            {
                if ( configuredOffset.equalsIgnoreCase( id.name() ) )
                {
                    result = id.getZoneOffset();
                }
            }

            if ( result == null )
            {
                // Otherwise, try to parse directly into an offset
                try
                {
                    result = ZoneOffset.of( configuredOffset );
                }
                catch ( DateTimeException dte )
                {
                    String message = "Could not figure out the zoneOffset. "
                                     + "Try formatting it like this: -05:00.";
                    throw new ProjectConfigException( sourceConfig,
                                                      message,
                                                      dte );
                }
            }
        }

        return result;
    }


    /**
     * Returns true if the sourceConfig from projectConfig is a persistence
     * baseline element.
     * @param projectConfig the project config, not null
     * @param sourceConfig the source config, not null
     * @return true when sourceConfig indicates persistence baseline.
     * @throws NullPointerException when projectConfig or sourceConfig are null
     */

    public static boolean isPersistence( ProjectConfig projectConfig,
                                         DataSourceConfig sourceConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( sourceConfig );

        return projectConfig.getInputs()
                            .getBaseline() != null
               && ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                         sourceConfig )
                              .equals( LeftOrRightOrBaseline.BASELINE )
               && projectConfig.getInputs()
                               .getBaseline()
                               .getTransformation() != null
               && projectConfig.getInputs()
                               .getBaseline()
                               .getTransformation()
                               .equals( PERSISTENCE );
    }


    /**
     * Report if the projectConfig has a persistence baseline
     * @param projectConfig the project config to look at
     * @return true if the projectConfig has persistence baseline, false otherwise
     * @throws NullPointerException when projectConfig or its inputs are null
     */

    public static boolean hasPersistenceBaseline( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getInputs() );

        return projectConfig.getInputs().getBaseline() != null
               && ConfigHelper.isPersistence( projectConfig,
                                              projectConfig.getInputs().getBaseline() );
    }

    /**
     * Return <code>true</code> if the project uses probability thresholds, otherwise <code>false</code>.
     * 
     * @param projectConfig the project declaration
     * @return Whether or not the project uses probability thresholds
     * @throws NullPointerException if the input is null or the metrics declaration is null
     */
    public static boolean hasProbabilityThresholds( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getMetrics() );
        
        // Iterate metrics configuration
        for ( MetricsConfig next : projectConfig.getMetrics() )
        {
            // Check thresholds           
            if ( next.getThresholds()
                     .stream()
                     .anyMatch( a -> Objects.isNull( a.getType() )
                                     || a.getType() == ThresholdType.PROBABILITY ) )
            {
                return true;
            }
        }
        return false;
    }
    
    private enum ConusZoneId
    {
        UTC( "+0000" ),
        GMT( "+0000" ),
        EDT( "-0400" ),
        EST( "-0500" ),
        CDT( "-0500" ),
        CST( "-0600" ),
        MDT( "-0600" ),
        MST( "-0700" ),
        PDT( "-0700" ),
        PST( "-0800" ),
        AKDT( "-0800" ),
        AKST( "-0900" ),
        HADT( "-0900" ),
        HAST( "-1000" );

        private final transient ZoneOffset zoneOffset;

        ConusZoneId( String zoneOffset )
        {
            this.zoneOffset = ZoneOffset.of( zoneOffset );
        }

        public ZoneOffset getZoneOffset()
        {
            return this.zoneOffset;
        }
    }

    public static Duration getTimeShift( final DataSourceConfig dataSourceConfig )
    {
        Duration timeShift = null;

        if ( Objects.nonNull( dataSourceConfig )
             && Objects.nonNull( dataSourceConfig.getTimeShift() ) )
        {
            timeShift = Duration.of(
                                     dataSourceConfig.getTimeShift().getWidth(),
                                     ChronoUnit.valueOf(
                                                         dataSourceConfig.getTimeShift()
                                                                         .getUnit()
                                                                         .toString()
                                                                         .toUpperCase() ) );
        }

        return timeShift;
    }

    /**
     * Given a config and a data source, return which kind the datasource is
     *
     * TODO: this method cannot work. Two or more source declarations can be equal and the LRB context
     * is not part of the declaration. See #67774.
     * The above comment is one opinion about whether to use a method like this.
     * 
     * @param projectConfig the project config the source belongs to
     * @param config the config we wonder about
     * @return left or right or baseline
     * @throws IllegalArgumentException when the config doesn't belong to project
     */

    public static LeftOrRightOrBaseline getLeftOrRightOrBaseline( ProjectConfig projectConfig,
                                                                  DataSourceConfig config )
    {
        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        if ( config == left )
        {
            LOGGER.debug( "Config {} is a left config.", config );
            return LeftOrRightOrBaseline.LEFT;
        }
        else if ( config == right )
        {
            LOGGER.debug( "Config {} is a right config.", config );
            return LeftOrRightOrBaseline.RIGHT;
        }
        else if ( config == baseline )
        {
            LOGGER.debug( "Config {} is a baseline config.", config );
            return LeftOrRightOrBaseline.BASELINE;
        }
        else
        {
            // This means either .equals doesn't work or the caller has a bug.
            throw new IllegalArgumentException( "The project configuration "
                                                + projectConfig
                                                + " doesn't seem to contain the"
                                                + " source config "
                                                + config );
        }
    }

    /**
     * Returns the first instance of the named metric configuration or null if no such configuration exists.
     * 
     * @param projectConfig the project configuration
     * @param metricName the metric name
     * @return the named metric configuration or null
     * @throws NullPointerException if one or both of the inputs are null
     */

    public static MetricConfig getMetricConfigByName( ProjectConfig projectConfig, MetricConfigName metricName )
    {
        Objects.requireNonNull( projectConfig, "Specify a non-null metric configuration as input." );
        Objects.requireNonNull( metricName, "Specify a non-null metric name as input." );

        for ( MetricsConfig next : projectConfig.getMetrics() )
        {
            Optional<MetricConfig> nextConfig =
                    next.getMetric().stream().filter( metric -> metric.getName().equals( metricName ) ).findFirst();
            if ( nextConfig.isPresent() )
            {
                return nextConfig.get();
            }
        }

        return null;
    }

    /**
     * Returns a {@link DecimalFormat} from the input configuration or null if no formatter is required.
     * 
     * @param destinationConfig the destination configuration
     * @return a decimal formatter or null.
     */

    public static DecimalFormat getDecimalFormatter( DestinationConfig destinationConfig )
    {
        DecimalFormat decimalFormatter = null;
        if ( destinationConfig.getDecimalFormat() != null
             && !destinationConfig.getDecimalFormat().isEmpty() )
        {
            decimalFormatter = new DecimalFormat();
            decimalFormatter.applyPattern( destinationConfig.getDecimalFormat() );
        }
        return decimalFormatter;
    }

    /**
     * <p>Returns an {@link OutputTypeSelection} from the input configuration or {@link OutputTypeSelection#DEFAULT} if 
     * no selection is provided.</p> 
     * 
     * <p>If an override exists for a metric with the identifier {@link MetricConfigName#ALL_VALID} and this has a 
     * designated {@link OutputTypeSelection}, the override is returned. If an override does not exist, the 
     * {@link OutputTypeSelection} associated with the destination configuration is returned instead.</p>
     * 
     * @param projectConfig the project configuration to search for overrides
     * @param destinationConfig the destination configuration
     * @return the required output type
     * @throws NullPointerException if any input is null
     */

    public static OutputTypeSelection getOutputTypeSelection( ProjectConfig projectConfig,
                                                              DestinationConfig destinationConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Objects.requireNonNull( destinationConfig, "Specify non-null destination configuration." );

        OutputTypeSelection returnMe = OutputTypeSelection.DEFAULT;
        if ( Objects.nonNull( destinationConfig.getOutputType() ) )
        {
            returnMe = destinationConfig.getOutputType();
        }

        return returnMe;
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig} and the {@link SampleMetadata}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        return ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                  destinationConfig,
                                                  meta,
                                                  (String) null,
                                                  metricName,
                                                  metricComponentName );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link SampleMetadata}
     * associated with the results and a {@link TimeWindowOuter}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             TimeWindowOuter timeWindow,
                                             ChronoUnit leadUnits,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig, "Enter non-null time window to establish a path for writing." );

        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );

        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );

        return ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                  destinationConfig,
                                                  meta,
                                                  TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                                  leadUnits )
                                                        + "_"
                                                        + leadUnits.name().toUpperCase(),
                                                  metricName,
                                                  metricComponentName );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link SampleMetadata}
     * associated with the results and a {@link OneOrTwoThresholds}.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param threshold the threshold
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             OneOrTwoThresholds threshold,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( threshold, "Enter non-null threshold to establish a path for writing." );

        return getOutputPathToWrite( outputDirectory,
                                     destinationConfig,
                                     meta,
                                     threshold.toStringSafe(),
                                     metricName,
                                     metricComponentName );
    }

    /**
     * Returns a path to write from a combination of the destination configuration, the input metadata and any 
     * additional string that should be appended to the path (e.g. lead time or threshold). 
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param append an optional string to append to the end of the path, may be null
     * @param metricName the metric name
     * @param metricComponentName name the optional component name
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated 
     *            with the sample metadata
     * @throws IOException if the path cannot be produced
     * @throws ProjectConfigException when the destination configuration is invalid
     */

    public static Path getOutputPathToWrite( Path outputDirectory,
                                             DestinationConfig destinationConfig,
                                             SampleMetadata meta,
                                             String append,
                                             MetricConstants metricName,
                                             MetricConstants metricComponentName )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig,
                                "Enter non-null destination configuration to establish "
                                                   + "a path for writing." );

        Objects.requireNonNull( meta, ENTER_NON_NULL_METADATA_TO_ESTABLISH_A_PATH_FOR_WRITING );

        Objects.requireNonNull( meta.getIdentifier(),
                                "Enter a non-null identifier for the metadata to establish "
                                                      + "a path for writing." );

        Objects.requireNonNull( metricName, "Specify a non-null metric name." );

        // Build the path 
        StringJoiner joinElements = new StringJoiner( "_" );
        DatasetIdentifier identifier = meta.getIdentifier();
        Evaluation evaluation = meta.getEvaluation();
        Pool pool = meta.getPool();

        // Work-around to figure out if this is gridded data and if so to use
        // something other than the feature name, use the description.
        // When you make gridded benchmarks congruent, remove this.
        if ( identifier.getFeatureTuple()
                       .getRight()
                       .getName()
                       .matches( "^-?[0-9]+\\.[0-9]+ -?[0-9]+\\.[0-9]+$" ) )
        {
            LOGGER.debug( "Using ugly workaround for ugly gridded benchmarks: {}",
                          identifier );
            joinElements.add( identifier.getFeatureTuple()
                                        .getRight()
                                        .getDescription() );
        }
        else
        {
            joinElements.add( identifier.getFeatureTuple()
                                        .getRight()
                                        .getName() );
        }

        joinElements.add( identifier.getVariableName() );


        // Baseline scenarioId
        String configuredScenarioId = null;
        String configuredBaselineScenarioId = null;
        if ( !evaluation.getRightDataName().isBlank() )
        {
            configuredScenarioId = evaluation.getRightDataName();
        }

        if ( !evaluation.getBaselineDataName().isBlank() )
        {
            configuredBaselineScenarioId = evaluation.getBaselineDataName();
        }

        // Add optional scenario identifier unless the configured identifiers cannot discriminate between 
        // RIGHT and BASELINE 
        if ( identifier.hasScenarioName() && !Objects.equals( configuredScenarioId, configuredBaselineScenarioId ) )
        {
            joinElements.add( identifier.getScenarioName() );
        }
        // If there are metrics for both the RIGHT and BASELINE, then additionally qualify the context
        else if ( identifier.hasLeftOrRightOrBaseline()
                  && pool.getIsBaselinePool() )
        {
            joinElements.add( identifier.getLeftOrRightOrBaseline().toString() );
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

        // Add extension
        String extension;

        // Default graphic extension type
        if ( destinationConfig.getType() == DestinationType.GRAPHIC )
        {
            extension = ".png";
        }
        // Default numeric extension type
        else if ( destinationConfig.getType() == DestinationType.NUMERIC )
        {
            extension = ".csv";
        }
        // Specific type
        else
        {
            extension = destinationConfig.getType().name().toLowerCase();
        }

        // Derive a sanitized name
        String safeName = URLEncoder.encode( joinElements.toString().replace( " ", "_" ) + extension, "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }

    /**
     * Returns a formatted file name for writing outputs to a specific time window using the destination 
     * information and other hints provided.
     *
     * @param outputDirectory the directory into which to write
     * @param destinationConfig the destination information
     * @param identifier the dataset identifier
     * @param timeWindow the time window
     * @param leadUnits the time units to use for the lead durations
     * @return the file name
     * @throws NoDataException if the output is empty
     * @throws NullPointerException if any of the inputs is null
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWriteForOneTimeWindow( final Path outputDirectory,
                                                             final DestinationConfig destinationConfig,
                                                             final DatasetIdentifier identifier,
                                                             final TimeWindowOuter timeWindow,
                                                             final ChronoUnit leadUnits )
            throws IOException
    {
        Objects.requireNonNull( outputDirectory, "Enter non-null output directory to establish a path for writing." );
        Objects.requireNonNull( destinationConfig, "Enter non-null time window to establish a path for writing." );

        Objects.requireNonNull( timeWindow, "Enter a non-null time window  to establish a path for writing." );

        Objects.requireNonNull( leadUnits,
                                "Enter a non-null time unit for the lead durations to establish a path for writing." );

        StringJoiner filename = new StringJoiner( "_" );

        filename.add( identifier.getVariableName() );

        // Add optional scenario identifier
        if ( identifier.hasScenarioName() )
        {
            filename.add( identifier.getScenarioName() );
        }

        if ( !timeWindow.getLatestReferenceTime().equals( Instant.MAX ) )
        {
            String lastTime = timeWindow.getLatestReferenceTime().toString();

            // TODO: Format the last time in the style of "20180505T2046"
            // instead of "2018-05-05 20:46:00.000-0000"
            lastTime = lastTime.replace( "-", "" )
                               .replace( ":", "" )
                               .replace( "Z$", "" );

            filename.add( lastTime );
        }

        // Format the duration with the default format
        filename.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                     leadUnits ) ) );
        filename.add( leadUnits.name().toUpperCase() );

        String extension = "";
        if ( destinationConfig.getType() == DestinationType.NETCDF )
        {
            extension = ".nc";
        }

        // Sanitize file name
        String safeName = URLEncoder.encode( filename.toString().replace( " ", "_" ) + extension, "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }

    /**
     * Returns <code>true</code> if a generated baseline is required, otherwise <code>false</code>.
     * 
     * @param baselineConfig the declaration to inspect
     * @return true if a generated baseline is required
     */

    public static boolean hasGeneratedBaseline( DataSourceConfig baselineConfig )
    {
        // Currently only one generated type supported
        return Objects.nonNull( baselineConfig )
               && baselineConfig.getTransformation() == SourceTransformationType.PERSISTENCE;
    }

    /**
     * Returns <code>true</code> if a baseline is present, otherwise <code>false</code>.
     * 
     * @param projectConfig the declaration to inspect
     * @return true if a baseline is present
     * @throws NullPointerException if the input is null
     */

    public static boolean hasBaseline( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        return Objects.nonNull( projectConfig.getInputs() )
               && Objects.nonNull( projectConfig.getInputs().getBaseline() );
    }

    /**
     * Gets the desired time scale associated with the pair declaration, if any.
     * 
     * @param pairConfig the pair declaration
     * @return the desired time scale or null
     */

    public static TimeScaleOuter getDesiredTimeScale( PairConfig pairConfig )
    {
        TimeScaleOuter returnMe = null;

        if ( Objects.nonNull( pairConfig )
             && Objects.nonNull( pairConfig.getDesiredTimeScale() ) )
        {
            returnMe = TimeScaleOuter.of( pairConfig.getDesiredTimeScale() );
        }

        return returnMe;
    }

    /**
     * Returns a list of output formats in the input configuration that can be mutated incrementally.
     * 
     * @param projectConfig the project configuration
     * @return the output formats in the configuration that can be mutated incrementally or the empty set
     * @throws NullPointerException if the input is null
     */

    public static Set<DestinationType> getIncrementalFormats( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Outputs output = projectConfig.getOutputs();

        if ( Objects.nonNull( output ) )
        {
            return output.getDestination()
                         .stream()
                         .map( DestinationConfig::getType )
                         .filter( next -> next == DestinationType.NETCDF || next == DestinationType.PROTOBUF )
                         .collect( Collectors.toUnmodifiableSet() );
        }

        // Return empty set
        return Collections.emptySet();
    }

    /**
     * Get the feature names relevant to a particular dataSource.
     *
     * The declaration only references names, not complete feature identities,
     * therefore we cannot have a full feature at this point, nor do we get one
     * from a database here, because the purpose here is to read names only.
     *
     * A dataset will have complete feature identities which will be ingested
     * at ingest-time. But to bootstrap ingest, we start with names only, which
     * can limit requests for data from data sources. After ingest we will have
     * the ability to get the full list of features for a dataset.
     *
     * This method is intended to be called by readers and with a fully dense
     * project declaration of features. In other words the project declaration
     * should have already been filled out either by the caller or by WRES
     * control module earlier, to have complete feature correlations.
     *
     * Not all readers require declared features, so those projects including
     * solely CSV or PI-XML, for example, will not need this method.
     *
     * This method is also used by FeatureFinder to get what is available from
     * a sparse declaration. It will give a dense declaration to the rest of the
     * evaluation pipeline so that reader will have a dense declaration.
     *
     * @param projectDeclaration The project declaration.
     * @param sourceDeclaration The source declared within the declaration.
     * @return A Set of String from the given declaration.
     * @throws UnsupportedOperationException When called with no features.
     */

    public static Set<String> getFeatureNamesForSource( ProjectConfig projectDeclaration,
                                                        DataSourceConfig sourceDeclaration )
    {
        SortedSet<String> featureNames = new TreeSet<>();
        List<Feature> featuresConfigured = projectDeclaration.getPair()
                                                             .getFeature();

        if ( featuresConfigured.isEmpty() )
        {
            // TODO: decide whether to ingest ALL 2.7m features or throw
            throw new UnsupportedOperationException( "Must configure features or specify a service to resolve features." );

        }

        LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( projectDeclaration,
                                                                           sourceDeclaration );
        // Reference equality on purpose here.
        if ( lrb.equals( LeftOrRightOrBaseline.LEFT ) )
        {
            for ( Feature featureConfigured : featuresConfigured )
            {
                String leftName = featureConfigured.getLeft();

                if ( Objects.nonNull( leftName ) )
                {
                    if ( leftName.isBlank() )
                    {
                        LOGGER.warn( "Encountered blank name in left feature declaration {}",
                                     featureConfigured );
                    }
                    else
                    {
                        featureNames.add( featureConfigured.getLeft() );
                    }
                }
            }
        }
        else if ( lrb.equals( LeftOrRightOrBaseline.RIGHT ) )
        {
            for ( Feature featureConfigured : featuresConfigured )
            {
                String rightName = featureConfigured.getRight();

                if ( Objects.nonNull( rightName ) )
                {
                    if ( rightName.isBlank() )
                    {
                        LOGGER.warn( "Encountered blank name in right feature declaration {}",
                                     featureConfigured );
                    }
                    else
                    {
                        featureNames.add( rightName );
                    }
                }
            }
        }
        else if ( lrb.equals( LeftOrRightOrBaseline.BASELINE )
                  && Objects.nonNull( projectDeclaration.getInputs()
                                                        .getBaseline() ) )
        {
            for ( Feature featureConfigured : featuresConfigured )
            {
                String baselineName = featureConfigured.getBaseline();

                if ( Objects.nonNull( baselineName ) )
                {
                    if ( baselineName.isBlank() )
                    {
                        LOGGER.warn( "Encountered blank name in baseline feature declaration {}",
                                     featureConfigured );
                    }
                    else
                    {
                        featureNames.add( featureConfigured.getBaseline() );
                    }
                }
            }
        }

        return Collections.unmodifiableSortedSet( featureNames );
    }
}
