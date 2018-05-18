package wres.io.config;

import static wres.config.generated.SourceTransformationType.PERSISTENCE;
import static wres.io.data.details.ProjectDetails.PairingMode.ROLLING;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.MetricConfigException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Outputs;
import wres.config.generated.ThresholdFormat;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.io.Operations;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.commaseparated.CommaSeparatedReader;
import wres.io.utilities.Database;
import wres.io.writing.SharedWriters;
import wres.io.writing.WriterHelper;
import wres.io.writing.netcdf.NetcdfDoubleScoreWriter;
import wres.util.Strings;
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
    private static final Logger LOGGER = LoggerFactory.getLogger( ConfigHelper.class );

    private static final ConcurrentMap<ProjectConfig, ConcurrentSkipListSet<String>> messages =
            new ConcurrentHashMap<>();

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

    public static Request getGridDataRequest(
            final ProjectDetails projectDetails,
            final DataSourceConfig dataSourceConfig,
            final Feature feature) throws SQLException
    {
        Request griddedRequest = Fetcher.prepareRequest();
        griddedRequest.addFeature( feature );
        griddedRequest.setVariableName( dataSourceConfig.getVariable().getValue() );

        boolean isForecast = ConfigHelper.isForecast( dataSourceConfig );

        griddedRequest.setIsForecast( isForecast );

        if (isForecast && projectDetails.getMinimumLeadHour() > Integer.MIN_VALUE)
        {
            griddedRequest.setEarliestLead(
                    Duration.of(projectDetails.getMinimumLeadHour(), TimeHelper.LEAD_RESOLUTION)
            );
        }

        if (isForecast && projectDetails.getMaximumLeadHour() < Integer.MAX_VALUE)
        {
            griddedRequest.setLatestLead(
                    Duration.of(projectDetails.getMaximumLeadHour(), TimeHelper.LEAD_RESOLUTION)
            );
        }

        if (projectDetails.getEarliestDate() != null)
        {
            griddedRequest.setEarliestValidTime( Instant.parse( projectDetails.getEarliestDate() ) );
        }

        if (projectDetails.getLatestDate() != null)
        {
            griddedRequest.setLatestValidTime( Instant.parse( projectDetails.getLatestDate() ) );
        }

        if (isForecast && projectDetails.getEarliestIssueDate() != null)
        {
            griddedRequest.setEarliestIssueTime( Instant.parse(projectDetails.getEarliestIssueDate()) );
        }

        if (isForecast && projectDetails.getLatestIssueDate() != null)
        {
            griddedRequest.setLatestIssueTime( Instant.parse(projectDetails.getLatestIssueDate()) );
        }

        DataSources.getSourcePaths( projectDetails, dataSourceConfig ).forEach( griddedRequest::addPath );

        return griddedRequest;
    }

    // TODO: Move to Project Details
    // ... or wres-config if useful outside of wres-io
    public static boolean usesUSGSData( ProjectConfig projectConfig )
    {
        for ( DataSourceConfig.Source source : projectConfig.getInputs().getLeft().getSource() )
        {
            if ( source.getFormat() == Format.USGS )
            {
                return true;
            }
        }

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getRight().getSource() )
        {
            if ( source.getFormat() == Format.USGS )
            {
                return true;
            }
        }

        if ( projectConfig.getInputs().getBaseline() != null )
        {
            for ( DataSourceConfig.Source source : projectConfig.getInputs()
                                                                .getBaseline()
                                                                .getSource() )
            {
                if ( source.getFormat() == Format.USGS )
                {
                    return true;
                }
            }
        }

        return false;
    }

    // TODO: Move to Project Details
    // ... or wres-config if useful outside of wres-io
    public static boolean usesNetCDFData( ProjectConfig projectConfig )
    {
        for ( DataSourceConfig.Source source : projectConfig.getInputs().getLeft().getSource() )
        {
            if ( source.getFormat() == Format.NET_CDF )
            {
                return true;
            }
        }

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getRight().getSource() )
        {
            if ( source.getFormat() == Format.NET_CDF )
            {
                return true;
            }
        }

        if ( projectConfig.getInputs().getBaseline() != null )
        {
            for ( DataSourceConfig.Source source : projectConfig.getInputs()
                                                                .getBaseline()
                                                                .getSource() )
            {
                if ( source.getFormat() == Format.NET_CDF )
                {
                    return true;
                }
            }
        }

        return false;
    }

    public static String getVariablePositionClause( Feature feature, int variableId, String alias )
            throws SQLException
    {
        StringBuilder clause = new StringBuilder();

        Integer variablePositionId = Features.getVariablePositionID( feature, variableId );

        if ( variablePositionId != null )
        {
            if ( Strings.hasValue( alias ) )
            {
                clause.append( alias ).append( "." );
            }

            clause.append( "variableposition_id = " ).append( variablePositionId );
        }

        return clause.toString();
    }

    public static Comparator<Feature> getFeatureComparator()
    {
        return ( feature, t1 ) -> {

            String featureDescription = getFeatureDescription( feature );
            String t1Description = getFeatureDescription( t1 );

            if ( Strings.hasValue( feature.getLocationId() ) &&
                 Strings.hasValue( t1.getLocationId() ) )
            {
                return feature.getLocationId().compareTo( t1.getLocationId() );
            }
            else
            {
                LOGGER.trace( "Either {} and {} have the same location ids or "
                              + "one or both of them don't have one.",
                              featureDescription,
                              t1Description );
            }

            if ( Strings.hasValue( feature.getGageId() ) &&
                 Strings.hasValue( t1.getGageId() ) )
            {
                return feature.getGageId().compareTo( t1.getGageId() );
            }
            else
            {
                LOGGER.trace( "Either {} and {} have the same gage ids or "
                              + "one or both of them don't have one.",
                              featureDescription,
                              t1Description );
            }

            if ( feature.getComid() != null &&
                 t1.getComid() != null )
            {
                return feature.getComid().compareTo( t1.getComid() );
            }
            else
            {
                LOGGER.trace( "Either {} and {} have the same com ids or "
                              + "one or both of them don't have one.",
                              featureDescription,
                              t1Description );
            }

            LOGGER.warn( "A proper comparison couldn't be made between {} and {}."
                         + " Now saying that {} is greater than {}.",
                         featureDescription,
                         t1Description,
                         featureDescription,
                         t1Description );
            return 1;
        };
    }

    public static int getValueCount( ProjectDetails projectDetails,
                                     DataSourceConfig dataSourceConfig,
                                     Feature feature )
            throws SQLException
    {
        final String NEWLINE = System.lineSeparator();
        Integer variableId;
        String member;

        if ( projectDetails.getRight().equals( dataSourceConfig ) )
        {
            variableId = projectDetails.getRightVariableID();
            member = ProjectDetails.RIGHT_MEMBER;
        }
        else if ( projectDetails.getLeft().equals( dataSourceConfig ) )
        {
            variableId = projectDetails.getLeftVariableID();
            member = ProjectDetails.LEFT_MEMBER;
        }
        else
        {
            variableId = projectDetails.getBaselineVariableID();
            member = ProjectDetails.BASELINE_MEMBER;
        }

        String variablePositionClause = ConfigHelper.getVariablePositionClause( feature, variableId, "" );

        StringBuilder script = new StringBuilder( "SELECT COUNT(*)::int" ).append( NEWLINE );

        if ( ConfigHelper.isForecast( dataSourceConfig ) )
        {
            script.append( "FROM wres.TimeSeries TS" ).append( NEWLINE );
            script.append( "INNER JOIN wres.ForecastValue FV" ).append( NEWLINE );
            script.append( "    ON TS.timeseries_id = FV.timeseries_id" ).append( NEWLINE );
            script.append( "WHERE " ).append( variablePositionClause ).append( NEWLINE );
            script.append( "    EXISTS (" ).append( NEWLINE );
            script.append( "        SELECT 1" ).append( NEWLINE );
            script.append( "        FROM wres.ForecastSource FS" ).append( NEWLINE );
            script.append( "        INNER JOIN wres.ProjectSource PS" ).append( NEWLINE );
            script.append( "            ON FS.source_id = PS.source_id" ).append( NEWLINE );
            script.append( "        WHERE PS.project_id = " ).append( projectDetails.getId() ).append( NEWLINE );
            script.append( "            AND PS.member = " ).append( member ).append( NEWLINE );
            script.append( "            AND PS.inactive_time IS NULL" ).append( NEWLINE );
            script.append( "            AND FS.forecast_id = TS.timeseries_id" ).append( NEWLINE );
            script.append( "    );" );
        }
        else
        {
            script.append( "FROM wres.Observation O" ).append( NEWLINE );
            script.append( "WHERE " ).append( variablePositionClause ).append( NEWLINE );
            script.append( "    AND EXISTS (" ).append( NEWLINE );
            script.append( "        SELECT 1" ).append( NEWLINE );
            script.append( "        FROM wres.ProjectSource PS" ).append( NEWLINE );
            script.append( "        WHERE PS.project_id = " ).append( projectDetails.getId() ).append( NEWLINE );
            script.append( "            AND PS.member = " ).append( member ).append( NEWLINE );
            script.append( "            AND PS.source_id = O.source_id" ).append( NEWLINE );
            script.append( "            AND PS.inactive_time IS NULL" ).append( NEWLINE );
            script.append( "    );" );
        }

        return Database.getResult( script.toString(), "count" );
    }

    public static boolean isForecast( DataSourceConfig dataSource )
    {
        return dataSource != null &&
               Strings.isOneOf( dataSource.getType().value(),
                                DatasourceType.SINGLE_VALUED_FORECASTS.value(),
                                DatasourceType.ENSEMBLE_FORECASTS.value() );
    }

    public static boolean isSimulation( DataSourceConfig dataSourceConfig )
    {
        return dataSourceConfig != null
               && dataSourceConfig.getType() == DatasourceType.SIMULATIONS;
    }

    public static ProjectConfig read( final String path ) throws IOException
    {
        Path actualPath = Paths.get( path );
        ProjectConfigPlus configPlus = ProjectConfigPlus.from( actualPath );
        return configPlus.getProjectConfig();
    }

    /**
     * // TODO: document what returning null means to this method to let the
     * caller decide what can or can't be done with a null response.
     * @param dataSourceConfig ?
     * @param filename ?
     * @return null when ______ (What does it mean for findDataSourceByFilename
     * to return null?)
     */
    public static DataSourceConfig.Source findDataSourceByFilename( DataSourceConfig dataSourceConfig, String filename )
    {
        DataSourceConfig.Source source = null;
        filename = Paths.get( filename ).toAbsolutePath().toString();
        String sourcePath = "";

        for ( DataSourceConfig.Source dataSource : dataSourceConfig.getSource() )
        {
            String fullDataSourcePath = Paths.get( dataSource.getValue() ).toAbsolutePath().toString();

            if ( filename.startsWith( fullDataSourcePath ) && fullDataSourcePath.length() > sourcePath.length() )
            {
                sourcePath = fullDataSourcePath;
                source = dataSource;
            }
        }

        return source;
    }

    /**
     * Returns the "earliest" datetime from given ProjectConfig Conditions
     *
     * TODO: Move to ProjectDetails
     *
     * @param config the project configuration
     * @return the most narrow "earliest" date, null otherwise
     */
    public static Instant getEarliestDateTimeFromDataSources( ProjectConfig config )
    {
        if ( config.getPair() == null )
        {
            return null;
        }

        String earliest = "";

        if ( config.getPair()
                   .getDates() != null
             && config.getPair()
                      .getDates()
                      .getEarliest() != null )
        {
            try
            {
                earliest = config.getPair()
                                 .getDates()
                                 .getEarliest();
                return Instant.parse( earliest );
            }
            catch ( DateTimeParseException dtpe )
            {
                String messageId = "date_parse_exception_earliest_date";
                if ( LOGGER.isWarnEnabled()
                     && ConfigHelper.messageSendPutIfAbsent( config,
                                                             messageId ) )
                {
                    LOGGER.warn( "Correct the date \"{}\" near line {} column "
                                 + "{} to ISO8601 format such as "
                                 + "\"2017-06-27T16:16:00Z\"",
                                 earliest,
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getLineNumber(),
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getColumnNumber() );
                }
                return null;
            }
        }
        else
        {
            String messageId = "no_earliest_date";
            if ( LOGGER.isInfoEnabled() && ConfigHelper.messageSendPutIfAbsent( config, messageId ) )
            {
                LOGGER.info( "No \"earliest\" date found in project. Use <dates earliest=\"2017-06-27T16:14:00Z\" latest=\"2017-07-06T11:35:00Z\" /> under <pair> (near line {} column {} of project file) to specify an earliest date.",
                             config.getPair()
                                   .sourceLocation()
                                   .getLineNumber(),
                             config.getPair()
                                   .sourceLocation()
                                   .getColumnNumber() );
            }
            return null;
        }
    }

    /**
     * Returns the earlier of any "latest" date specified in left or right datasource.
     * If only one date is specified, that one is returned.
     * If no dates for "latest" are specified, null is returned.
     *
     * TODO: Move to ProjectDetails
     *
     * @param config the project configuration
     * @return the most narrow "latest" date, null otherwise.
     */
    public static Instant getLatestDateTimeFromDataSources( ProjectConfig config )
    {
        if ( config.getPair() == null )
        {
            return null;
        }

        String latest = "";

        if ( config.getPair()
                   .getDates() != null
             && config.getPair()
                      .getDates()
                      .getLatest() != null )
        {
            try
            {
                latest = config.getPair()
                               .getDates()
                               .getLatest();
                return Instant.parse( latest );
            }
            catch ( DateTimeParseException dtpe )
            {
                String messageId = "date_parse_exception_latest_date";
                if ( LOGGER.isWarnEnabled()
                     && ConfigHelper.messageSendPutIfAbsent( config,
                                                             messageId ) )
                {
                    LOGGER.warn( "Correct the date \"{}\" after line {} col {} "
                                 + "to ISO8601 format such as "
                                 + "\"2017-06-27T16:16:00Z\"",
                                 latest,
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getLineNumber(),
                                 config.getPair()
                                       .getDates()
                                       .sourceLocation()
                                       .getColumnNumber() );
                }
                return null;
            }
        }
        else
        {
            String messageId = "no_latest_date";
            if ( LOGGER.isInfoEnabled() && ConfigHelper.messageSendPutIfAbsent( config, messageId ) )
            {
                LOGGER.info( "No \"latest\" date found in project. Use <dates earliest=\"2017-06-27T16:14:00Z\" latest=\"2017-07-06T11:35:00Z\" />  under <pair> (near line {} col {} of project file) to specify a latest date.",
                             config.getPair()
                                   .sourceLocation()
                                   .getLineNumber(),
                             config.getPair()
                                   .sourceLocation()
                                   .getColumnNumber() );

            }
            return null;
        }
    }

    /**
     * Returns true if the caller is the one who should log a particular message.
     *
     * The exact message is not contained here, just an ad-hoc ID for it,
     * created by the caller.
     *
     * May be too clever, may have a race condition. Rather have race condition
     * than too much locking, this is just for messaging.
     *
     * The idea is that when only one message should appear for the user about
     * a particular validation issue in the configuration (but multiple tasks
     * are able to log this message), the caller first asks this method if it
     * should be the one to log the validation message.
     *
     * @param projectConfig the configuration object to send a message about
     * @param message the identifier for the message to send
     * @return true if the caller should log
     */
    private static boolean messageSendPutIfAbsent( ProjectConfig projectConfig,
                                                   String message )
    {
        // In case we are the first to call regarding a given config:
        ConcurrentSkipListSet<String> possiblyNewSet = new ConcurrentSkipListSet<>();
        possiblyNewSet.add( message );

        ConcurrentSkipListSet<String> theSet = messages.putIfAbsent( projectConfig,
                                                                     possiblyNewSet );
        if ( theSet == null )
        {
            // this call was first to put a set for this config, return true.
            return true;
        }
        // this call was not the first to put a set for this config.
        return theSet.add( message );
    }

    // TODO: Should this move to wres.io.data.caching.Features?
    public static String getFeatureDescription( Feature feature )
    {
        String description = null;

        if ( feature != null )
        {
            if ( feature.getLocationId() != null
                 && !feature.getLocationId()
                            .trim()
                            .isEmpty() )
            {
                description = feature.getLocationId();
            }
            else if ( feature.getGageId() != null
                      && !feature.getGageId()
                                 .trim()
                                 .isEmpty() )
            {
                description = feature.getGageId();
            }
            else if ( feature.getComid() != null )
            {
                description = String.valueOf( feature.getComid() );
            }
            else if ( Strings.hasValue( feature.getName() ) )
            {
                description = feature.getName();
            }
            else if (feature.getCoordinate() != null)
            {
                description = feature.getCoordinate().getLongitude() + " " +
                              feature.getCoordinate().getLatitude();
            }
        }

        return description;
    }

    public static String getFeatureDescription( FeaturePlus featurePlus )
    {
        return ConfigHelper.getFeatureDescription( featurePlus.getFeature() );
    }


    /**
     * Get a comma separated description of a list of features.
     *
     * TODO: Should this move to wres.io.data.caching.Features?
     *
     * @param features the list of features to describe, nonnull
     * @return a description of all features
     * @throws NullPointerException when list of features is null
     */
    public static String getFeaturesDescription( List<Feature> features )
    {
        Objects.requireNonNull( features );
        StringJoiner result = new StringJoiner( ", ", "( ", " )" );

        for ( Feature feature : features )
        {
            String description = ConfigHelper.getFeatureDescription( feature );
            result.add( description );
        }

        return result.toString();
    }

    /**
     * Convert a DestinationConfig into a directory to write to.
     *
     * @param d the destination configuration element to read
     * @return a File referring to a directory to write to
     * @throws ProjectConfigException when the path inside d is null
     * @throws NullPointerException when d is null
     */
    public static File getDirectoryFromDestinationConfig( DestinationConfig d )
    {
        Path outputDirectory = Paths.get( d.getPath() );

        if ( outputDirectory == null )
        {
            String message = "Destination path " + d.getPath()
                             +
                             " could not be found.";
            throw new ProjectConfigException( d, message );
        }
        else
        {
            File outputLocation = outputDirectory.toFile();
            if ( outputLocation.isDirectory() )
            {
                return outputLocation;
            }
            else if ( outputLocation.isFile() )
            {
                // Use parent directory, warn user
                LOGGER.warn( "Using parent directory {} for output instead of "
                             + "{} because there may be more than one file to "
                             + "write.",
                             outputDirectory.getParent(),
                             outputDirectory );

                return outputDirectory.getParent().toFile();
            }
            else
            {
                // If we have neither a file nor a directory, is issue.
                String message = "Destination path " + d.getPath()
                                 + " needs to be changed to a directory"
                                 + " that can be written to.";
                throw new ProjectConfigException( d, message );
            }
        }
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
     * <p>Returns a {@link TimeWindow} from the input configuration using the specified lead time to form the interval
     * on the forecast horizon. The earliest and latest times on the UTC timeline are determined by whichever of the
     * following is available (in this order):</p> 
     * 
     * <ol>
     * <li>The valid times in {@link DateCondition#getEarliest()} and {@link DateCondition#getLatest()}; or</li>
     * <li>The issue times in {@link DateCondition#getEarliest()} and the {@link DateCondition#getLatest()}; or</li>
     * <li>The earliest and latest possible dates on the UTC timeline in valid time, namely {@link Instant#MIN} and 
     * {@link Instant#MAX}, respectively.</li>
     * </ol>
     * 
     * <p>Dates are parsed using {@link Instant#parse(CharSequence)} and an exception is thrown if the dates do not meet 
     * the associated formatting requirement. Validation of dates should be conducted at the earliest 
     * opportunity, which may be well before this point.</p>
     * 
     * @param projectDetails the project configuration
     * @param firstLead the earliest lead time
     * @param lastLead the latest lead time
     * @param sequenceStep the position of the window within a sequence
     * @return a time window 
     * @throws NullPointerException if the config is null
     * @throws DateTimeParseException if the configuration contains dates that cannot be parsed
     */
    public static TimeWindow getTimeWindow( ProjectDetails projectDetails,
                                            Duration firstLead,
                                            Duration lastLead,
                                            int sequenceStep )
    {
        // TODO: simplify this method, if possible
        Objects.requireNonNull( projectDetails );

        Instant earliestTime;
        Instant latestTime;

        Duration beginningLead;
        Duration endingLead = lastLead;

        // Default reference time
        ReferenceTime referenceTime = ReferenceTime.VALID_TIME;

        if ( projectDetails.usesTimeSeriesMetrics() )
        {
            beginningLead = firstLead;

            referenceTime = ReferenceTime.ISSUE_TIME;
        }
        else if ( projectDetails.getProjectConfig().getPair().getLeadTimesPoolingWindow() != null )
        {
            PoolingWindowConfig leadPoolingWindow =
                    projectDetails.getProjectConfig().getPair().getLeadTimesPoolingWindow();

            beginningLead =
                    endingLead.minus( leadPoolingWindow.getPeriod(),
                                      ChronoUnit.valueOf( leadPoolingWindow.getUnit().toString().toUpperCase() ) );
        }
        else
        {
            beginningLead = endingLead;
        }

        if ( projectDetails.getPairingMode() == ROLLING )
        {
            long frequencyOffset = TimeHelper.unitsToLeadUnits( projectDetails.getIssuePoolingWindowUnit(),
                                                                projectDetails.getIssuePoolingWindowFrequency() )
                                   * sequenceStep;

            earliestTime = Instant.parse( projectDetails.getEarliestIssueDate() );
            earliestTime = earliestTime.plus( frequencyOffset, ChronoUnit.HOURS );

            if ( projectDetails.getIssuePoolingWindowPeriod() > 0 )
            {
                latestTime = earliestTime.plus( projectDetails.getIssuePoolingWindowPeriod(),
                                                ChronoUnit.valueOf( projectDetails.getIssuePoolingWindowUnit()
                                                                                  .toUpperCase() ) );
            }
            else
            {
                latestTime = earliestTime;
            }

            referenceTime = ReferenceTime.ISSUE_TIME;

        }
        //Valid dates available
        else if ( projectDetails.getEarliestDate() != null && projectDetails.getLatestDate() != null )
        {
            earliestTime = Instant.parse( projectDetails.getEarliestDate() );
            latestTime = Instant.parse( projectDetails.getLatestDate() );
        }
        //Issue dates available
        else if ( projectDetails.getEarliestIssueDate() != null && projectDetails.getLatestIssueDate() != null )
        {
            earliestTime = Instant.parse( projectDetails.getEarliestIssueDate() );
            latestTime = Instant.parse( projectDetails.getLatestIssueDate() );

            referenceTime = ReferenceTime.ISSUE_TIME;

        }
        //No dates available
        else
        {
            earliestTime = Instant.MIN;
            latestTime = Instant.MAX;
        }

        return TimeWindow.of( earliestTime,
                              latestTime,
                              referenceTime,
                              beginningLead,
                              endingLead );
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


    /**
     * Creates a SQL snippet expected by the ScriptGenerator as part of a where
     * clause, filtering by season specified in the configuration passed in.
     * <br>
     * Caller is responsible for saying which column to compare to.
     * @param projectDetails the configuration, non-null
     * @param databaseColumnName the column name with a date or time to use
     * @param timeShift the amount of time to shift, null otherwise
     * @return a where clause sql snippet or empty string if no season
     * @throws NullPointerException when projectDetails or databaseColumnName is null
     * @throws DateTimeException when the values in the season are invalid
     */

    public static String getSeasonQualifier( ProjectDetails projectDetails,
                                             String databaseColumnName,
                                             Integer timeShift )
    {
        Objects.requireNonNull( projectDetails, "projectDetails needs to exist" );
        Objects.requireNonNull( databaseColumnName, "databaseColumnName needs to exist" );
        StringBuilder s = new StringBuilder();

        // This admittedly makeshift MONTH_MULTIPLIIER is to put the month in
        // the most significant place in a unified "month and day" integer.
        // This was simpler than trying to concatenate strings. Why? Because
        // SQL EXTRACT will only pull out one digit for single digit months.
        // Maybe could have used postgres-specific SQL, but this technique seems
        // to work OK so far. If a more elegant or more straightforward way is
        // found, by all means, eliminate the MONTH_MULTIPLIER.
        final int MONTH_MULTIPLIER = 100;

        if ( projectDetails.specifiesSeason() )
        {
            MonthDay earliest = projectDetails.getEarliestDayInSeason();
            MonthDay latest = projectDetails.getLatestDayInSeason();

            s.append( "     AND ( " );
            s.append( MONTH_MULTIPLIER );
            s.append( " * " );

            s.append( ConfigHelper.getExtractSqlSnippet( "month",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " + " );

            s.append( ConfigHelper.getExtractSqlSnippet( "day",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " >= " );

            s.append( earliest.getMonthValue() * MONTH_MULTIPLIER
                      + earliest.getDayOfMonth() );

            if ( earliest.isAfter( latest ) )
            {
                s.append( " OR " );
            }
            else
            {
                s.append( " AND " );
            }

            s.append( MONTH_MULTIPLIER );
            s.append( " * " );

            s.append( ConfigHelper.getExtractSqlSnippet( "month",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " + " );

            s.append( ConfigHelper.getExtractSqlSnippet( "day",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " <= " );

            s.append( latest.getMonthValue() * MONTH_MULTIPLIER
                      + latest.getDayOfMonth() );

            s.append( " )" );
            s.append( System.lineSeparator() );
        }

        LOGGER.trace( "{}", s );

        return s.toString();
    }

    private static String getExtractSqlSnippet( String toExtract,
                                                String databaseColumnName,
                                                Integer timeShift )
    {
        StringBuilder s = new StringBuilder();

        s.append( "EXTRACT( " );
        s.append( toExtract );
        s.append( " from " );
        s.append( databaseColumnName );

        if ( timeShift != null )
        {
            s.append( " + INTERVAL '1 HOUR' * " );
            s.append( timeShift );
        }

        s.append( " )" );

        return s.toString();
    }


    /**
     * Given a config and a data source, return which kind the datasource is
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

        if ( config.equals( left ) )
        {
            return LeftOrRightOrBaseline.LEFT;
        }
        else if ( config.equals( right ) )
        {
            return LeftOrRightOrBaseline.RIGHT;
        }
        else if ( config.equals( baseline ) )
        {
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
     * Returns true if the input contains instantaneous data, false otherwise. A {@link TimeScaleConfig} is
     * considered instantaneous if the {@link TimeScaleConfig#getPeriod()} is 1 and the
     * {@link TimeScaleConfig#getUnit()} is {@link DurationUnit#NANOS}.
     * 
     * @param input the input to test
     * @return true if the input aggregation denotes instantaneous data, false otherwise
     * @throws NullPointerException if the input is null
     */

    public static boolean isInstantaneous( TimeScaleConfig input )
    {
        Objects.requireNonNull( input, "Specify non-null input to check for instantanous data." );
        return input.getUnit().equals( DurationUnit.NANOS ) && input.getPeriod() == 1;
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
     * Get a duration of a period from a timescale config
     * @param timeScaleConfig the config
     * @return the duration
     */
    public static Duration getDurationFromTimeScale( TimeScaleConfig timeScaleConfig )
    {
        ChronoUnit unit = ChronoUnit.valueOf( timeScaleConfig.getUnit()
                                                             .value()
                                                             .toUpperCase() );
        return Duration.of( timeScaleConfig.getPeriod(), unit );
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
     * <p>Returns the variable identifier from the project configuration. The identifier is one of the following in 
     * order of precedent:</p>
     * 
     * <p>If the variable identifier is required for the left and right:</p>
     * <ol>
     * <li>The label associated with the variable in the left source.</li>
     * <li>The label associated with the variable in the right source.</li>
     * <li>The value associated with the left variable.</li>
     * </ol>
     * 
     * <p>If the variable identifier is required for the baseline:</p>
     * <ol>
     * <li>The label associated with the variable in the baseline source.</li>
     * <li>The value associated with the baseline variable.</li>
     * </ol>
     * 
     * <p>In both cases, the last declaration is always present.</p>
     * 
     * @param projectConfig the project configuration
     * @param isBaseline is true if the variable name is required for the baseline
     * @return the variable identifier
     * @throws IllegalArgumentException if the baseline variable is requested and the input does not contain 
     *            a baseline source
     */

    public static String getVariableIdFromProjectConfig( ProjectConfig projectConfig, boolean isBaseline )
    {
        // Baseline required?
        if ( isBaseline )
        {
            // Has a baseline source
            if ( Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
            {
                // Has a baseline source with a label
                if ( Objects.nonNull( projectConfig.getInputs().getBaseline().getLabel() ) )
                {
                    return projectConfig.getInputs().getBaseline().getVariable().getLabel();
                }
                // Only has a baseline source with a variable value
                return projectConfig.getInputs().getBaseline().getVariable().getValue();
            }
            throw new IllegalArgumentException( "Cannot identify the variable for the baseline as the input project "
                                                + "does not contain a baseline source." );
        }
        // Has a left source with a label 
        if ( Objects.nonNull( projectConfig.getInputs().getLeft().getVariable().getLabel() ) )
        {
            return projectConfig.getInputs().getLeft().getVariable().getLabel();
        }
        // Has a right source with a label
        else if ( Objects.nonNull( projectConfig.getInputs().getRight().getVariable().getLabel() ) )
        {
            return projectConfig.getInputs().getRight().getVariable().getLabel();
        }
        // Has a left source with a variable value
        return projectConfig.getInputs().getLeft().getVariable().getValue();
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig} and the {@link MetricOutputMetadata}.
     * 
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @return a path to write
     * @throws NullPointerException if any input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( DestinationConfig destinationConfig,
                                             MetricOutputMetadata meta )
            throws IOException
    {
        return ConfigHelper.getOutputPathToWrite( destinationConfig, meta, (String) null );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link MetricOutputMetadata} 
     * associated with the results and a {@link TimeWindow}.
     * 
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param timeWindow the time window
     * @return a path to write
     * @throws NullPointerException if any input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( DestinationConfig destinationConfig,
                                             MetricOutputMetadata meta,
                                             TimeWindow timeWindow )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig, "Enter non-null time window to establish a path for writing." );

        return ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                  meta,
                                                  timeWindow.getLatestLeadTimeInHours() + "_HOUR" );
    }

    /**
     * Returns a path to write from a combination of the {@link DestinationConfig}, the {@link MetricOutputMetadata} 
     * associated with the results and a {@link OneOrTwoThresholds}.
     * 
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param threshold the threshold
     * @return a path to write
     * @throws NullPointerException if any input is null, including the identifier associated with the metadata
     * @throws IOException if the path cannot be produced
     */

    public static Path getOutputPathToWrite( DestinationConfig destinationConfig,
                                             MetricOutputMetadata meta,
                                             OneOrTwoThresholds threshold )
            throws IOException
    {
        Objects.requireNonNull( meta, "Enter non-null metadata to establish a path for writing." );

        Objects.requireNonNull( threshold, "Enter non-null threshold to establish a path for writing." );

        return getOutputPathToWrite( destinationConfig, meta, threshold.toStringSafe() );
    }

    /**
     * Returns a path to write from a combination of the destination configuration, the input metadata and any 
     * additional string that should be appended to the path (e.g. lead time or threshold). 
     * 
     * @param destinationConfig the destination configuration
     * @param meta the metadata
     * @param append an optional string to append to the end of the path, may be null
     * @return a path to write
     * @throws NullPointerException if any required input is null, including the identifier associated 
     *            with the metadata
     * @throws IOException if the path cannot be produced
     * @throws ProjectConfigException when the destination configuration is invalid
     */

    public static Path getOutputPathToWrite( DestinationConfig destinationConfig,
                                             MetricOutputMetadata meta,
                                             String append )
            throws IOException
    {
        Objects.requireNonNull( destinationConfig, "Enter non-null destination configuration to establish "
                                                   + "a path for writing." );

        Objects.requireNonNull( meta, "Enter non-null metadata to establish a path for writing." );

        Objects.requireNonNull( meta.getIdentifier(), "Enter a non-null identifier for the metadata to establish "
                                                      + "a path for writing." );

        // Determine the directory
        File outputDirectory = getDirectoryFromDestinationConfig( destinationConfig );

        // Build the path 
        StringJoiner joinElements = new StringJoiner( "_" );
        joinElements.add( meta.getIdentifier().getGeospatialID() )
                    .add( meta.getIdentifier().getVariableID() );

        // Add optional scenario identifier
        if ( meta.getIdentifier().hasScenarioID() )
        {
            joinElements.add( meta.getIdentifier().getScenarioID() );
        }

        // Add the metric name()
        joinElements.add( meta.getMetricID().name() );

        // Add a non-default component name
        if ( meta.hasMetricComponentID() && MetricConstants.MAIN != meta.getMetricComponentID() )
        {
            joinElements.add( meta.getMetricComponentID().name() );
        }

        // Add optional append
        if ( Objects.nonNull( append ) )
        {
            joinElements.add( append );
        }

        // Add extension
        String extension;
        if ( destinationConfig.getType() == DestinationType.GRAPHIC )
        {
            extension = ".png";
        }
        else
        {
            extension = ".csv";
        }

        // Derive a sanitized name
        String safeName = URLEncoder.encode( joinElements.toString().replace( " ", "_" ) + extension, "UTF-8" );

        return Paths.get( outputDirectory.toString(), safeName );
    }
    
    /**
     * Reads all external sources of thresholds from the input configuration and returns a map containing a set of 
     * {@link Threshold} for each {@link FeaturePlus} in the source. If no source is provided, an empty map is
     * returned.
     * 
     * @param projectConfig the project configuration
     * @return the thresholds associated with each feature obtained from a source in the project configuration
     * @throws MetricConfigException if the metric configuration is invalid
     */

    public static Map<FeaturePlus, ThresholdsByMetric>
            readExternalThresholdsFromProjectConfig( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        Map<FeaturePlus, ThresholdsByMetric> returnMe = new HashMap<>();

        // Obtain and read thresholds
        List<MetricsConfig> metrics = projectConfig.getMetrics();

        // Obtain any units for non-probability thresholds
        Dimension units = null;
        if ( Objects.nonNull( projectConfig.getPair() ) && Objects.nonNull( projectConfig.getPair().getUnit() ) )
        {
            DataFactory dataFactory = DefaultDataFactory.getInstance();
            units = dataFactory.getMetadataFactory().getDimension( projectConfig.getPair().getUnit() );
        }

        for ( MetricsConfig nextGroup : metrics )
        {

            // Obtain the set of external thresholds to read
            Set<ThresholdsConfig> external = nextGroup.getThresholds()
                                                      .stream()
                                                      .filter( t -> t.getCommaSeparatedValuesOrSource() instanceof ThresholdsConfig.Source )
                                                      .collect( Collectors.toSet() );

            // Iterate the external sources and read them all into the map
            for ( ThresholdsConfig next : external )
            {
                // Add or append
                ConfigHelper.addExternalThresholdsForOneMetricConfigGroup( projectConfig,
                                                                           returnMe,
                                                                           nextGroup,
                                                                           next,
                                                                           units );
            }

        }

        return Collections.unmodifiableMap( returnMe );
    }  
    
    /**
     * Mutates a map of thresholds, adding the thresholds for one metric configuration group.
     * 
     * @param projectConfig the project configuration
     * @param mutate the map of results to mutate
     * @param group The group of metrics to add the threshold to
     * @param units the optional units associated with the threshold values
     * @throws MetricConfigException if the metric configuration is invalid
     * @throws NullPointerException if any input is null
     */

    private static void
            addExternalThresholdsForOneMetricConfigGroup( ProjectConfig projectConfig,
                                                          Map<FeaturePlus, ThresholdsByMetric> mutate,
                                                          MetricsConfig group,
                                                          ThresholdsConfig thresholdsConfig,
                                                          Dimension units )
    {

        Objects.requireNonNull( mutate, "Specify a non-null map of thresholds to mutate." );

        Objects.requireNonNull( group, "Specify a non-null configuration group." );

        Objects.requireNonNull( group, "Specify non-null threshold configuration." );

        // Obtain the metrics
        Set<MetricConstants> metrics = ProjectConfigs.getMetricsFromMetricsConfig( group, projectConfig );

        // Obtain the thresholds
        Map<FeaturePlus, ThresholdsByMetric> thresholdsByFeature =
                ConfigHelper.readOneExternalThresholdFromProjectConfig( thresholdsConfig, metrics, units );

        // Iterate the thresholds
        for ( Entry<FeaturePlus, ThresholdsByMetric> nextEntry : thresholdsByFeature.entrySet() )
        {
            // Feature exists in the uber map: mutate it
            if ( mutate.containsKey( nextEntry.getKey() ) )
            {
                ThresholdsByMetric union = mutate.get( nextEntry.getKey() ).unionWithThisStore( nextEntry.getValue() );
                mutate.put( nextEntry.getKey(), union );
            }
            // New feature: add a new map
            else
            {
                mutate.put( nextEntry.getKey(), nextEntry.getValue() );
            }
        }
    }

    /**
     * Reads a {@link ThresholdsConfig} and returns a corresponding {@link Set} of external {@link Threshold}
     * by {@link FeaturePlus}.
     * 
     * @param threshold the threshold configuration
     * @param metrics the metrics to which the threshold applies
     * @param units the optional units associated with the threshold values
     * @return a map of thresholds by feature
     * @throws MetricConfigException if the threshold could not be read
     * @throws NullPointerException if the threshold configuration is null or the metrics are null
     */

    private static Map<FeaturePlus, ThresholdsByMetric>
            readOneExternalThresholdFromProjectConfig( ThresholdsConfig threshold,
                                                       Set<MetricConstants> metrics,
                                                       Dimension units )
    {

        Objects.requireNonNull( threshold, "Specify non-null threshold configuration." );

        Objects.requireNonNull( threshold, "Specify non-null metrics." );

        Map<FeaturePlus, ThresholdsByMetric> returnMe = new TreeMap<>();

        ThresholdsConfig.Source nextSource = (ThresholdsConfig.Source) threshold.getCommaSeparatedValuesOrSource();

        // Pre-validate path
        if ( Objects.isNull( nextSource.getValue() ) )
        {
            throw new MetricConfigException( threshold, "Specify a non-null path to read for the external "
                                                        + "source of thresholds." );
        }
        // Validate format
        if ( nextSource.getFormat() != ThresholdFormat.CSV )
        {
            throw new MetricConfigException( threshold,
                                             "Unsupported source format for thresholds '"
                                                        + nextSource.getFormat() + "'" );
        }

        // Missing value?
        Double missing = null;

        if ( Objects.nonNull( nextSource.getMissingValue() ) )
        {
            missing = Double.parseDouble( nextSource.getMissingValue() );
        }

        //Path
        Path commaSeparated = Paths.get( nextSource.getValue() );

        // Condition: default to greater
        ThresholdConstants.Operator operator = ThresholdConstants.Operator.GREATER;
        if ( Objects.nonNull( threshold.getOperator() ) )
        {
            operator = ProjectConfigs.getThresholdOperator( threshold );
        }

        // Data type: default to left
        ThresholdConstants.ThresholdDataType dataType = ThresholdConstants.ThresholdDataType.LEFT;
        if ( Objects.nonNull( threshold.getApplyTo() ) )
        {
            dataType = ProjectConfigs.getThresholdDataType( threshold );
        }
        
        // Threshold type: default to probability
        ThresholdConstants.ThresholdGroup thresholdType = ThresholdConstants.ThresholdGroup.PROBABILITY;
        if ( Objects.nonNull( threshold.getType() ) )
        {
            thresholdType = ProjectConfigs.getThresholdGroup( threshold );
        }

        // Default to probability
        boolean isProbability = thresholdType == ThresholdConstants.ThresholdGroup.PROBABILITY;

        try
        {
            Map<FeaturePlus, Set<Threshold>> read = CommaSeparatedReader.readThresholds( commaSeparated,
                                                                                         isProbability,
                                                                                         operator,
                                                                                         dataType,
                                                                                         missing,
                                                                                         units );

            // Add the thresholds for each feature
            for ( Entry<FeaturePlus, Set<Threshold>> nextEntry : read.entrySet() )
            {
                ThresholdsByMetricBuilder builder = DefaultDataFactory.getInstance().ofThresholdsByMetricBuilder();
                Map<MetricConstants, Set<Threshold>> thresholds = new EnumMap<>( MetricConstants.class );

                // Add the thresholds for each metric in the group
                metrics.forEach( nextMetric -> thresholds.put( nextMetric, nextEntry.getValue() ) );
                
                // Add to builder
                builder.addThresholds( thresholds, thresholdType );

                // Add to store
                returnMe.put( nextEntry.getKey(), builder.build() );
            }
        }
        catch ( IOException e )
        {
            throw new MetricConfigException( threshold,
                                             "Failed to read the comma separated thresholds "
                                                        + "from '" + commaSeparated + "'.",
                                             e );
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

        // The only incremental type currently supported is DestinationType.NETCDF
        if ( Objects.nonNull( output )
             && output.getDestination().stream().anyMatch( type -> type.getType() == DestinationType.NETCDF ) )
        {
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( DestinationType.NETCDF ) ) );
        }

        // Return empty set
        return Collections.emptySet();
    }

    /**
     * Returns a set of writers to be shared across instances of writing. Returns a {@link SharedWriters} that 
     * contains one writer for each supported incremental data format and type.
     *
     * @param projectIdentifier the unique project identifier
     * @param projectConfig the project configuration
     * @param featureCount the number of features
     * @param thresholdCount the number of thresholds
     * @param metrics the resolved DoubleScore metrics to write
     * @return a writer
     * @throws IOException if the project could not be validated or the writer could not be created
     * @throws ProjectConfigException if project configuration is invalid
     */

    public static NetcdfDoubleScoreWriter getNetcdfWriter( String projectIdentifier,
                                                           ProjectConfig projectConfig,
                                                           int featureCount,
                                                           int thresholdCount,
                                                           Set<MetricConstants> metrics )
            throws IOException
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        // Validate configuration
        WriterHelper.validateProjectForWriting( projectConfig );

        int basisTimes;
        int leadCount;

        try
        {
            leadCount = (int) Operations.getLeadCountsForProject( projectIdentifier );
        }
        catch ( SQLException se )
        {
            throw new IOException( "Unable to get lead counts.", se );
        }

        if ( leadCount > Integer.MAX_VALUE )
        {
            throw new IOException( "Cannot use more than "
                                   + Integer.MAX_VALUE
                                   + " lead times in a netCDF file." );
        }

        try
        {
            basisTimes = (int) Operations.getBasisTimeCountsForProject( projectIdentifier );
        }
        catch ( SQLException se )
        {
            throw new IOException( "Unable to get basis time counts.", se );
        }

        if ( basisTimes > Integer.MAX_VALUE )
        {
            throw new IOException( "Cannot use more than "
                                   + Integer.MAX_VALUE
                                   + " basis times in a netCDF file." );
        }

        return NetcdfDoubleScoreWriter.of( projectConfig,
                                           featureCount,
                                           basisTimes,
                                           leadCount,
                                           thresholdCount,
                                           metrics );
    }


    /**
     * Returns the lead time units associated with the input configuration. Returns {@link ChronoUnit#HOURS} if no
     * desired time scale is defined in the input configuration, otherwise the units of the desired time scale.
     * 
     * @param projectConfig the project configuration
     * @return the units associated with the forecast lead times
     * @throws NullPointerException if the input is null or the lead time units are null in the configuration
     * @throws IllegalArgumentException if the lead time units are not recognized
     */

    public static ChronoUnit getLeadTimeUnitsFromProjectConfig( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig, NULL_CONFIGURATION_ERROR );

        ChronoUnit returnMe = ChronoUnit.HOURS;

        if ( Objects.nonNull( projectConfig.getPair() )
             && Objects.nonNull( projectConfig.getPair().getDesiredTimeScale() ) )
        {
            returnMe = ChronoUnit.valueOf( projectConfig.getPair()
                                                        .getDesiredTimeScale()
                                                        .getUnit()
                                                        .toString()
                                                        .toUpperCase() );
        }

        return returnMe;
    }

}
