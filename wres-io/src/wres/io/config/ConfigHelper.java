package wres.io.config;

import static wres.config.generated.SourceTransformationType.PERSISTENCE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeWindowMode;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.Strings;
import wres.util.TimeHelper;

public class ConfigHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);

    private static final ConcurrentMap<ProjectConfig, ConcurrentSkipListSet<String>> messages
            = new ConcurrentHashMap<>();

    private ConfigHelper()
    {
        // prevent construction
    }

    // TODO: Move to Project Details
    public static boolean usesUSGSData(ProjectConfig projectConfig)
    {
        for ( DataSourceConfig.Source source : projectConfig.getInputs().getLeft().getSource())
        {
            if ( source.getFormat() == Format.USGS )
            {
                return true;
            }
        }

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getRight().getSource())
        {
            if ( source.getFormat() == Format.USGS )
            {
                return true;
            }
        }

        if (projectConfig.getInputs().getBaseline() != null)
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
    public static boolean usesNetCDFData(ProjectConfig projectConfig)
    {
        for ( DataSourceConfig.Source source : projectConfig.getInputs().getLeft().getSource())
        {
            if (source.getFormat() == Format.NET_CDF)
            {
                return true;
            }
        }

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getRight().getSource())
        {
            if (source.getFormat() == Format.NET_CDF)
            {
                return true;
            }
        }

        if (projectConfig.getInputs().getBaseline() != null)
        {
            for ( DataSourceConfig.Source source : projectConfig.getInputs()
                                                                .getBaseline()
                                                                .getSource() )
            {
                if (source.getFormat() == Format.NET_CDF)
                {
                    return true;
                }
            }
        }

        return false;
    }

    public static String getVariablePositionClause( Feature feature, int variableId, String alias)
            throws SQLException
    {
        StringBuilder clause = new StringBuilder();

        Integer variablePositionId = Features.getVariablePositionID( feature, variableId );

        if (variablePositionId != null)
        {
            if (Strings.hasValue( alias ))
            {
                clause.append(alias).append(".");
            }

            clause.append("variableposition_id = ").append(variablePositionId);
        }

        return clause.toString();
    }

    public static Comparator<Feature> getFeatureComparator()
    {
        return ( feature, t1 ) -> {

            String featureDescription = getFeatureDescription(feature);
            String t1Description = getFeatureDescription( t1 );
            try
            {
                if (Strings.hasValue(feature.getLocationId()) &&
                    Strings.hasValue(t1.getLocationId()))
                {
                    return feature.getLocationId().compareTo( t1.getLocationId() );
                }
                else
                {
                    LOGGER.trace("Either {} and {} have the same location ids or "
                                + "one or both of them don't have one.",
                                featureDescription,
                                t1Description);
                }
            }
            catch (Exception e)
            {
                LOGGER.error("Error occurred when comparing locations between '{}' "
                             + "and '{}'",
                             featureDescription,
                             t1Description);
                throw e;
            }

            try
            {
                if ( Strings.hasValue( feature.getHuc() ) &&
                     Strings.hasValue( t1.getHuc() ) )
                {
                    return feature.getHuc().compareTo( t1.getHuc() );
                }
                else
                {
                    LOGGER.trace("Either {} and {} have the same huc or "
                                + "one or both of them don't have one.",
                                featureDescription,
                                t1Description);
                }
            }
            catch (Exception e)
            {
                LOGGER.error("Error occurred when comparing hucs between '{}' "
                             + "and '{}'",
                             featureDescription,
                             t1Description);
                throw e;
            }

            try
            {
                if (Strings.hasValue( feature.getGageId() ) &&
                    Strings.hasValue(t1.getGageId()))
                {
                    return feature.getGageId().compareTo( t1.getGageId() );
                }
                else
                {
                    LOGGER.trace("Either {} and {} have the same gage ids or "
                                + "one or both of them don't have one.",
                                featureDescription,
                                t1Description);
                }
            }
            catch (Exception e)
            {
                LOGGER.error("Error occurred when comparing gages between '{}' "
                             + "and '{}'",
                             featureDescription,
                             t1Description);
                throw e;
            }

            try
            {
                if (feature.getComid() != null &&
                    t1.getComid() != null)
                {
                    return feature.getComid().compareTo( t1.getComid() );
                }
                else
                {
                    LOGGER.trace("Either {} and {} have the same com ids or "
                                + "one or both of them don't have one.",
                                featureDescription,
                                t1Description);
                }
            }
            catch (Exception e)
            {
                LOGGER.error("Error occurred when comparing comids between '{}' "
                             + "and '{}'",
                             featureDescription,
                             t1Description);
                throw e;
            }

            LOGGER.info("A proper comparison couldn't be made between {} and {}."
                        + " Now saying that {} is greater than {}.",
                        featureDescription,
                        t1Description,
                        featureDescription,
                        t1Description);
            return 1;
        };
    }

    public static int getValueCount(ProjectDetails projectDetails,
                                    DataSourceConfig dataSourceConfig,
                                    Feature feature) throws SQLException
    {
        final String NEWLINE = System.lineSeparator();
        Integer variableId;
        String member;

        if (projectDetails.getRight().equals( dataSourceConfig ))
        {
            variableId = projectDetails.getRightVariableID();
            member = ProjectDetails.RIGHT_MEMBER;
        }
        else if (projectDetails.getLeft().equals( dataSourceConfig ))
        {
            variableId = projectDetails.getLeftVariableID();
            member = ProjectDetails.LEFT_MEMBER;
        }
        else
        {
            variableId = projectDetails.getBaselineVariableID();
            member = ProjectDetails.BASELINE_MEMBER;
        }

        String variablePositionClause = ConfigHelper.getVariablePositionClause( feature, variableId, "");

        StringBuilder script = new StringBuilder("SELECT COUNT(*)::int").append(NEWLINE);

        if (ConfigHelper.isForecast( dataSourceConfig ))
        {
            script.append("FROM wres.TimeSeries TS").append(NEWLINE);
            script.append("INNER JOIN wres.ForecastValue FV").append(NEWLINE);
            script.append("    ON TS.timeseries_id = FV.timeseries_id").append(NEWLINE);
            script.append("WHERE ").append(variablePositionClause).append(NEWLINE);
            script.append("    EXISTS (").append(NEWLINE);
            script.append("        SELECT 1").append(NEWLINE);
            script.append("        FROM wres.ForecastSource FS").append(NEWLINE);
            script.append("        INNER JOIN wres.ProjectSource PS").append(NEWLINE);
            script.append("            ON FS.source_id = PS.source_id").append(NEWLINE);
            script.append("        WHERE PS.project_id = ").append(projectDetails.getId()).append(NEWLINE);
            script.append("            AND PS.member = ").append(member).append(NEWLINE);
            script.append("            AND PS.inactive_time IS NULL").append(NEWLINE);
            script.append("            AND FS.forecast_id = TS.timeseries_id").append(NEWLINE);
            script.append("    );");
        }
        else
        {
            script.append("FROM wres.Observation O").append(NEWLINE);
            script.append("WHERE ").append(variablePositionClause).append(NEWLINE);
            script.append("    AND EXISTS (").append(NEWLINE);
            script.append("        SELECT 1").append(NEWLINE);
            script.append("        FROM wres.ProjectSource PS").append(NEWLINE);
            script.append("        WHERE PS.project_id = ").append(projectDetails.getId()).append(NEWLINE);
            script.append("            AND PS.member = ").append(member).append(NEWLINE);
            script.append("            AND PS.source_id = O.source_id").append(NEWLINE);
            script.append("            AND PS.inactive_time IS NULL").append(NEWLINE);
            script.append("    );");
        }

        return Database.getResult( script.toString(), "count" );
    }

    public static boolean isForecast(DataSourceConfig dataSource)
    {
        return dataSource != null &&
                Strings.isOneOf(dataSource.getType().value(),
                                DatasourceType.SINGLE_VALUED_FORECASTS.value(),
                                DatasourceType.ENSEMBLE_FORECASTS.value());
    }

    public static boolean isSimulation(DataSourceConfig dataSourceConfig)
    {
        return dataSourceConfig != null
                && dataSourceConfig.getType() == DatasourceType.SIMULATIONS;
    }

    public static ProjectConfig read(final String path) throws IOException
    {
        Path actualPath = Paths.get( path );
        ProjectConfigPlus configPlus = ProjectConfigPlus.from( actualPath );
        return configPlus.getProjectConfig();
    }

    public static DataSourceConfig.Source findDataSourceByFilename(DataSourceConfig dataSourceConfig, String filename)
    {
        DataSourceConfig.Source source = null;
        filename = Paths.get(filename).toAbsolutePath().toString();
        String sourcePath = "";

        for (DataSourceConfig.Source dataSource : dataSourceConfig.getSource())
        {
            String fullDataSourcePath = Paths.get(dataSource.getValue()).toAbsolutePath().toString();

            if (filename.startsWith(fullDataSourcePath) && fullDataSourcePath.length() > sourcePath.length())
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
    public static Instant getEarliestDateTimeFromDataSources(ProjectConfig config)
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
            if (LOGGER.isInfoEnabled() && ConfigHelper.messageSendPutIfAbsent(config, messageId))
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
    public static Instant getLatestDateTimeFromDataSources(ProjectConfig config)
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
            if (LOGGER.isInfoEnabled() && ConfigHelper.messageSendPutIfAbsent(config, messageId))
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
    private static boolean messageSendPutIfAbsent(ProjectConfig projectConfig,
                                                      String message)
    {
        // In case we are the first to call regarding a given config:
        ConcurrentSkipListSet<String> possiblyNewSet = new ConcurrentSkipListSet<>();
        possiblyNewSet.add(message);

        ConcurrentSkipListSet<String> theSet = messages.putIfAbsent(projectConfig,
                                                                    possiblyNewSet);
        if (theSet == null)
        {
            // this call was first to put a set for this config, return true.
            return true;
        }
        // this call was not the first to put a set for this config.
        return theSet.add(message);
    }

    // TODO: Should this move to wres.io.data.caching.Features?
    public static String getFeatureDescription(Feature feature)
    {
        String description = null;

        if ( feature != null )
        {
            if (Strings.hasValue( feature.getName() ))
            {
                description = feature.getName();
            }
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
        }

        return description;
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
        StringJoiner result = new StringJoiner( ", ", "( ", " )");

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
            throws ProjectConfigException
    {
        Path outputDirectory = Paths.get( d.getPath() );

        if ( outputDirectory == null )
        {
            String message = "Destination path " + d.getPath() +
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
                             outputDirectory);

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
     * @param lead the earliest and latest lead time
     * @param sequenceStep the position of the window within a sequence
     * @return a time window 
     * @throws NullPointerException if the config is null
     * @throws DateTimeParseException if the configuration contains dates that cannot be parsed
     */
    public static TimeWindow getTimeWindow( ProjectDetails projectDetails, long lead, int sequenceStep)
    {
        Objects.requireNonNull( projectDetails );
        TimeWindow windowMetadata;

        Duration beginningLead;
        Duration endingLead = Duration.ofHours( lead );

        if (projectDetails.getProjectConfig().getPair().getLeadTimesPoolingWindow() != null)
        {
            PoolingWindowConfig leadPoolingWindow = projectDetails.getProjectConfig().getPair().getLeadTimesPoolingWindow();
            /*long leadPoolPeriod = TimeHelper.unitsToLeadUnits(
                    leadPoolingWindow.getUnit().value(),
                    leadPoolingWindow.getPeriod()
            );*/

            beginningLead = endingLead.minus( leadPoolingWindow.getPeriod(), ChronoUnit.valueOf( leadPoolingWindow.getUnit().toString().toUpperCase() ) );
            //beginningLead = Duration.of( lead - leadPoolPeriod, ChronoUnit.HOURS );
        }
        else
        {
            beginningLead = endingLead;
        }

        if ( projectDetails.getPoolingMode() == TimeWindowMode.ROLLING )
        {
            long frequencyOffset = TimeHelper.unitsToLeadUnits( projectDetails.getIssuePoolingWindowUnit(),
                                                                  projectDetails.getIssuePoolingWindowFrequency() )
                                     * sequenceStep;

            Instant first = Instant.parse( projectDetails.getEarliestIssueDate() );
            first = first.plus( frequencyOffset, ChronoUnit.HOURS );
            Instant second;

            if (projectDetails.getIssuePoolingWindowPeriod() > 0)
            {
                second = first.plus( projectDetails.getIssuePoolingWindowPeriod(),
                                     ChronoUnit.valueOf( projectDetails.getIssuePoolingWindowUnit().toUpperCase() ));
            }
            else
            {
                second = first;
            }

            windowMetadata = TimeWindow.of( first,
                                            second,
                                            ReferenceTime.ISSUE_TIME,
                                            beginningLead,
                                            endingLead
                                            //Duration.ofHours( lead ),
                                            //Duration.ofHours( lead )
                                            );
        }
        //Valid dates available
        else if ( projectDetails.getEarliestDate() != null && projectDetails.getLatestDate() != null)
        {
            windowMetadata = TimeWindow.of( Instant.parse( projectDetails.getEarliestDate() ),
                                  Instant.parse( projectDetails.getLatestDate() ),
                                  ReferenceTime.VALID_TIME,
                                            beginningLead,
                                            endingLead
                                            //Duration.ofHours( lead ),
                                            //Duration.ofHours( lead )
            );
        }
        //Issue dates available
        else if ( projectDetails.getEarliestIssueDate() != null && projectDetails.getLatestIssueDate() != null )
        {
            return TimeWindow.of( Instant.parse( projectDetails.getEarliestIssueDate() ),
                                  Instant.parse( projectDetails.getLatestIssueDate() ),
                                  ReferenceTime.ISSUE_TIME,
                                  beginningLead,
                                  endingLead
                                  //Duration.ofHours( lead ),
                                  //Duration.ofHours( lead )
            );
        }
        //No dates available
        else
        {
            //Duration leadTime = Duration.ofHours( lead );
            return TimeWindow.of( Instant.MIN,
                                  Instant.MAX,
                                  ReferenceTime.VALID_TIME,
                                  beginningLead,
                                  endingLead
                                  //Duration.ofHours( lead ),
                                  //Duration.ofHours( lead )
            );
        }

        return windowMetadata;
    }

    /**
     * Get the time zone offset from a datasource config or null if not found.
     * @param sourceConfig the configuration element to retrieve for
     * @return the time zone offset or null if not specified in dataSourceConfig
     * @throws ProjectConfigException when the date time could not be parsed
     */

    public static ZoneOffset getZoneOffset( DataSourceConfig.Source sourceConfig )
            throws ProjectConfigException
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
        UTC ( "+0000" ),
        GMT ( "+0000" ),
        EDT ( "-0400" ),
        EST ( "-0500" ),
        CDT ( "-0500" ),
        CST ( "-0600" ),
        MDT ( "-0600" ),
        MST ( "-0700" ),
        PDT ( "-0700" ),
        PST ( "-0800" ),
        AKDT ( "-0800" ),
        AKST ( "-0900" ),
        HADT ( "-0900" ),
        HAST ( "-1000" );

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
                                             Integer timeShift)
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

            s.append( " + ");

            s.append( ConfigHelper.getExtractSqlSnippet( "day",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " >= " );

            s.append( earliest.getMonthValue() * MONTH_MULTIPLIER
                      + earliest.getDayOfMonth() );

            if ( earliest.isAfter( latest ) )
            {
                s.append( " OR ");
            }
            else
            {
                s.append( " AND ");
            }

            s.append( MONTH_MULTIPLIER );
            s.append( " * " );

            s.append( ConfigHelper.getExtractSqlSnippet( "month",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " + ");

            s.append( ConfigHelper.getExtractSqlSnippet( "day",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " <= ");

            s.append( latest.getMonthValue() * MONTH_MULTIPLIER
                      + latest.getDayOfMonth() );

            s.append( " )" );
            s.append(System.lineSeparator());
        }

        LOGGER.trace( "{}", s );

        return s.toString();
    }

    private static String getExtractSqlSnippet( String toExtract,
                                                String databaseColumnName,
                                                Integer timeShift )
    {
        StringBuilder s = new StringBuilder();

        s.append( "EXTRACT( ");
        s.append( toExtract );
        s.append( " from " );
        s.append( databaseColumnName );

        if ( timeShift != null )
        {
            s.append(" + INTERVAL '1 HOUR' * ");
            s.append( timeShift );
        }

        s.append( " )");

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
                                                + " source config " + config );
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
        for ( MetricConfig next : projectConfig.getMetrics().getMetric() )
        {
            // Match
            if ( next.getName().equals( metricName ) )
            {
                return next;
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
}
