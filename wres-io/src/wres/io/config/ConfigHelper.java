package wres.io.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
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
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeAggregationConfig;
import wres.config.generated.TimeAggregationFunction;
import wres.config.generated.TimeAggregationMode;
import wres.io.data.caching.Features;
import wres.io.data.caching.Projects;
import wres.io.data.caching.Variables;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.Strings;
import wres.util.Time;

public class ConfigHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);

    private static final ConcurrentMap<ProjectConfig, ConcurrentSkipListSet<String>> messages
            = new ConcurrentHashMap<>();

    private ConfigHelper()
    {
        // prevent construction
    }

    /**
     * Given a config, generate feature IDs and return a sql string of them.
     *
     * @param config the project config
     * @return sql string useful in a where clause
     * @throws IOException if the feature ID could not be retrieved or added
     */
    public static String getFeatureIdsAndPutIfAbsent(ProjectConfig config)
    throws IOException
    {
        if ( config.getPair() == null
             || config.getPair().getFeature() == null )
        {
            return "";
        }

        StringJoiner result = new StringJoiner(",", "feature_id in (", ")");

        try
        {
            // build a sql string of feature_ids, using cache to populate as needed
            for ( Feature feature : Collections.where(config.getPair().getFeature(), feature -> {
                return feature.getLid() != null && !feature.getLid().isEmpty();
            } ) )
            {
                Integer i = Features.getFeatureID( feature.getLid(),
                                                   feature.getName() );
                result.add(Integer.toString(i));
            }
        }
        catch ( SQLException e )
        {
            throw new IOException("Failed to get or put a feature id.", e);
        }

        return result.toString();
    }

    public static boolean usesProbabilityThresholds(final ProjectConfig projectConfig)
    {
        boolean hasProbabilityThreshold = projectConfig.getOutputs().getProbabilityThresholds() != null;

        if (!hasProbabilityThreshold)
        {
            hasProbabilityThreshold = Collections.exists(projectConfig.getOutputs().getMetric(), (MetricConfig config) -> {
                return config.getProbabilityThresholds() != null;
            });
        }

        return hasProbabilityThreshold;
    }

    /**
     *
     * @param projectConfig
     * @param currentLead
     * @return
     * @throws InvalidPropertiesFormatException Thrown if the time aggregation unit is not supported
     */
    public static int getLead(ProjectConfig projectConfig, int currentLead) throws InvalidPropertiesFormatException {
        TimeAggregationConfig timeAggregationConfig = ConfigHelper.getTimeAggregation( projectConfig );
        return Time.unitsToHours(timeAggregationConfig.getUnit().name(),
                                 currentLead * timeAggregationConfig.getPeriod()).intValue();
    }

    public static Integer getVariableID(DataSourceConfig dataSourceConfig) throws SQLException
    {
        return Variables.getVariableID(dataSourceConfig.getVariable().getValue(),
                                       dataSourceConfig.getVariable().getUnit());
    }

    public static Boolean hasBaseline(ProjectConfig projectConfig)
    {
        return projectConfig.getInputs().getBaseline() != null &&
                !projectConfig.getInputs().getBaseline().getSource().isEmpty();
    }

    public static TimeAggregationConfig getTimeAggregation(ProjectConfig projectConfig)
    {
        TimeAggregationConfig timeAggregationConfig = projectConfig.getPair().getDesiredTimeAggregation();

        if (timeAggregationConfig == null)
        {
            timeAggregationConfig = new TimeAggregationConfig( TimeAggregationFunction.AVG,
                                                               1,
                                                               null,
                                                               DurationUnit.HOUR,
                                                               "",
                                                               TimeAggregationMode.BACK_TO_BACK);
        }

        return timeAggregationConfig;
    }

    /**
     *
     * @param projectConfig The configuration that controls how windows are calculated
     * @param windowNumber The indicator of the window whose lead description needs.  In the simplest case, the first
     *                     window could represent 'lead = 1' while the third 'lead = 3'. In more complicated cases,
     *                     the first window could be '40 &gt; lead AND lead &ge; 1' and the second '80 &gt; lead AND lead &ge; 40'
     * @return A description of what a window number means in terms of lead times
     * @throws InvalidPropertiesFormatException Thrown if the time aggregation unit is not supported
     */
    public static String getLeadQualifier(ProjectConfig projectConfig,
                                          int windowNumber,
                                          int offset)
            throws InvalidPropertiesFormatException
    {
        String qualifier;

        TimeAggregationConfig timeAggregationConfig = ConfigHelper.getTimeAggregation( projectConfig );

        if (!(timeAggregationConfig.getPeriod() == 1 && timeAggregationConfig.getUnit() == DurationUnit.HOUR)) {

            // We perform -1 on beginning and not +1 on end because this is 1s indexed, which throws us off
            int beginning = getLead( projectConfig, windowNumber );
            int end = getLead( projectConfig, windowNumber + 1 );

            qualifier = String.valueOf(end + offset);
            qualifier += " >= lead AND lead > ";
            qualifier += String.valueOf(beginning + offset);
        }
        else
        {
            // We add the plus one because the value yielded by
            // getLead(projectConfig, windowNumber) grants us the first exclusive
            // value, not the first inclusive value
            qualifier = "lead = " + (getLead(projectConfig, windowNumber) + 1);
        }

        return qualifier;
    }

    public static String getVariablePositionClause( Feature feature, int variableId, String alias)
    {
        StringBuilder clause = new StringBuilder();

        if ( feature.getLid() != null )
        {
            try
            {
                // TODO: This only works when a) a location is specified and b) an lid is specified
                // TODO: This needs to work with all other identifiers
                Integer variablePositionId
                        = Features.getVariablePositionID( feature.getLid(),
                                                          feature.getName(),
                                                          variableId );

                if (variablePositionId != null)
                {
                    if (Strings.hasValue( alias ))
                    {
                        clause.append(alias).append(".");
                    }

                    clause.append("variableposition_id = ").append(variablePositionId);
                }
            }
            catch (Exception e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }

        return clause.toString();
    }

    public static Double getWindowWidth( ProjectConfig projectConfig )
            throws InvalidPropertiesFormatException
    {
        TimeAggregationConfig timeAggregationConfig = projectConfig.getPair().getDesiredTimeAggregation();
        return Time.unitsToHours( timeAggregationConfig.getUnit().value(),
                                  timeAggregationConfig.getPeriod() );
    }

    public static Integer getLeadOffset( ProjectConfig projectConfig, Feature feature )
            throws InvalidPropertiesFormatException, SQLException
    {
        Integer offset;
        String newline = System.lineSeparator();
        int width = getWindowWidth( projectConfig ).intValue();

        ProjectDetails projectDetails = Projects.getProject( projectConfig );

        String leftVariablepositionClause =
                ConfigHelper.getVariablePositionClause( feature,
                                                        projectDetails.getLeftVariableID(),
                                                        "O" );
        String rightVariablepositionClause =
                ConfigHelper.getVariablePositionClause( feature,
                                                        projectDetails.getRightVariableID(),
                                                        "TS" );

        StringBuilder script = new StringBuilder(  );

        script.append("SELECT FV.lead - ").append(width).append(" AS offset").append(newline);
        script.append("FROM wres.TimeSeries TS").append(newline);
        script.append("INNER JOIN wres.ForecastValue FV").append(newline);
        script.append("     ON FV.timeseries_id = TS.timeseries_id").append(newline);
        script.append("INNER JOIN wres.Observation O").append(newline);
        script.append("     ON O.observation_time");

        if (projectDetails.getLeftTimeShift() != 0)
        {
            script.append(" + INTERVAL '1 HOUR' *").append(projectDetails.getLeft().getTimeShift().getWidth());
        }

        script.append(" = TS.initialization_date + INTERVAL '1 HOUR' * (FV.lead + ").append(width).append(")");

        if (projectDetails.getRightTimeShift() != 0)
        {
            script.append(" + INTERVAL '1 HOUR' *").append(projectDetails.getRight().getTimeShift().getWidth());
        }

        script.append(newline);
        script.append("WHERE ").append(leftVariablepositionClause).append(newline);
        script.append("     AND ").append(rightVariablepositionClause).append(newline);

        if (width > 1)
        {
            script.append( "     AND FV.lead - " )
                  .append( width )
                  .append( " >= 0" )
                  .append( newline );
        }

        if ( ConfigHelper.isMinimumLeadHourSpecified( projectConfig ) )
        {
            script.append( "     AND FV.lead >= " )
                  .append( ConfigHelper.getMinimumLeadHour( projectConfig ) )
                  .append( newline );
        }

        if ( ConfigHelper.isMaximumLeadHourSpecified( projectConfig ))
        {
            script.append( "     AND FV.lead <= " )
                  .append( ConfigHelper.getMaximumLeadHour( projectConfig ) )
                  .append( newline );
        }

        if ( projectConfig.getPair()
                          .getDates() != null )
        {
            if ( projectConfig.getPair()
                              .getDates()
                              .getEarliest() != null
                 && !projectConfig.getPair()
                                  .getDates()
                                  .getEarliest()
                                  .trim()
                                  .isEmpty() )
            {
                script.append("     AND TS.initialization_date >= '")
                      .append( projectConfig.getPair()
                                            .getDates()
                                            .getEarliest() )
                      .append("'")
                      .append(newline);
                script.append("     AND O.observation_time >= '")
                      .append( projectConfig.getPair()
                                            .getDates()
                                            .getEarliest() )
                      .append("'")
                      .append(newline);
            }

            if ( projectConfig.getPair()
                              .getDates()
                              .getLatest() != null
                 && !projectConfig.getPair()
                                  .getDates()
                                  .getLatest()
                                  .trim()
                                  .isEmpty() )
            {
                script.append("     AND TS.initialization_date <= '")
                      .append( projectConfig.getPair()
                                            .getDates()
                                            .getLatest() )
                      .append("'")
                      .append(newline);
                script.append("     AND O.observation_time <= '")
                      .append( projectConfig.getPair().getDates().getLatest())
                      .append("'")
                      .append(newline);
            }
        }

        // Filtering on existence guarentees early exit
        script.append("     AND EXISTS (").append(newline);
        script.append("         SELECT 1").append(newline);
        script.append("         FROM wres.ProjectSource OPS").append(newline);
        script.append("         INNER JOIN wres.ForecastSource OFS").append(newline);
        script.append("             ON OFS.source_id = OPS.source_id").append(newline);
        script.append("         WHERE OPS.project_id = ").append(projectDetails.getId()).append(newline);
        script.append("             AND OPS.member = 'right'").append(newline);
        script.append("             AND OFS.forecast_id = TS.timeseries_id").append(newline);
        script.append("     )").append(newline);
        script.append("     AND EXISTS (").append(newline);
        script.append("         SELECT 1").append(newline);
        script.append("         FROM wres.ProjectSource OPS").append(newline);
        script.append("         WHERE OPS.project_id = ").append(projectDetails.getId()).append(newline);
        script.append("             AND OPS.member =  'left'").append(newline);
        script.append("             AND OPS.source_id = O.source_id").append(newline);
        script.append("     )").append(newline);

        script.append("ORDER BY FV.lead").append(newline);
        script.append("LIMIT 1;");

        offset = Database.getResult(script.toString(), "offset");

        return offset;
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

    public static boolean isLeft(DataSourceConfig dataSourceConfig, ProjectConfig projectConfig)
    {
        return projectConfig.getInputs().getLeft().equals( dataSourceConfig );
    }

    public static boolean isRight(DataSourceConfig dataSourceConfig, ProjectConfig projectConfig)
    {
        return projectConfig.getInputs().getRight().equals( dataSourceConfig );
    }

    public static boolean isBaseline(DataSourceConfig dataSourceConfig, ProjectConfig projectConfig)
    {
        return projectConfig.getInputs().getBaseline() != null &&
               projectConfig.getInputs().getBaseline().equals( dataSourceConfig );
    }

    /**
     * Returns the "earliest" datetime from given ProjectConfig Conditions
     * @param config the project configuration
     * @return the most narrow "earliest" date, null otherwise
     */
    public static LocalDateTime getEarliestDateTimeFromDataSources(ProjectConfig config)
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
                return LocalDateTime.parse( earliest );
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
                                 + "\"2017-06-27T16:16\"",
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
                LOGGER.info( "No \"earliest\" date found in project. Use <dates earliest=\"2017-06-27T16:14\" latest=\"2017-07-06T11:35\" /> under <pair> (near line {} column {} of project file) to specify an earliest date.",
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
     * @param config the project configuration
     * @return the most narrow "latest" date, null otherwise.
     */
    public static LocalDateTime getLatestDateTimeFromDataSources(ProjectConfig config)
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
                return LocalDateTime.parse( latest );
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
                                 + "\"2017-06-27T16:16\"",
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
                LOGGER.info( "No \"latest\" date found in project. Use <dates earliest=\"2017-06-27T16:14\" latest=\"2017-07-06T11:35\" />  under <pair> (near line {} col {} of project file) to specify a latest date.",
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

    public static String getFeatureDescription(Feature feature)
    {
        String description = null;

        if ( feature != null )
        {
            if ( feature.getLid() != null
                 && !feature.getLid()
                            .trim()
                            .isEmpty() )
            {
                description = feature.getLid();
            }
            else if ( feature.getHuc() != null
                      && !feature.getHuc()
                                 .trim()
                                 .isEmpty() )
            {
                description = feature.getHuc();
            }
            else if ( feature.getComid() != null )
            {
                description = String.valueOf( feature.getComid() );
            }
            else if ( feature.getGageId() != null
                      && !feature.getGageId()
                                 .trim()
                                 .isEmpty() )
            {
                description = feature.getGageId();
            }
        }

        return description;
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
     * Get all the pair destinations from a configuration.
     *
     * @param config the config to search through
     * @return a list of pair destinations
     * @throws NullPointerException when config is null
     */

    public static List<DestinationConfig> getPairDestinations( ProjectConfig config )
    {
        return getDestinationsOfType( config, DestinationType.PAIRS );
    }


    /**
     * Get whether the minimum lead hour from a project config was specified.
     * @param config the config to use
     * @return true if the config specified a lead hour, false otherwise
     */

    public static boolean isMaximumLeadHourSpecified( ProjectConfig config )
    {
        return config.getPair() != null
               && config.getPair()
                        .getLeadHours() != null
               && config.getPair()
                        .getLeadHours()
                        .getMaximum() != null;
    }


    /**
     * Get the maximum lead hours from a project config or a default.
     * @param config the config to use
     * @return the maximum value specified or a default of Integer.MAX_VALUE
     */

    public static int getMaximumLeadHour( ProjectConfig config )
    {
        int result = Integer.MAX_VALUE;

        if ( config.getPair() != null
             && config.getPair()
                      .getLeadHours() != null
             && config.getPair()
                      .getLeadHours()
                      .getMaximum() != null )
        {
            result = config.getPair()
                           .getLeadHours()
                           .getMaximum();
        }

        return result;
    }


    /**
     * Get whether the minimum lead hour from a project config was specified.
     * @param config the config to use
     * @return true if the config specified a lead hour, false otherwise
     */

    public static boolean isMinimumLeadHourSpecified( ProjectConfig config )
    {
        return config.getPair() != null
               && config.getPair()
                        .getLeadHours() != null
               && config.getPair()
                        .getLeadHours()
                        .getMinimum() != null;
    }


    /**
     * Get the minimum lead hours from a project config or a default.
     * @param config the config to use
     * @return the minimum value specified or a default of Integer.MIN_VALUE
     */

    public static int getMinimumLeadHour( ProjectConfig config )
    {
        int result = Integer.MIN_VALUE;

        if ( config.getPair() != null
             && config.getPair()
                      .getLeadHours() != null
             && config.getPair()
                      .getLeadHours()
                      .getMinimum() != null )
        {
            result = config.getPair()
                           .getLeadHours()
                           .getMinimum();
        }

        return result;
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

        private final ZoneOffset zoneOffset;

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
     * @param projectConfig the configuration, non-null
     * @param databaseColumnName the column name with a date or time to use
     * @param timeShift the amount of time to shift, null otherwise
     * @return a where clause sql snippet or empty string if no season
     * @throws NullPointerException when projectConfig or databaseColumnName is null
     * @throws DateTimeException when the values in the season are invalid
     */

    public static String getSeasonQualifier( ProjectConfig projectConfig,
                                             String databaseColumnName,
                                             Integer timeShift)
    {
        Objects.requireNonNull( projectConfig, "projectConfig needs to exist" );
        Objects.requireNonNull( databaseColumnName, "databaseColumnName needs to exist" );
        StringBuilder s = new StringBuilder();
        PairConfig.Season season = projectConfig.getPair()
                                                .getSeason();
        if ( season != null )
        {
            MonthDay earliest = MonthDay.of( season.getEarliestMonth(),
                                             season.getEarliestDay() );
            MonthDay latest = MonthDay.of( season.getLatestMonth(),
                                           season.getLatestMonth() );

            s.append( "     AND ( " );
            s.append( ConfigHelper.getExtractSqlSnippet( "day",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " >= ");
            s.append( season.getEarliestDay() );

            if ( earliest.isAfter( latest ) )
            {
                s.append( " OR ");
            }
            else
            {
                s.append( " AND ");
            }

            s.append( ConfigHelper.getExtractSqlSnippet( "day",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " <= " );
            s.append( season.getLatestDay() );
            s.append( " )" );

            s.append( System.lineSeparator() );

            s.append( "     AND ( ");
            s.append( ConfigHelper.getExtractSqlSnippet( "month",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( "  >= " );
            s.append( season.getEarliestMonth() );

            if ( earliest.isAfter( latest ) )
            {
                s.append( " OR ");
            }
            else
            {
                s.append( " AND ");
            }

            s.append( ConfigHelper.getExtractSqlSnippet( "month",
                                                         databaseColumnName,
                                                         timeShift ) );

            s.append( " <= " );
            s.append( season.getLatestMonth() );
            s.append( " ) " );
        }

        LOGGER.trace( s.toString() );
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
}
