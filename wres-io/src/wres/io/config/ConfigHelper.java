package wres.io.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Objects;
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
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.MetricConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.RollingWindowFocus;
import wres.config.generated.TimeAggregationConfig;
import wres.config.generated.TimeAggregationFunction;
import wres.config.generated.TimeAggregationMode;
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
            timeAggregationConfig = new TimeAggregationConfig(
                    TimeAggregationFunction.AVG,
                    1,
                    1,
                    1,
                    DurationUnit.HOUR,
                    "",
                    TimeAggregationMode.BACK_TO_BACK,
                    RollingWindowFocus.CENTER
            );
        }

        return timeAggregationConfig;
    }

    /**
     *
     * @param projectDetails The configuration that controls how windows are calculated
     * @param windowNumber The indicator of the window whose lead description needs.  In the simplest case, the first
     *                     window could represent 'lead = 1' while the third 'lead = 3'. In more complicated cases,
     *                     the first window could be '40 &gt; lead AND lead &ge; 1' and the second '80 &gt; lead AND lead &ge; 40'
     * @param offset The offset                    
     * @return A description of what a window number means in terms of lead times
     * @throws InvalidPropertiesFormatException Thrown if the time aggregation unit is not supported
     */
    public static String getLeadQualifier(ProjectDetails projectDetails,
                                          int windowNumber,
                                          int offset)
            throws InvalidPropertiesFormatException
    {
        String qualifier;

        if (!(projectDetails.getAggregationPeriod() == 1 &&
              projectDetails.getAggregationUnit().equalsIgnoreCase( DurationUnit.HOUR.toString() )))
        {
            int beginning = projectDetails.getLead( windowNumber );
            int end = projectDetails.getLead( windowNumber + 1 );

            qualifier = String.valueOf(end + offset);
            qualifier += " >= FV.lead AND FV.lead > ";
            qualifier += String.valueOf(beginning + offset);
        }
        else
        {
            // We add the plus one because the value yielded by
            // getLead(projectDetails, windowNumber) grants us the first exclusive
            // value, not the first inclusive value
            qualifier = "FV.lead = " + ( projectDetails.getLead(windowNumber) + 1);
        }

        return qualifier;
    }

    public static String getVariablePositionClause( Feature feature, int variableId, String alias)
    {
        StringBuilder clause = new StringBuilder();

        try
        {
            Integer variablePositionId = Features.getVariablePositionID( feature, variableId );

            if (variablePositionId != null)
            {
                if (Strings.hasValue( alias ))
                {
                    clause.append(alias).append(".");
                }

                clause.append("variableposition_id = ").append(variablePositionId);
            }
        }
        catch (Exception e)
        {
            LOGGER.error(Strings.getStackTrace(e));
        }

        return clause.toString();
    }

    public static Double getWindowWidth( ProjectConfig projectConfig )
            throws InvalidPropertiesFormatException
    {
        TimeAggregationConfig timeAggregationConfig = projectConfig.getPair().getDesiredTimeAggregation();
        return TimeHelper.unitsToHours( timeAggregationConfig.getUnit().value(),
                                        timeAggregationConfig.getPeriod() );
    }

    public static Integer getLeadOffset( ProjectDetails projectDetails, Feature feature )
            throws InvalidPropertiesFormatException, SQLException
    {
        ProjectConfig projectConfig = projectDetails.getProjectConfig();
        Integer offset;
        String newline = System.lineSeparator();
        int width = getWindowWidth( projectConfig ).intValue();

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
     * @param feature the feature
     * @return a time window 
     * @throws NullPointerException if the config is null
     * @throws DateTimeParseException if the configuration contains dates that cannot be parsed
     * @throws InvalidPropertiesFormatException if dates could not be established
     * @throws SQLException if the anchor date for the rolling windows could not be established
     */

    public static TimeWindow getTimeWindow( ProjectDetails projectDetails, long lead, int sequenceStep, Feature feature)
            throws InvalidPropertiesFormatException, SQLException
    {
        Objects.requireNonNull( projectDetails );
        TimeWindow windowMetadata;

        if (projectDetails.getAggregation().getMode() == TimeAggregationMode.ROLLING)
        {
            // Defines logic for left focused, center focused, and right focused
            // rolling windows. Initially, the differences between the three will
            // be minute, but those differences will grow once different variables
            // used by other projects are introduced.

            // Determine how many hours into the sequence this window is
            double frequencyOffset = TimeHelper.unitsToHours( projectDetails.getAggregationUnit(),
                                                              projectDetails.getAggregation().getFrequency()) *
                                     sequenceStep;

            // Get the first date that matching data for a feature is valid
            String focusDate = projectDetails.getInitialRollingDate( feature );

            // Add the lead time to the focus date to get to where this set of
            // sequences really starts
            focusDate = TimeHelper.plus( focusDate, projectDetails.getAggregationUnit(), lead );

            // Add the frequency offset to focus date to jump to the correct
            // sequence
            focusDate = TimeHelper.plus( focusDate, "hours", frequencyOffset );

            String firstDate;
            String lastDate;

            if (projectDetails.getAggregation().getFocus() == RollingWindowFocus.CENTER)
            {
                // Since the focus is in the center of the window, our first
                // date is half the span before the focus and our last date
                // is half the span after
                double halfSpan = projectDetails.getAggregation().getSpan() / 2.0;
                firstDate = TimeHelper.minus( focusDate,
                                              projectDetails.getAggregationUnit(),
                                              halfSpan );
                lastDate = TimeHelper.plus( focusDate,
                                            projectDetails.getAggregationUnit(),
                                            halfSpan);
            }
            else if (projectDetails.getAggregation().getFocus() == RollingWindowFocus.LEFT)
            {
                // Since the focus is on the left of the window, our first date
                // is actually the focus, while the last date is the entire
                // span after it
                firstDate = focusDate;
                lastDate = TimeHelper.plus( focusDate,
                                            projectDetails.getAggregationUnit(),
                                            projectDetails.getAggregation().getSpan());
            }
            else
            {
                // Since the focus is on the right of the window, our first date
                // is the entire span prior to the focus and the last date is
                // the focus itself
                firstDate = TimeHelper.minus( focusDate,
                                              projectDetails.getAggregationUnit(),
                                              projectDetails.getAggregation().getSpan() );
                lastDate = focusDate;
            }

            OffsetDateTime first = TimeHelper.convertStringToDate( firstDate );
            OffsetDateTime last = TimeHelper.convertStringToDate( lastDate );

            windowMetadata = TimeWindow.of( first.toInstant(),
                                            last.toInstant(),
                                            ReferenceTime.ISSUE_TIME,
                                            Duration.ofHours( lead ),
                                            Duration.ofHours( lead ) );
        }
        //Valid dates available
        else if ( projectDetails.getEarliestDate() != null && projectDetails.getLatestDate() != null)
        {
            windowMetadata = TimeWindow.of( Instant.parse( projectDetails.getEarliestDate() ),
                                  Instant.parse( projectDetails.getLatestDate() ),
                                  ReferenceTime.VALID_TIME,
                                  Duration.ofHours( lead ),
                                  Duration.ofHours( lead ) );
        }
        //Issue dates available
        else if ( projectDetails.getEarliestIssueDate() != null && projectDetails.getLatestIssueDate() != null )
        {
            return TimeWindow.of( Instant.parse( projectDetails.getEarliestIssueDate() ),
                                  Instant.parse( projectDetails.getLatestIssueDate() ),
                                  ReferenceTime.ISSUE_TIME,
                                  Duration.ofHours( lead ),
                                  Duration.ofHours( lead ) );
        }
        //No dates available
        else
        {
            Duration leadTime = Duration.ofHours( lead );
            return TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, leadTime, leadTime );
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

        private transient final ZoneOffset zoneOffset;

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
}
