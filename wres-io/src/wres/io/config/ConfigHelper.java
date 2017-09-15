package wres.io.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.LocalDateTime;
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
import wres.config.generated.Coordinate;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeAggregationConfig;
import wres.config.generated.TimeAggregationFunction;
import wres.config.generated.TimeAggregationMode;
import wres.io.data.caching.Features;
import wres.io.data.caching.Variables;
import wres.util.Collections;
import wres.util.Strings;
import wres.util.Time;

public class ConfigHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);

    private static final ConcurrentMap<ProjectConfig, ConcurrentSkipListSet<String>> messages
            = new ConcurrentHashMap<>();

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
        if (config.getConditions() == null
            || config.getConditions().getFeatures() == null)
        {
            return "";
        }

        StringJoiner result = new StringJoiner(",", "feature_id in (", ")");

        try
        {
            // build a sql string of feature_ids, using cache to populate as needed
            for (Feature feature : Collections.where(config.getConditions().getFeatures(), feature -> {
                return feature.getLocation() != null && !feature.getLocation().getLid().isEmpty();
            }))
            {
                Integer i = Features.getFeatureID(feature.getLocation().getLid(), feature.getLocation().getName());
                result.add(Integer.toString(i));
            }
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
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
     * @param dataSourceConfig
     * @param currentLead
     * @return
     * @throws InvalidPropertiesFormatException Thrown if the time aggregation unit is not supported
     */
    private static int getLead(DataSourceConfig dataSourceConfig, int currentLead) throws InvalidPropertiesFormatException {
        TimeAggregationConfig timeAggregationConfig = ConfigHelper.getTimeAggregation( dataSourceConfig );
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

    public static TimeAggregationConfig getTimeAggregation(DataSourceConfig dataSourceConfig)
    {
        TimeAggregationConfig timeAggregationConfig = dataSourceConfig.getTimeAggregationDescription();

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
     * @param dataSourceConfig The configuration that controls how windows are calculated
     * @param windowNumber The indicator of the window whose lead description needs.  In the simplest case, the first
     *                     window could represent 'lead = 1' while the third 'lead = 3'. In more complicated cases,
     *                     the first window could be '40 &gt; lead AND lead &ge; 1' and the second '80 &gt; lead AND lead &ge; 40'
     * @return A description of what a window number means in terms of lead times
     * @throws InvalidPropertiesFormatException Thrown if the time aggregation unit is not supported
     */
    public static String getLeadQualifier(DataSourceConfig dataSourceConfig,
                                          int windowNumber)
            throws InvalidPropertiesFormatException
    {
        String qualifier;

        TimeAggregationConfig timeAggregationConfig = ConfigHelper.getTimeAggregation( dataSourceConfig );

        if (!(timeAggregationConfig.getPeriod() == 1 && timeAggregationConfig.getUnit() == DurationUnit.HOUR)) {
            int beginning = getLead( dataSourceConfig, windowNumber - 1 );
            int end = getLead( dataSourceConfig, windowNumber );
            qualifier = String.valueOf(end) +
                        " >= lead AND lead >";

            if (beginning == 0)
            {
                qualifier += "= ";
            }
            else
            {
                qualifier += " ";
            }
            qualifier += String.valueOf(beginning);
        }
        else
        {
            qualifier = "lead = " + getLead(dataSourceConfig, windowNumber);
        }

        return qualifier;
    }

    public static String getVariablePositionClause( Feature feature, int variableId)
    {
        StringBuilder clause = new StringBuilder();

        if (feature.getLocation() != null)
        {
            try
            {
                Integer variablePositionId = Features.getVariablePositionID(feature.getLocation().getLid(),
                                                                            feature.getLocation().getName(),
                                                                            variableId);

                if (variablePositionId != null)
                {
                    clause.append("variableposition_id = ").append(variablePositionId);
                }
            }
            catch (Exception e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }
        else if (feature.getIndex() != null)
        {
            throw new wres.util.NotImplementedException("Selecting a variable position based on its x and y values has not been implemented yet.");
        }
        else if (feature.getPoint() != null)
        {
            throw new wres.util.NotImplementedException("Selecting a variable position based on a coordinate has not been implemented yet.");
        }
        else if (feature.getPolygon() != null)
        {
            throw new wres.util.NotImplementedException("Selecting variable positions based on a polygon has not be implemented yet.");
        }

        return clause.toString();
    }

    public static double getWindowWidth( TimeAggregationConfig timeAggregationConfig)
            throws InvalidPropertiesFormatException
    {
        return Time.unitsToHours(timeAggregationConfig.getUnit().value(), timeAggregationConfig.getPeriod());
    }

    public static boolean isForecast(DataSourceConfig dataSource)
    {
        return dataSource != null &&
                Strings.isOneOf(dataSource.getType().value(),
                                DatasourceType.SINGLE_VALUED_FORECASTS.value(),
                                DatasourceType.ENSEMBLE_FORECASTS.value());
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
        if (config.getConditions() == null)
        {
            return null;
        }

        String earliest = "";

        if ( config.getConditions().getDates() != null
             && config.getConditions().getDates().getEarliest() != null )
        {
            try
            {
                earliest = config.getConditions().getDates().getEarliest();
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
                                config.getConditions().getDates().sourceLocation().getLineNumber(),
                                config.getConditions().getDates().sourceLocation().getColumnNumber() );
                }
                return null;
            }
        }
        else
        {
            String messageId = "no_earliest_date";
            if (LOGGER.isInfoEnabled() && ConfigHelper.messageSendPutIfAbsent(config, messageId))
            {
                LOGGER.info("No \"earliest\" date found in project. Use <dates earliest=\"2017-06-27T16:14\" latest=\"2017-07-06T11:35\" /> under <conditions> (near line {} column {} of project file) to specify an earliest date.",
                            config.getConditions().sourceLocation().getLineNumber(),
                            config.getConditions().sourceLocation().getColumnNumber());
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
        if (config.getConditions() == null)
        {
            return null;
        }

        String latest = "";

        if ( config.getConditions().getDates() != null
             && config.getConditions().getDates().getLatest() != null )
        {
            try
            {
                latest = config.getConditions().getDates().getLatest();
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
                                config.getConditions().getDates().sourceLocation().getLineNumber(),
                                config.getConditions().getDates().sourceLocation().getColumnNumber() );
                }
                return null;
            }
        }
        else
        {
            String messageId = "no_latest_date";
            if (LOGGER.isInfoEnabled() && ConfigHelper.messageSendPutIfAbsent(config, messageId))
            {
                LOGGER.info("No \"latest\" date found in project. Use <dates earliest=\"2017-06-27T16:14\" latest=\"2017-07-06T11:35\" />  under <conditions> (near line {} col {} of project file) to specify a latest date.",
                            config.getConditions().sourceLocation().getLineNumber(),
                            config.getConditions().sourceLocation().getColumnNumber());

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

        if (feature.getLocation() != null)
        {
            if (Strings.hasValue(feature.getLocation().getLid())) {
                description = feature.getLocation().getLid();
            }
            else if (Strings.hasValue(feature.getLocation().getHuc())) {
                description = feature.getLocation().getHuc();
            }
            else if (feature.getLocation().getComid() != null)
            {
                description = String.valueOf(feature.getLocation().getComid());
            }
            else if (Strings.hasValue(feature.getLocation().getGageId()))
            {
                description = feature.getLocation().getGageId();
            }
        }
        else if (feature.getPolygon() != null && feature.getPolygon().getPoint().size() >= 3)
        {
            description = "Within_";
            List<String> coordinates = new ArrayList<>();

            for (Coordinate coordinate : feature.getPolygon().getPoint())
            {
                coordinates.add(String.valueOf(coordinate.getX()) + "," + String.valueOf(coordinate.getY()));
            }

            description += String.join("_", coordinates);
        }
        else if (feature.getPoint() != null)
        {
            description = String.valueOf(feature.getPoint().getX()) + "," + String.valueOf(feature.getPoint().getY());
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
}
