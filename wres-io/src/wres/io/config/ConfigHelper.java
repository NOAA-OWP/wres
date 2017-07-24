package wres.io.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.*;
import wres.io.data.caching.Features;
import wres.util.Collections;
import wres.util.Strings;
import wres.util.Time;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.InvalidPropertiesFormatException;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class ConfigHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigHelper.class);

    private static final ConcurrentMap<ProjectConfig, ConcurrentSkipListSet<String>> messages
            = new ConcurrentHashMap<>();

    /**
     * Given a config, generate feature IDs and return a sql string of them.
     *
     * @param config the project config.
     * @return sql string useful in a where clause
     */
    public static String getFeatureIdsAndPutIfAbsent(ProjectConfig config)
    throws IOException
    {
        if (config.getConditions() == null
            || config.getConditions().getFeature() == null)
        {
            return "";
        }

        StringJoiner result = new StringJoiner(",", "feature_id in (", ")");

        try
        {
            // build a sql string of feature_ids, using cache to populate as needed
            for (Conditions.Feature feature : Collections.where(config.getConditions().getFeature(), feature -> {
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

    /**
     *
     * @param projectConfig
     * @param currentLead
     * @return
     * @throws InvalidPropertiesFormatException Thrown if the time aggregation unit is not supported
     */
    private static int getLead(ProjectConfig projectConfig, int currentLead) throws InvalidPropertiesFormatException {
        return Time.unitsToHours(projectConfig.getPair().getTimeAggregation().getUnit().name(),
                                 currentLead).intValue();
    }

    /**
     *
     * @param config
     * @param currentLead
     * @param finalLead
     * @return
     */
    public static boolean leadIsValid(ProjectConfig config, int currentLead, int finalLead)
    {
        Integer lead = null;

        try {
            lead = getLead(config, currentLead);
        }
        catch (InvalidPropertiesFormatException e) {
            e.printStackTrace();
        }

        return lead != null &&
                lead <= finalLead &&
                lead <= config.getConditions().getLastLead() &&
                lead >= config.getConditions().getFirstLead();
    }

    /**
     *
     * @param projectConfig
     * @param step
     * @return
     * @throws InvalidPropertiesFormatException Thrown if the time aggregation unit is not supported
     */
    public static String getLeadQualifier(ProjectConfig projectConfig, int step) throws InvalidPropertiesFormatException {
        String qualifier;

        if (projectConfig.getPair().getTimeAggregation() != null && projectConfig.getPair().getTimeAggregation().getPeriod().get(0) > 1) {
            int period = projectConfig.getPair().getTimeAggregation().getPeriod().get(0);
            Double range = null;
            range = Time.unitsToHours(projectConfig.getPair().getTimeAggregation().getUnit().value(), period);
            qualifier = String.valueOf((int) (step * range)) + " > lead && lead >= " + String.valueOf((int) ((step - 1) * range));
        }
        else
        {
            qualifier = "lead = " + getLead(projectConfig, step);
        }

        return qualifier;
    }

    public static String getVariablePositionClause(Conditions.Feature feature, int variableId) throws wres.util.NotImplementedException {
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
                e.printStackTrace();
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

    public static boolean isForecast(DataSourceConfig dataSource)
    {
        return dataSource != null &&
                Strings.isOneOf(dataSource.getType().value(),
                                DatasourceType.ASSIMILATIONS.value(),
                                DatasourceType.SIMPLE_FORECASTS.value(),
                                DatasourceType.ENSEMBLE_FORECASTS.value(),
                                DatasourceType.MODEL_OUTPUTS.value());
    }

    public static ProjectConfig read(final String path) throws JAXBException {
        ProjectConfig projectConfig;

        File xmlFile = new File(path);
        Source xmlSource = new StreamSource(xmlFile);
        JAXBContext jaxbContext = JAXBContext.newInstance(ObjectFactory.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();

        JAXBElement<ProjectConfig> wrappedConfig = jaxbUnmarshaller.unmarshal(xmlSource, ProjectConfig.class);
        projectConfig = wrappedConfig.getValue();

        return projectConfig;
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
     * @param config
     * @return the most narrow "earliest" date, null otherwise
     */
    public static LocalDateTime getEarliestDateTimeFromDataSources(ProjectConfig config)
    {
        if (config.getConditions() == null)
        {
            return null;
        }

        String earliest = "";

        try
        {
            earliest = config.getConditions().getDates().getEarliest();
            return LocalDateTime.parse(earliest);
        }
        catch (NullPointerException npe)
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
        catch (DateTimeParseException dtpe)
        {
            String messageId = "date_parse_exception_earliest_date";
            if (LOGGER.isWarnEnabled() && ConfigHelper.messageSendPutIfAbsent(config, messageId))
            {
                LOGGER.warn("Correct the date \"{}\" near line {} column {} to ISO8601 format such as \"2017-06-27T16:16\"",
                            earliest,
                            config.getConditions().getDates().sourceLocation().getLineNumber(),
                            config.getConditions().getDates().sourceLocation().getColumnNumber());
            }
            return null;
        }
    }

    /**
     * Returns the earlier of any "latest" date specified in left or right datasource.
     * If only one date is specified, that one is returned.
     * If no dates for "latest" are specified, null is returned.
     * @param config
     * @return the most narrow "latest" date, null otherwise.
     */
    public static LocalDateTime getLatestDateTimeFromDataSources(ProjectConfig config)
    {
        if (config.getConditions() == null)
        {
            return null;
        }

        String latest = "";

        try
        {
            latest = config.getConditions().getDates().getLatest();
            return LocalDateTime.parse(latest);
        }
        catch (NullPointerException npe)
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
        catch (DateTimeParseException dtpe)
        {
            String messageId = "date_parse_exception_latest_date";
            if (LOGGER.isWarnEnabled() && ConfigHelper.messageSendPutIfAbsent(config, messageId))
            {
                LOGGER.warn("Correct the date \"{}\" after line {} col {} to ISO8601 format such as \"2017-06-27T16:16\"",
                            latest,
                            config.getConditions().getDates().sourceLocation().getLineNumber(),
                            config.getConditions().getDates().sourceLocation().getColumnNumber());
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
}
