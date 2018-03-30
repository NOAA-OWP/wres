package wres.io.reading.usgs;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.StatementRunner;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.USGSParameters;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.VariableDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.waterml.Response;
import wres.io.reading.waterml.WaterMLSource;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.utilities.Database;
import wres.io.utilities.IOExceptionalConsumer;
import wres.io.utilities.NoDataException;
import wres.util.FormattedStopwatch;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.TimeHelper;

public class USGSReader extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( USGSReader.class );

    // Hardcoded because this is the reader for USGS' services, not a generic waterml service
    // If a generic water ML service is required, one should be written for it.
    private static final String USGS_URL = "https://waterservices.usgs.gov/nwis/";
    private static final String INSTANTANEOUS_VALUE = "iv";
    private static final String DAILY_VALUE = "dv";

    public static final String EARLIEST_DATE = "2008-01-01T00:00:00Z";

    // There's a chance this operation will output the time in the wrong format
    public static final String LATEST_DATE = TimeHelper.convertDateToString( OffsetDateTime.now( ZoneId.of( "UTC" ) ) );

    /**
     * The amount of features that may be retrieved in a single USGS request
     */
    private static final int FEATURE_REQUEST_LIMIT = 100;

    private static final String UPSERT =
            "WITH upsert AS" + System.lineSeparator() +
            "(" + System.lineSeparator() +
            "    UPDATE wres.Observation" + System.lineSeparator() +
            "        SET observed_value = ?" + System.lineSeparator() +
            "    WHERE variableposition_id = ?" + System.lineSeparator() +
            "        AND observation_time = (?)::timestamp without time zone" + System.lineSeparator() +
            "        AND measurementunit_id = ?" + System.lineSeparator() +
            "        AND source_id = ?" + System.lineSeparator() +
            "    RETURNING *" + System.lineSeparator() +
            ")" + System.lineSeparator() +
            "INSERT INTO wres.Observation (" + System.lineSeparator() +
            "    variableposition_id," + System.lineSeparator() +
            "    observation_time," + System.lineSeparator() +
            "    observed_value," + System.lineSeparator() +
            "    measurementunit_id," + System.lineSeparator() +
            "    source_id" + System.lineSeparator() +
            ")" + System.lineSeparator() +
            "SELECT ?, (?)::timestamp without time zone, ?, ?, ?" + System.lineSeparator() +
            "WHERE NOT EXISTS (" + System.lineSeparator() +
            "    SELECT 1" + System.lineSeparator() +
            "    FROM upsert U" + System.lineSeparator() +
            "    WHERE U.variableposition_id = ?" + System.lineSeparator() +
            "        AND U.observation_time = (?)::timestamp without time zone" + System.lineSeparator() +
            "        AND U.measurementunit_id = ?" + System.lineSeparator() +
            "        AND U.source_id = ?" + System.lineSeparator() +
            ");" + System.lineSeparator();

    public USGSReader( ProjectConfig projectConfig )
    {
        super( projectConfig );
    }

    private class UpsertValue
    {
        UpsertValue(String gageID, String observationTime, Double value)
        {
            this.gageID = gageID;
            this.observationTime = TimeHelper.standardize( observationTime );
            this.value = value;
        }

        Object[] getParameters() throws SQLException
        {
            return new Object[] {
                    this.value,
                    getVariablePositionID(gageID),
                    this.observationTime,
                    parameter.getMeasurementUnitID(),
                    getSourceID(),
                    getVariablePositionID(gageID),
                    this.observationTime,
                    this.value,
                    parameter.getMeasurementUnitID(),
                    getSourceID(),
                    getVariablePositionID(gageID),
                    this.observationTime,
                    parameter.getMeasurementUnitID(),
                    getSourceID()
            };
        }

        private final String observationTime;
        private final Double value;
        private final String gageID;
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        // This is saved as the output time for the source
        this.operationStartTime = TimeHelper.convertDateToString( OffsetDateTime.now() );

        int featuresSaved = 0;
        List<Collection<FeatureDetails>> featureDetails;

        try
        {
            // Break down locations to request in easily digestible blocks
            featureDetails = this.getFeatureRequestBlocks();

            if (featureDetails.size() == 0)
            {
                throw new IngestException( "No features to ingest from USGS "
                                           + "could be found." );
            }
        }
        catch ( SQLException e )
        {
            throw new IOException( "The process used to determine what "
                                   + "features to request failed.", e );
        }

        // Request Observation data for each block of locations
        for (Collection<FeatureDetails> details : featureDetails)
        {
            // Request observation data from USGS
            Response usgsResponse = this.load(details);

            // Throw an error if absolutely nothing came back from USGS (this
            // is unlikely to ever happen, considering that an error should
            // have been hit in "load(..)")
            if (usgsResponse == null )
            {
                throw new IOException( "No USGS data could be loaded with the given configuration." );
            }

            // Since we're sending many requests instead of one, there's a
            // chance a single request might not values but requests before
            // and after will. In that case, we should continue.
            if (wres.util.Collections.exists(
                    usgsResponse.getValue().getTimeSeries(), (TimeSeries series) ->
                            series.getValues() == null || series.getValues().length == 0 ))
            {
                LOGGER.debug("No timeseries were returned from the query:");
                LOGGER.debug(usgsResponse.getValue().getQueryInfo().getQueryURL());
                continue;
            }

            LOGGER.debug( "There are a grand total of {} different locations "
                          + "that we want to save data to.",
                          details.size());

            // Despite the code lying within the following class looking
            // identical to the code within this reader, the following results
            // in consistent database deadlocks. This should eventually be used,
            // but it is unable for now.
            /*try
            {
                WaterMLSource source = new WaterMLSource( this.projectConfig,
                                                          usgsResponse,
                                                          this.getSourceID(),
                                                          this.parameter.getMeasurementUnitID() );
                source.setDataSourceConfig( this.getDataSourceConfig() );
                source.setSourceConfig( this.getSourceConfig() );
                source.setSpecifiedFeatures( this.getSpecifiedFeatures() );
                source.setInvalidSeriesHandler( this.createInvalidTimeSeriesHandler() );
                return source.save();
            }
            catch ( SQLException e )
            {
                throw new IOException("Information about USGS source data could not "
                                      + "be saved to the database.", e);
            }*/

            // Save data from the response and record the number of saved features
            featuresSaved += this.saveResponse( usgsResponse );
        }

        // Throw an error if nothing was saved
        if (featuresSaved == 0)
        {
            throw new IngestException( "No data from any USGS features could "
                                       + "be saved for evaluation." );
        }

        // Complete lingering upserts
        try
        {
            this.performUpserts();
        }
        catch ( SQLException e )
        {
            throw new IOException( "USGS observations could not be saved.", e );
        }

        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                this.getDataSourceConfig(),
                                                this.getHash(),
                                                false );
    }

    private List<Collection<FeatureDetails>> getFeatureRequestBlocks()
            throws SQLException
    {
        List<Collection<FeatureDetails>> requestBlocks = new ArrayList<>();

        Collection<FeatureDetails> details = Features.getAllDetails( this.getProjectConfig() );

        if (details.size() > USGSReader.FEATURE_REQUEST_LIMIT)
        {
            List<FeatureDetails> block = new ArrayList<>();

            for (FeatureDetails featureDetails : details)
            {
                block.add(featureDetails);

                if (block.size() > USGSReader.FEATURE_REQUEST_LIMIT)
                {
                    requestBlocks.add( block );
                    block = new ArrayList<>();
                }
            }

            if (block.size() > 0)
            {
                requestBlocks.add( block );
            }
        }
        else
        {
            requestBlocks.add( details );
        }

        return requestBlocks;
    }

    private int saveResponse(Response usgsResponse) throws IOException
    {
        int readSeriesCount = 0;


        for (TimeSeries series : usgsResponse.getValue().getTimeSeries())
        {
            boolean validSeries = this.readSeries( series );

            if (validSeries)
            {
                readSeriesCount++;
            }
        }

        LOGGER.debug( "Data for {} different locations have been saved.",
                      readSeriesCount );

        return readSeriesCount;
    }

    private Response load(Collection<FeatureDetails> featureDetails) throws IOException
    {
        String requestURL = USGS_URL;
        Client client = null;

        Response usgsResponse;
        try
        {

            client = ClientBuilder.newClient();
            WebTarget webTarget = client.target( USGS_URL );

            if (this.dataSourceConfig.getExistingTimeScale() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            // Determines if we use the daily REST service or the instantaenous REST service
            webTarget = webTarget.path( this.getValueType() );

            // Not necessary, but aids with debugging
            webTarget = webTarget.queryParam( "indent", "on" );

            // The current object tree supports JSON; additional work will
            // need to be done to support XML.
            webTarget = webTarget.queryParam( "format", "json" );
            webTarget = webTarget.queryParam( "sites", this.getGageIdParameter( featureDetails ) );
            webTarget = webTarget.queryParam( "startDT", this.getStartDate() );
            webTarget = webTarget.queryParam( "endDT", this.getEndDate() );

            // We use "all" because we could theoretically need historical data
            // from now defunct sites
            webTarget = webTarget.queryParam( "siteStatus", "all" );
            webTarget = webTarget.queryParam( "parameterCd", this.getParameterCode());

            requestURL = webTarget.getUri().toURL().toString();

            LOGGER.debug("Requesting data from: {}", requestURL);

            Invocation.Builder invocationBuilder =
                    webTarget.request( MediaType.APPLICATION_JSON );

            FormattedStopwatch stopwatch = new FormattedStopwatch();
            stopwatch.start();

            usgsResponse = invocationBuilder.get( Response.class );

            stopwatch.stop();

            LOGGER.debug( "It took {} to load the USGS data.", stopwatch.getFormattedDuration() );
        }
        catch (IOException e)
        {
            String message = "Data from the location '" +
                             String.valueOf(requestURL) +
                             "' could not be retrieved.";
            LOGGER.debug( message );
            throw new IngestException( message, e );
        }
        catch ( SQLException e )
        {
            throw new IngestException( "The desired parameter could not be determined.", e );
        }
        finally
        {
            if (client != null)
            {
                client.close();
            }
        }

        LOGGER.debug("A set of time series has been downloaded from USGS.");

        return usgsResponse;
    }

    private String getGageIdParameter(Collection<FeatureDetails> features)
            throws NoDataException
    {
        String parameter = null;

        for (FeatureDetails feature : features)
        {
            if ( Strings.hasValue( feature.getGageID()) && !Strings.hasValue( parameter ))
            {
                parameter = feature.getGageID();
            }
            else if (Strings.hasValue( feature.getGageID() ))
            {
                parameter += "," + feature.getGageID();
            }
        }

        if (!Strings.hasValue( parameter ))
        {
            throw new NoDataException( "No valid gageIDs could be found. USGS data could not be ingested." );
        }

        return parameter;
    }

    private String getParameterCode() throws SQLException, IngestException
    {
        if (this.parameterCode == null)
        {
            // If the user uses the explicit USGS code, use that and bypass everything else
            if (this.getDataSourceConfig().getVariable().getValue().matches( "\\d{5}" ))
            {
                this.parameterCode = this.getDataSourceConfig().getVariable().getValue();
            }
            else
            {
                this.parameterCode = this.getParameter().getParameterCode();
            }
        }
        return this.parameterCode;
    }

    private USGSParameters.USGSParameter getParameter()
            throws SQLException, IngestException
    {
        if (this.parameter == null)
        {
            String variableName = this.getDataSourceConfig().getVariable().getValue();

            // If someone enters a variable of the form "Discharge, cubic feet per second",
            // we should go ahead and try to find the data by the description of the variable
            if (variableName.matches( ".+,.+" ))
            {
                this.parameter = USGSParameters.getParameterByDescription( variableName );
            }
            else
            {
                String unit;

                if ( this.getDataSourceConfig().getVariable().getUnit()
                     != null )
                {
                    unit = this.getDataSourceConfig().getVariable().getUnit();
                }
                else
                {
                    unit = this.getProjectConfig().getPair().getUnit();

                    VariableDetails
                            currentDetails =
                            Variables.getByName( variableName );

                    if ( currentDetails != null
                         && currentDetails.getMeasurementunitId() != null )
                    {
                        unit =
                                MeasurementUnits.getNameByID( currentDetails.getMeasurementunitId() );
                    }
                }

                if (unit != null && this.getDataSourceConfig().getExistingTimeScale() == null)
                {
                    this.parameter = USGSParameters.getParameter( variableName, unit );
                }
                else if ( unit != null )
                {
                    String aggregation;
                    if ( this.getDataSourceConfig()
                             .getExistingTimeScale()
                             .getUnit() == DurationUnit.SECONDS )
                    {
                        aggregation = "instant";
                    }
                    else
                    {
                        aggregation = this.getDataSourceConfig()
                                          .getExistingTimeScale()
                                          .getFunction()
                                          .value();
                    }

                    this.parameter = USGSParameters.getParameter( variableName,
                                                                  unit,
                                                                  aggregation );
                }
                else
                {
                    String message =
                            "Not enough information was supplied to find " +
                            "the requested USGS values. A variable name and " +
                            "measurement are both required to find the right" +
                            "data.";
                    throw new IngestException( message );
                }
            }
        }

        return this.parameter;
    }

    private String getStartDate() throws IOException
    {
        if (this.startDate == null)
        {

            if (this.dataSourceConfig.getExistingTimeScale() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            Instant earliest = ConfigHelper.getEarliestDateTimeFromDataSources( this.getProjectConfig() );
            if ( earliest == null)
            {
                this.startDate = EARLIEST_DATE;
            }
            else
            {
                this.startDate = earliest.toString();
            }

            switch (this.dataSourceConfig.getExistingTimeScale().getUnit())
            {
                case DAYS:
                    // No time or time zone information is allowed
                    this.startDate = TimeHelper.convertStringDateTimeToDate( this.startDate );
                    break;
                default:
                    // The space inbetween the date and time needs to be split with a T
                    this.startDate = this.startDate.replaceAll( " ", "T" );
                    break;
            }
        }
        return this.startDate;
    }

    private String getEndDate() throws IOException
    {
        if (this.endDate == null)
        {
            if (this.dataSourceConfig.getExistingTimeScale() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            Instant latest = ConfigHelper.getLatestDateTimeFromDataSources( this.getProjectConfig() );
            if ( latest == null )
            {
                this.endDate = LATEST_DATE;
            }
            else
            {
                this.endDate = latest.toString();
            }

            switch (this.dataSourceConfig.getExistingTimeScale().getUnit())
            {
                case DAYS:
                    // No time or time zone information is allowed
                    this.endDate = TimeHelper.convertStringDateTimeToDate( this.endDate );
                    break;
                default:

                    // USGS is [inclusive, exclusive) where we need
                    // [inclusive, inclusive]. We add time to ensure that we
                    // have all the data we need
                    OffsetDateTime dateTime = OffsetDateTime.parse( this.endDate );
                    dateTime = dateTime.withOffsetSameInstant( ZoneOffset.UTC );
                    dateTime = dateTime.plus(
                            this.dataSourceConfig.getExistingTimeScale().getPeriod(),
                            ChronoUnit.valueOf(
                                    this.dataSourceConfig.getExistingTimeScale()
                                                         .getUnit()
                                                         .value()
                                                         .toUpperCase()
                            )
                    );
                    this.endDate = TimeHelper.convertDateToString( dateTime );

                    // The space inbetween the date and time needs to be split with a T
                    this.endDate = this.endDate.replaceAll( " ", "T" );
                    this.endDate += "Z";
                    break;
            }
        }
        return this.endDate;
    }

    private String getValueType()
    {
        String valueType;

        if ( this.dataSourceConfig.getExistingTimeScale() != null &&
             this.dataSourceConfig.getExistingTimeScale().getUnit() == DurationUnit.DAYS)
        {
            valueType = DAILY_VALUE;
        }
        else
        {
            valueType = INSTANTANEOUS_VALUE;
        }

        return valueType;
    }

    private Integer getVariablePositionID(String gageID) throws SQLException
    {
        if (this.variablePositionIDs == null)
        {
            this.variablePositionIDs = new TreeMap<>(  );
        }

        FeatureDetails details;

        if (!this.variablePositionIDs.containsKey( gageID ))
        {
            details =
                    wres.util.Collections.find( this.getFeatureDetailsSet(),
                                                feature ->
                                                        feature.getGageID()
                                                        != null &&
                                                        feature.getGageID()
                                                               .equalsIgnoreCase(
                                                                       gageID ) );

            this.variablePositionIDs.put( gageID,
                                          details.getVariablePositionID(
                                                  this.getVariableID() ) );
        }

        return this.variablePositionIDs.get( gageID );
    }

    private Integer getVariableID() throws SQLException
    {
        if (this.variableID == null)
        {
            DataSourceConfig.Variable variable;

            if (this.getProjectConfig().getInputs().getLeft().equals( this.getDataSourceConfig() ))
            {
                variable = this.getProjectConfig()
                               .getInputs()
                               .getLeft()
                               .getVariable();
            }
            else if (this.getProjectConfig().getInputs().getRight().equals( this.getDataSourceConfig() ))
            {
                variable = this.getProjectConfig()
                               .getInputs()
                               .getRight()
                               .getVariable();
            }
            else
            {
                variable = this.getProjectConfig()
                               .getInputs()
                               .getBaseline()
                               .getVariable();
            }
            this.variableID = Variables.getVariableID( variable.getValue(),
                                                       variable.getUnit() );
        }

        return this.variableID;
    }

    private void addValue(String gageID, String observationTime, Double value)
            throws SQLException
    {
        this.upsertValues.add(
                new UpsertValue( gageID,
                                 observationTime,
                                 value )
        );

        if ( this.upsertValues.size() >= SystemSettings.maximumDatabaseInsertStatements())
        {
            this.performUpserts();
        }
    }

    private IOExceptionalConsumer<TimeSeries> createInvalidTimeSeriesHandler()
    {
        return series -> {
            String gageId = series.getSourceInfo().getSiteCode()[0].getValue();

            FeatureDetails invalidFeature = null;

            try
            {
                for (FeatureDetails feature : this.getFeatureDetailsSet())
                {
                    if (Strings.hasValue( feature.getGageID()) && feature.getGageID().equalsIgnoreCase( gageId ) )
                    {
                        invalidFeature = feature;
                        break;
                    }
                }

                this.getFeatureDetailsSet().remove( invalidFeature );
                LOGGER.trace("The location '{}' was removed from the project because it didn't have valid USGS data.",
                             String.valueOf(invalidFeature));
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not iterate through available " +
                                       "features in order to avoid processing " +
                                       "the invalid location '" + gageId + "'" ,
                                       e);
            }
        };
    }

    private boolean readSeries(TimeSeries series) throws IOException
    {
        String gageID = series.getSourceInfo().getSiteCode()[0].getValue();
        int validSeriesCount = 0;

        if (series.getValues().length == 0)
        {
            FeatureDetails invalidFeature = null;

            try
            {
                for ( FeatureDetails feature : this.getFeatureDetailsSet())
                {
                    if (Strings.hasValue( feature.getGageID() ) && feature.getGageID().equalsIgnoreCase( gageID ))
                    {
                        invalidFeature = feature;
                        break;
                    }
                }

                this.getFeatureDetailsSet().remove( invalidFeature );
                LOGGER.trace("The location '{}' was removed from the project because it didn't have valid USGS data.",
                             String.valueOf(invalidFeature));
                return false;
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not iterate through available " +
                                       "features in order to avoid processing " +
                                       "the invalid location '" + gageID + "'" ,
                                       e);
            }
        }

        for (TimeSeriesValues valueSet : series.getValues())
        {
            if (valueSet.getValue().length == 0)
            {
                continue;
            }

            for (TimeSeriesValue value : valueSet.getValue())
            {
                try
                {
                    Double readValue = value.getValue();

                    if (series.getVariable().getNoDataValue() != null &&
                        Precision.equals(readValue, series.getVariable().getNoDataValue(), EPSILON))
                    {
                        readValue = null;
                    }

                    this.addValue( series.getSourceInfo().getSiteCode()[0].getValue(),
                                   value.getDateTime(),
                                   readValue );
                }
                catch ( SQLException e )
                {
                    throw new IOException( e );
                }
            }

            validSeriesCount++;
        }

        if (validSeriesCount == 0)
        {
            FeatureDetails invalidFeature = null;

            try
            {
                for (FeatureDetails feature : this.getFeatureDetailsSet() )
                {
                    if (Strings.hasValue( feature.getGageID() ) && feature.getGageID().equalsIgnoreCase( gageID ))
                    {
                        invalidFeature = feature;
                        break;
                    }
                }

                this.getFeatureDetailsSet().remove( invalidFeature );
                LOGGER.trace("The location '{}' was removed from the project because it didn't have valid USGS data.",
                             String.valueOf(invalidFeature));
                return false;
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not iterate through available " +
                                       "features in order to avoid processing " +
                                       "the invalid location '" + gageID + "'" ,
                                       e);
            }
        }

        LOGGER.info("A USGS time series has been parsed for location '{}'",
                    series.getSourceInfo().getSiteName());

        return true;
    }

    private void performUpserts() throws SQLException
    {
        if (this.upsertValues.size() > 0)
        {
            List<Object[]> values = new ArrayList<>(  );

            while (!upsertValues.empty())
            {
                values.add( upsertValues.pop().getParameters() );
            }

            StatementRunner statement = new StatementRunner( USGSReader.UPSERT, values );
            //statement.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            Database.ingest(statement);

            //ProgressMonitor.increment();
            LOGGER.trace("USGS data has been submitted to the database queue.");
        }
    }

    @Override
    protected String getHash()
    {
        if (this.hash == null)
        {
            // TODO: hash the contents, not the name
            // Currently, the NWIS url along with the service (instantaneous or
            // daily) is saved, representing "yeah, this data came from NWIS".
            // This benefits us by only saving unique data to the database.
            // If I have one project asking for May through July and June
            // through August, this prevents us from saving duplicate data.

            // If we accept the duplicate data, we'll need to generate a
            // source_id per request instead of per ingest
            this.hash = Strings.getMD5Checksum(
                    (USGSReader.USGS_URL + "/" + getValueType() ).getBytes()
            );
        }
        return this.hash;
    }

    @Override
    protected Logger getLogger()
    {
        return USGSReader.LOGGER;
    }

    private Integer getSourceID() throws SQLException
    {
        if (this.sourceID == null)
        {
            this.sourceID = DataSources.getSourceID(USGSReader.USGS_URL,
                                                       operationStartTime,
                                                       null,
                                                       this.getHash());
        }
        return sourceID;
    }

    private Set<FeatureDetails> getFeatureDetailsSet()
            throws SQLException
    {
        if ( this.featureDetailsSet == null )
        {
            this.featureDetailsSet = Features.getAllDetails( this.getProjectConfig() );
        }
        return this.featureDetailsSet;
    }

    // If daily values were used, start and end date cannot have minutes or timezones
    // While these are hardcoded, they will need to be specified from the config
    // date must be separated by T, negative timezone will need to be separated by %2b
    //private String startDate = "2017-08-01T00:00%2b0000";
    //private String endDate = "2017-11-01T00:00%2b0000";

    private String startDate;
    private String endDate;
    private Integer variableID;
    private Map<String, Integer> variablePositionIDs;
    private Integer sourceID;
    private String hash;

    private final Stack<UpsertValue> upsertValues = new Stack<>();

    // "00060" corresponds to a specific type of discharge. We need a list and
    // a way to transform user specifications about variable, time aggregation,
    // and measurement to find the correct code from the config
    private String parameterCode;

    private USGSParameters.USGSParameter parameter;

    private String operationStartTime;
    private Set<FeatureDetails> featureDetailsSet;
}
