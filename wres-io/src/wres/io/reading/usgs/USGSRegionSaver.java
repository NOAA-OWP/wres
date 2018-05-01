package wres.io.reading.usgs;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.CopyExecutor;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.USGSParameters;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.VariableDetails;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.waterml.Response;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.utilities.Database;
import wres.util.functional.ExceptionalConsumer;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.util.FormattedStopwatch;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.TimeHelper;

public class USGSRegionSaver extends WRESCallable<IngestResult>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(USGSRegionSaver.class);

    // Hardcoded because this is the reader for USGS' services, not a generic waterml service
    // If a generic water ML service is required, one should be written for it.
    private static final String USGS_URL = "https://waterservices.usgs.gov/nwis/";
    private static final String INSTANTANEOUS_VALUE = "iv";
    private static final String DAILY_VALUE = "dv";
    private static final String DELIMITER = "|";
    private static final double EPSILON = 0.0000001;

    private static final String EARLIEST_DATE = "2008-01-01T00:00:00Z";

    // There's a chance this operation will output the time in the wrong format
    private static final String LATEST_DATE = TimeHelper.convertDateToString(
            OffsetDateTime.now( ZoneId.of( "UTC" ) )
    );

    private static final String COPY_HEADER = "wres.Observation ("
                                              + "variableposition_id, "
                                              + "observation_time, "
                                              + "observed_value, "
                                              + "measurementunit_id, "
                                              + "source_id)";

    private static class WebResponse
    {
        private WebResponse(final Response usgsResponse,
                            final boolean alreadyRequested,
                            final int sourceId,
                            final String URL,
                            final String hash)
        {
            this.usgsResponse = usgsResponse;
            this.alreadyRequested = alreadyRequested;
            this.URL = URL;
            this.sourceId = sourceId;
            this.hash = hash;
        }

        private Response getUsgsResponse()
        {
            return this.usgsResponse;
        }

        private boolean wasAlreadyRequested()
        {
            return this.alreadyRequested;
        }

        private int getSourceId()
        {
            return this.sourceId;
        }

        private String getURL()
        {
            return this.URL;
        }

        private final Response usgsResponse;
        private final boolean alreadyRequested;
        private final int sourceId;
        private final String URL;
        private final String hash;
    }

    private static class WebResponseRetryStrategy
    {
        private static final int RETRIES = 10;
        private static final long WAIT_MILLIS = 1000;

        private int numberOfRetries;
        private long waitTime;
        private boolean hasAsked;

        boolean shouldTry()
        {
            // If we've never tried, yes, we want to try
            if (!hasAsked)
            {
                this.hasAsked = true;
                return true;
            }
            else if ( numberOfRetries == 0)
            {
                // If we've asked if we should try and didn't have to retry,
                // we're good we don't need to try again.
                return false;
            }

            return RETRIES - this.numberOfRetries > 0;
        }

        void errorOccured(WebApplicationException exception) throws IOException
        {
            this.numberOfRetries++;

            if (400 >= exception.getResponse().getStatus() && exception.getResponse().getStatus() < 500)
            {
                throw exception;
            }
            if (!this.shouldTry())
            {
                throw new IOException("", exception);
            }

            this.waitUntilNextTry();
        }

        private void waitUntilNextTry()
        {
            try
            {
                this.waitTime += WAIT_MILLIS;
                Thread.sleep( this.waitTime );
            }
            catch ( InterruptedException interruption )
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    USGSRegionSaver( final Collection<FeatureDetails> region,
                            final ProjectConfig projectConfig,
                            final DataSourceConfig dataSourceConfig)
            throws IOException
    {
        if (dataSourceConfig.getExistingTimeScale() == null)
        {
            throw new IOException( "An existing time scale must be defined to ingest USGS data" );
        }
        
        this.region = region;
        this.dataSourceConfig = dataSourceConfig;
        this.projectConfig = projectConfig;
    }

    @Override
    protected IngestResult execute() throws Exception
    {
        // This is saved as the output time for the source
        this.operationStartTime = TimeHelper.convertDateToString( OffsetDateTime.now() );

        IngestResult result = null;

        // Request observation data from USGS
        WebResponse response = this.load();

        // Throw an error if absolutely nothing came back from USGS (this
        // is unlikely to ever happen, considering that an error should
        // have been hit in "load(..)")
        if (response == null )
        {
            throw new IOException( "No USGS data could be loaded with the given configuration." );
        }

        if (response.wasAlreadyRequested())
        {
            result = IngestResult.from( this.projectConfig,
                                            this.dataSourceConfig,
                                            response.hash,
                                            true );
        }
        else
        {
            Response usgsResponse = response.getUsgsResponse();

            // Since we're sending many requests instead of one, there's a
            // chance a single request might not values but requests before
            // and after will. In that case, we should continue.
            if (usgsResponse.getValue().getNumberOfPopulatedTimeSeries() == 0)
            {
                LOGGER.debug( "No timeseries were returned from the query:" );
                LOGGER.debug( usgsResponse.getValue()
                                          .getQueryInfo()
                                          .getQueryURL() );
            }
            else
            {
                LOGGER.debug(
                        "There are a grand total of {} different locations "
                        + "that we want to save data to.",
                        usgsResponse.getValue()
                                    .getNumberOfPopulatedTimeSeries() );

                // Save data from the response and record the number of saved features
                int amountSaved = this.saveResponse( usgsResponse,
                                                     response.getSourceId() );

                if ( amountSaved > 0 )
                {
                    result = IngestResult.from( this.projectConfig,
                                                this.dataSourceConfig,
                                                response.hash,
                                                false );

                    LOGGER.debug( "Data for {} different locations have been saved.",
                                  amountSaved );
                }
            }
        }

        // Throw an error if nothing was saved
        if (result == null)
        {
            throw new IngestException( "No data from any USGS features could "
                                       + "be saved for evaluation." );
        }

        // Complete lingering copies
        try
        {
            this.performCopy();
        }
        catch ( SQLException e )
        {
            throw new IOException( "USGS observations could not be saved.", e );
        }


        return result;
    }
    
    private WebResponse load() throws IngestException
    {
        String requestURL = USGS_URL;
        Client client = null;
        WebTarget webTarget;
        
        Response usgsResponse;
        WebResponse response = null;

        String gageStatement;
        String earliestDate;
        String latestDate;
        String parameterCode;

        try
        {
            gageStatement = this.getGageIdParameter( this.region );
        }
        catch ( NoDataException e )
        {
            throw new IngestException( "Locations to request from USGS could not be determined.", e );
        }

        try
        {
            earliestDate = this.getStartDate();
        }
        catch ( IOException e )
        {
            throw new IngestException( "The date for the earliest data to ask "
                                       + "USGS for could not be determined.", e );
        }

        try
        {
            latestDate = this.getEndDate();
        }
        catch ( IOException e )
        {
            throw new IngestException( "The date of the latest data to ask "
                                       + "USGS for could not be determined.", e );
        }

        try
        {
            parameterCode = this.getParameterCode();
        }
        catch ( SQLException e )
        {
            throw new IngestException( "The USGS parameter code could not be "
                                       + "determined.", e );
        }

        try
        {
            client = ClientBuilder.newClient();
            webTarget = client.target( USGS_URL );
            // Determines if we use the daily REST service or the instantaenous REST service
            webTarget = webTarget.path( this.getValueType() );

            // Not necessary, but aids with debugging
            webTarget = webTarget.queryParam( "indent", "on" );

            // The current object tree supports JSON; additional work will
            // need to be done to support XML.
            webTarget = webTarget.queryParam( "format", "json" );
            webTarget = webTarget.queryParam( "sites", gageStatement );
            webTarget = webTarget.queryParam( "startDT", earliestDate );
            webTarget = webTarget.queryParam( "endDT", latestDate );

            // We use "all" because we could theoretically need historical data
            // from now defunct sites
            webTarget = webTarget.queryParam( "siteStatus", "all" );
            webTarget = webTarget.queryParam( "parameterCd", parameterCode);

            requestURL = webTarget.getUri().toURL().toString();
            String hash = Strings.getMD5Checksum( requestURL.getBytes());
            SourceDetails usgsDetails;

            try
            {
                usgsDetails = DataSources.get( requestURL,
                                               operationStartTime,
                                               null,
                                               hash
                );

                if (!usgsDetails.performedInsert())
                {
                    LOGGER.debug( "The data for '{}' had been previously ingested.", requestURL );
                    return new USGSRegionSaver.WebResponse( null,
                                                                true,
                                                                usgsDetails.getId(),
                                                                requestURL,
                                                                hash );
                }
            }
            catch ( SQLException e )
            {
                throw new IOException( "Source information about the requested "
                                       + "USGS data could not be found.", e );
            }

            LOGGER.debug("Requesting data from: {}", requestURL);

            Invocation.Builder invocationBuilder =
                    webTarget.request( MediaType.APPLICATION_JSON );

            FormattedStopwatch stopwatch = null;

            if (LOGGER.isDebugEnabled())
            {
                stopwatch = new FormattedStopwatch();
                stopwatch.start();
            }

            usgsResponse = invocationBuilder.get( Response.class );

            if (LOGGER.isDebugEnabled() && stopwatch != null)
            {
                stopwatch.stop();

                LOGGER.debug( "It took {} to download the USGS data.",
                              stopwatch.getFormattedDuration() );
            }

            WebResponseRetryStrategy retryStrategy = new WebResponseRetryStrategy();

            while (retryStrategy.shouldTry())
            {
                try
                {
                    response = new USGSRegionSaver.WebResponse( usgsResponse,
                                                                false,
                                                                usgsDetails.getId(),
                                                                requestURL,
                                                                hash );
                }
                catch (WebApplicationException exception)
                {
                    retryStrategy.errorOccured( exception );
                }
            }
        }
        catch ( IOException e)
        {
            String message = "Data from the location '" +
                             String.valueOf(requestURL) +
                             "' could not be retrieved.";
            LOGGER.debug( message );
            throw new IngestException( message, e );
        }
        finally
        {
            if (client != null)
            {
                client.close();
            }
        }
        
        return response;
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
            if (this.dataSourceConfig.getVariable().getValue().matches( "\\d{5}" ))
            {
                this.parameterCode = this.dataSourceConfig.getVariable().getValue();
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
            String variableName = this.dataSourceConfig.getVariable().getValue();

            // If someone enters a variable of the form "Discharge, cubic feet per second",
            // we should go ahead and try to find the data by the description of the variable
            if (variableName.matches( ".+,.+" ))
            {
                this.parameter = USGSParameters.getParameterByDescription( variableName );
            }
            else
            {
                String unit;

                if ( this.dataSourceConfig.getVariable().getUnit()
                     != null )
                {
                    unit = this.dataSourceConfig.getVariable().getUnit();
                }
                else
                {
                    unit = this.projectConfig.getPair().getUnit();

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

                if (unit != null && this.dataSourceConfig.getExistingTimeScale() == null)
                {
                    this.parameter = USGSParameters.getParameter( variableName, unit );
                }
                else if ( unit != null )
                {
                    String aggregation;
                    if ( this.dataSourceConfig
                             .getExistingTimeScale()
                             .getUnit() == DurationUnit.SECONDS )
                    {
                        aggregation = "instant";
                    }
                    else
                    {
                        aggregation = this.dataSourceConfig
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
                throw new IOException( "An existing time scale must be defined to ingest USGS data." );
            }

            Instant earliest = ConfigHelper.getEarliestDateTimeFromDataSources( this.projectConfig );
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

            Instant latest = ConfigHelper.getLatestDateTimeFromDataSources( this.projectConfig );
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
                    wres.util.Collections.find( this.region,
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

            if (this.projectConfig.getInputs().getLeft().equals( this.dataSourceConfig ))
            {
                variable = this.projectConfig
                               .getInputs()
                               .getLeft()
                               .getVariable();
            }
            else if (this.projectConfig.getInputs().getRight().equals( this.dataSourceConfig ))
            {
                variable = this.projectConfig
                               .getInputs()
                               .getRight()
                               .getVariable();
            }
            else
            {
                variable = this.projectConfig
                               .getInputs()
                               .getBaseline()
                               .getVariable();
            }
            this.variableID = Variables.getVariableID( variable.getValue() );
        }

        return this.variableID;
    }

    private void addValue(String gageID, String observationTime, Double value, int sourceID)
            throws SQLException
    {
        if (this.copyScript == null)
        {
            this.copyScript = new ScriptBuilder(  );
        }

        observationTime = OffsetDateTime.parse( observationTime)
                                        .withOffsetSameInstant( ZoneOffset.UTC )
                                        .toString();

        this.copyScript.add(this.getVariablePositionID( gageID )).add("|")
                       .add("'" + observationTime + "'").add("|");

        if (value == null)
        {
            this.copyScript.add("\\N").add("|");
        }
        else
        {
            this.copyScript.add( value ).add( "|" );
        }

        this.copyScript.add(this.parameter.getMeasurementUnitID()).add("|")
                       .addLine(sourceID);

        this.copyCount++;

        if ( this.copyCount >= SystemSettings.getMaximumCopies())
        {
            this.performCopy();
        }
    }

    private int saveResponse(Response usgsResponse, int sourceID) throws IOException
    {
        int readSeriesCount = 0;

        for ( TimeSeries series : usgsResponse.getValue().getTimeSeries() )
        {
            boolean validSeries = this.readSeries( series, sourceID );

            this.update( series );

            if ( validSeries )
            {
                readSeriesCount++;
            }
        }

        LOGGER.debug( "Data for {} different locations have been saved.",
                      readSeriesCount );

        return readSeriesCount;
    }

    private boolean readSeries( TimeSeries series, int sourceID) throws IOException
    {
        if (!series.isPopulated())
        {
            return false;
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
                        Precision.equals( readValue, series.getVariable().getNoDataValue(), EPSILON))
                    {
                        readValue = null;
                    }

                    this.addValue( series.getSourceInfo().getSiteCode()[0].getValue(),
                                   value.getDateTime(),
                                   readValue,
                                   sourceID);
                }
                catch ( SQLException e )
                {
                    throw new IOException( e );
                }
            }
        }

        return true;
    }

    private void performCopy() throws SQLException
    {
        if (this.copyCount > 0)
        {
            CopyExecutor copier = new CopyExecutor( USGSRegionSaver.COPY_HEADER, this.copyScript.toString(), DELIMITER );

            // TODO: If we want to only update the ProgressMonitor for files, remove these handlers
            // Tell the copier to increase the number representing the
            // total number of operations to perform when the thread starts.
            // It is debatable whether we should increase the number in this
            // thread or in the thread operating on the actual database copy
            // statement
            copier.setOnRun( ProgressMonitor.onThreadStartHandler() );

            // Tell the copier to inform the ProgressMonitor that work has been
            // completed when the thread has finished
            copier.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

            // Send the copier to the Database handler's task queue and add
            // the resulting future to our list of copy operations
            Database.ingest( copier );

            // Reset the values to copy
            this.copyScript = new ScriptBuilder(  );

            // Reset the count of values to copy
            this.copyCount = 0;

        }
    }

    void setOnUpdate( ExceptionalConsumer<TimeSeries, IOException> handler)
    {
        this.onUpdate = handler;
    }

    private void update( TimeSeries series) throws IOException
    {
        if ( this.onUpdate != null)
        {
            this.onUpdate.accept( series );
        }
    }

    private String startDate;
    private String endDate;
    private Integer variableID;
    private Map<String, Integer> variablePositionIDs;
    private final Collection<FeatureDetails> region;
    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
    private String operationStartTime;

    private ExceptionalConsumer<TimeSeries, IOException> onUpdate;

    // "00060" corresponds to a specific type of discharge. We need a list and
    // a way to transform user specifications about variable, time aggregation,
    // and measurement to find the correct code from the config
    private String parameterCode;

    private USGSParameters.USGSParameter parameter;

    private ScriptBuilder copyScript = null;
    private int copyCount = 0;

    @Override
    protected Logger getLogger()
    {
        return USGSRegionSaver.LOGGER;
    }
}
