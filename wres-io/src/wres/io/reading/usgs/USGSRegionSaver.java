package wres.io.reading.usgs;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DurationUnit;
import wres.config.generated.ProjectConfig;
import wres.datamodel.scale.TimeScale;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.caching.USGSParameters;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.PreIngestException;
import wres.io.reading.SourceCompleter;
import wres.io.reading.waterml.Response;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.reading.waterml.variable.Variable;
import wres.system.DatabaseLockManager;
import wres.io.utilities.OutOfAttemptsException;
import wres.io.utilities.WebRetryStrategy;
import wres.util.functional.ExceptionalConsumer;
import wres.io.utilities.NoDataException;
import wres.util.FormattedStopwatch;
import wres.util.Strings;
import wres.util.TimeHelper;

public class USGSRegionSaver extends WRESCallable<IngestResult>
{
    
    /**
     * Enumeration of NWIS endpoints.
     */
    
    private enum USGSEndpoint
    {
        /**
         * Instantaneous values endpoint.
         */

        INSTANTANEOUS_VALUE,

        /**
         * Daily values endpoint.
         */

        DAILY_VALUE;

        /**
         * String representation of the endpoint.
         * 
         * @return a string for use within a URI
         */
        @Override
        public String toString()
        {
            if ( this == INSTANTANEOUS_VALUE )
            {
                return "iv";
            }
            else
            {
                return "dv";
            }
        }
    }
    
    private static final Logger LOGGER = LoggerFactory.getLogger(USGSRegionSaver.class);

    // Hardcoded because this is the reader for USGS' services, not a generic waterml service
    // If a generic water ML service is required, one should be written for it.
    private static final URI USGS_URL =
            URI.create( "https://waterservices.usgs.gov/nwis/" );

    private static final double EPSILON = 0.0000001;

    private static final int RETRY_COUNT = 5;
    private static final Duration RETRY_WAIT = Duration.of(5, ChronoUnit.SECONDS);

    private static final String EARLIEST_DATE = "2008-01-01T00:00:00Z";
    
    /**
     * Endpoint used by this instance.
     */
    
    private final USGSEndpoint endpoint;

    private String startDate;
    private String endDate;
    private Integer variableID;
    private Map<String, Integer> variableFeatureIDs;
    private final Collection<FeatureDetails> region;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();
    private String operationStartTime;
    private URI requestURL;

    private ExceptionalConsumer<TimeSeries, IOException> onUpdate;

    // "00060" corresponds to a specific type of discharge. We need a list and
    // a way to transform user specifications about variable, time aggregation,
    // and measurement to find the correct code from the config
    private String parameterCode;

    private USGSParameters.USGSParameter parameter;

    // There's a chance this operation will output the time in the wrong format
    private static final String LATEST_DATE = TimeHelper.convertDateToString(
            OffsetDateTime.now( ZoneId.of( "UTC" ) )
    );

    private static class WebResponse
    {
        private WebResponse(final Response usgsResponse,
                            final boolean alreadyRequested,
                            final int sourceId,
                            final String hash)
        {
            this.usgsResponse = usgsResponse;
            this.alreadyRequested = alreadyRequested;
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

        private final Response usgsResponse;
        private final boolean alreadyRequested;
        private final int sourceId;
        private final String hash;
    }

    /**
     * Construct an instance that points to the {@link USGSEndpoint#INSTANTANEOUS_VALUE} endpoint.
     * 
     * @param region the region to consider
     * @param projectConfig the project declaration
     * @param dataSource the data source declaration
     * @param lockManager the lock manager to use
     * @throws NullPointerException if any input is null
     */

    USGSRegionSaver( final Collection<FeatureDetails> region,
                     final ProjectConfig projectConfig,
                     DataSource dataSource,
                     DatabaseLockManager lockManager )
    {
        this( region,
              projectConfig,
              dataSource,
              USGSEndpoint.INSTANTANEOUS_VALUE,
              lockManager );
    }

    /**
     * Construct an instance that points to a prescribed endpoint.
     * 
     * @param region the region to consider
     * @param projectConfig the project declaration
     * @param dataSource the data source information
     * @param endpoint the service endpoint
     * @param lockManager the lock manager to use
     * @throws NullPointerException if any input is null
     */

    USGSRegionSaver( final Collection<FeatureDetails> region,
                     final ProjectConfig projectConfig,
                     DataSource dataSource,
                     final USGSEndpoint endpoint,
                     DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( region );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( endpoint );
        Objects.requireNonNull( lockManager );

        this.region = region;
        this.dataSource = dataSource;
        this.projectConfig = projectConfig;
        this.endpoint = endpoint;
        this.lockManager = lockManager;
    }

    @Override
    protected IngestResult execute() throws IOException
    {
        // This is saved as the output time for the source
        this.operationStartTime = TimeHelper.convertDateToString( OffsetDateTime.now() );

        WebResponse response;
        try
        {
            // Request observation data from USGS
            response = this.load();
        }
        catch ( IngestException ie )
        {
            String errorString = String.format( "A USGS Request failed. The URL was: %s", this.requestURL );
            throw new IngestException( errorString, ie );
        }

        LOGGER.debug("NWIS Data was loaded from {}", this.requestURL);

        // If the WebResponse object didn't return anything, it means that there was nothing to fail on,
        // whereas failing below means data could not be parsed
        if (response == null)
        {
            return null;
        }

        IngestResult result = null;

        DataSource innerDataSource = DataSource.of( this.dataSource.getSource(),
                                                    this.dataSource.getContext(),
                                                    this.dataSource.getLinks(),
                                                    this.requestURL );
        if (response.wasAlreadyRequested())
        {
            result = IngestResult.from( this.projectConfig,
                                        innerDataSource,
                                        response.hash,
                                        true );
            LOGGER.debug("We've already seen this data before.");
        }
        else
        {
            Response usgsResponse = response.getUsgsResponse();

            // Since we're sending many requests instead of one, there's a
            // chance a single request might not values but requests before
            // and after will. In that case, we should continue.
            if (usgsResponse.getValue().getNumberOfPopulatedTimeSeries() == 0)
            {
                LOGGER.debug( "No timeseries were returned from the query: {}",
                              requestURL );
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
                                                innerDataSource,
                                                response.hash,
                                                false );

                    LOGGER.debug( "Data for {} different locations have been saved.",
                                  amountSaved );
                }
                else
                {
                    LOGGER.info("No data from {} was saved.", this.requestURL );
                }
            }
        }

        // Throw an error if nothing was saved
        if (result == null)
        {
            throw new IngestException( "No data from any USGS features could "
                                       + "be saved for evaluation." );
        }

        if ( !response.alreadyRequested )
        {
            SourceCompleter sourceCompleter =
                    new SourceCompleter( response.sourceId, this.getLockManager() );
            sourceCompleter.complete( this.latches );
        }

        return result;
    }
    
    private WebResponse load() throws IngestException
    {
        Client client = null;

        try
        {
            client = ClientBuilder.newClient();
            WebTarget webTarget = this.buildWebTarget( client );

            requestURL = webTarget.getUri();

            LOGGER.debug("Requesting data from: {}", requestURL);

            Response usgsResponse = this.getResponse( webTarget );

            if ( usgsResponse == null )
            {
                throw new PreIngestException( "A request to USGS url "
                                              + requestURL
                                              + " returned 'success' but had an"
                                              + " empty body. "
                                              + "Please contact USGS at: https://water.usgs.gov/contact/gsanswers" );
            }

            LOGGER.trace( "A valid USGS response was encountered for url {}",
                          requestURL );

            String responseHash = Strings.getMD5Checksum( usgsResponse );

            SourceDetails.SourceKey sourceKey =
                    new SourceDetails.SourceKey( requestURL,
                                                 operationStartTime,
                                                 null,
                                                 responseHash );

            return this.createWebResponse( usgsResponse, sourceKey, responseHash );
        }
        catch ( IOException e)
        {
            String message = "Data from the location '"
                             + requestURL
                             + "' could not be retrieved.";
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
    }
    
    /**
     * Creates a {@link WebResponse} from the input.
     * @param usgsResponse the usgsResponse
     * @param sourceKey the source key
     * @param responseHash the hash of the body
     * @return the web response
     * @throws IOException if the resnpose could not be created
     */
    
    private WebResponse createWebResponse( Response usgsResponse, SourceDetails.SourceKey sourceKey, String responseHash )
            throws IOException
    {
        try
        {

            if ( DataSources.isCached( sourceKey ) )
            {
                LOGGER.debug( "The data for '{}' had been previously ingested.", requestURL );
                return new WebResponse( usgsResponse,
                                        true,
                                        DataSources.getActiveSourceID( responseHash ),
                                        responseHash );
            }

            SourceDetails usgsDetails = new SourceDetails( sourceKey );
            usgsDetails.save();

            int sourceId = usgsDetails.getId();

            if ( usgsDetails.performedInsert() )
            {
                // Signal that this source is currently under ingest.
                this.getLockManager()
                    .lockSource( sourceId );
            }
            else
            {
                LOGGER.debug( "The data for '{}' had been previously ingested.", requestURL );
            }

            return new WebResponse( usgsResponse,
                                    !usgsDetails.performedInsert(),
                                    sourceId,
                                    responseHash );
        }
        catch ( SQLException e )
        {
            throw new IOException( "Source information about the requested "
                                   + "USGS data could not be found.",
                                   e );
        }
    }

    private WebTarget buildWebTarget(final Client client) throws IngestException
    {
        String earliestDate = this.getStartDate();
        String latestDate = this.getEndDate();
        String gageStatement;
        String parCode;

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
            parCode = this.getParameterCode();
        }
        catch ( SQLException e )
        {
            throw new IngestException( "The USGS parameter code could not be "
                                       + "determined.", e );
        }

        WebTarget webTarget = client.target( USGS_URL );
        // Determines if we use the daily REST service or the instantaneous REST service
        webTarget = webTarget.path( this.getEndpoint().toString() );

        // The current object tree supports JSON; additional work will
        // need to be done to support XML.
        webTarget = webTarget.queryParam( "format", "json" );
        webTarget = webTarget.queryParam( "sites", gageStatement );
        webTarget = webTarget.queryParam( "startDT", earliestDate );
        webTarget = webTarget.queryParam( "endDT", latestDate );

        // We use "all" because we could theoretically need historical data
        // from now defunct sites
        webTarget = webTarget.queryParam( "siteStatus", "all" );
        webTarget = webTarget.queryParam( "parameterCd", parCode);

        return webTarget;
    }

    private Response getResponse(final WebTarget webTarget) throws OutOfAttemptsException
    {
        WebRetryStrategy strategy = new WebRetryStrategy( RETRY_COUNT, RETRY_WAIT );

        Invocation.Builder invocationBuilder =
                webTarget.request( MediaType.APPLICATION_JSON );

        FormattedStopwatch stopwatch = null;

        if (LOGGER.isDebugEnabled())
        {
            stopwatch = new FormattedStopwatch();
            stopwatch.start();
        }

        Response usgsResponse =  strategy.execute( invocationBuilder::get, Response.class );

        if (LOGGER.isDebugEnabled() && stopwatch != null)
        {
            stopwatch.stop();

            LOGGER.debug( "It took {} to download the USGS data.",
                          stopwatch.getFormattedDuration() );
        }

        return usgsResponse;
    }

    /**
     * @throws NoDataException when gage ids cannot be found
     */

    private String getGageIdParameter( Collection<FeatureDetails> features )
    {
        StringBuilder parameterName = new StringBuilder();

        for ( FeatureDetails feature : features )
        {
            String gageID = feature.getGageID();

            if ( !StringUtils.isNumeric( gageID ) )
            {
                LOGGER.warn( "Invalid USGS gageID {} will not be used from feature {}.",
                             gageID,
                             feature );
                continue;
            }

            if ( Strings.hasValue( gageID ) && !Strings.hasValue( parameterName.toString() ) )
            {
                parameterName.append( gageID );
            }
            else if ( Strings.hasValue( gageID ) )
            {
                parameterName.append( "," ).append( gageID );
            }
        }

        if ( !Strings.hasValue( parameterName.toString() ) )
        {
            throw new NoDataException( "No valid gageIDs could be found. USGS data could not be ingested." );
        }

        return parameterName.toString();
    }

    private boolean hasDiscreteParameterCode()
    {
        // TODO: '\\d{5}' will probably match on "My Variable: 9238238942397' since it has 5 digits
        // Test with "^\\d{5}$", meaning have the string contain ONLY those 5 digits
        return this.dataSource.getVariable()
                              .getValue()
                              .matches( "\\d{5}" );
    }

    private String getParameterCode() throws SQLException, IngestException
    {
        if (this.parameterCode == null)
        {
            // If the user uses the explicit USGS code, use that and bypass everything else
            if (this.hasDiscreteParameterCode())
            {
                // If this is new, parameter will be set to null, but we still
                // get the parameter code
                this.parameter = USGSParameters.getParameterByCode(
                        this.dataSource.getVariable()
                                       .getValue()
                );
                this.parameterCode = this.dataSource.getVariable()
                                                    .getValue();
            }
            else
            {
                this.parameter = this.getParameter();
                this.parameterCode = this.parameter.getParameterCode();
            }
        }
        return this.parameterCode;
    }

    private USGSParameters.USGSParameter getParameter()
            throws SQLException, IngestException
    {
        if (this.parameter == null)
        {
            String variableName = this.dataSource.getVariable()
                                                 .getValue();

            // If someone enters a variable of the form "Discharge, cubic feet per second",
            // we should go ahead and try to find the data by the description of the variable
            if (variableName.matches( ".+,.+" ))
            {
                this.parameter = USGSParameters.getParameterByDescription( variableName );
            }
            else
            {
                String unit = this.dataSource.getVariable()
                                             .getUnit();

                if (unit != null && this.dataSource.getContext()
                                                   .getExistingTimeScale() == null)
                {
                    this.parameter = USGSParameters.getParameter( variableName, unit );
                }
                else if ( unit != null )
                {
                    String aggregation;
                    if ( this.dataSource.getContext()
                                        .getExistingTimeScale()
                                        .getUnit() == DurationUnit.SECONDS )
                    {
                        aggregation = "instant";
                    }
                    else
                    {
                        aggregation = this.dataSource.getContext()
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

    private String getStartDate()
    {
        if (this.startDate == null)
        {
            Instant earliest = ConfigHelper.getEarliestDateTimeFromDataSources( this.projectConfig );
            if ( earliest == null)
            {
                this.startDate = EARLIEST_DATE;
            }
            else
            {
                this.startDate = earliest.toString();
            }

            if ( this.dataSource.getContext()
                                .getExistingTimeScale() != null
                 && this.dataSource.getContext()
                                   .getExistingTimeScale()
                                   .getUnit() == DurationUnit.DAYS )
            {
                this.startDate = TimeHelper.convertStringDateTimeToDate( this.startDate );
            }
            else
            {
                // The space inbetween the date and time needs to be split with a T
                this.startDate = this.startDate.replaceAll( " ", "T" );
            }
        }
        return this.startDate;
    }

    private String getEndDate()
    {
        if (this.endDate == null)
        {
            Instant latest = ConfigHelper.getLatestDateTimeFromDataSources( this.projectConfig );
            if ( latest == null )
            {
                this.endDate = LATEST_DATE;
            }
            else
            {
                this.endDate = latest.toString();
            }


            if ( this.dataSource.getContext()
                                .getExistingTimeScale() != null
                 && this.dataSource.getContext()
                                   .getExistingTimeScale()
                                   .getUnit() == DurationUnit.DAYS )
            {
                // No time or time zone information is allowed
                this.endDate = TimeHelper.convertStringDateTimeToDate( this.endDate );
            }
            else
            {

                // USGS is [inclusive, exclusive) where we need
                // [inclusive, inclusive]. We add time to ensure that we
                // have all the data we need
                OffsetDateTime dateTime = OffsetDateTime.parse( this.endDate );
                dateTime = dateTime.withOffsetSameInstant( ZoneOffset.UTC );

                if ( this.dataSource.getContext()
                                    .getExistingTimeScale() != null )
                {
                    dateTime = dateTime.plus(
                            this.dataSource.getContext()
                                           .getExistingTimeScale()
                                           .getPeriod(),
                            ChronoUnit.valueOf(
                                    this.dataSource.getContext()
                                                   .getExistingTimeScale()
                                                   .getUnit()
                                                   .value()
                                                   .toUpperCase()
                            )
                    );
                }
                else
                {
                    dateTime = dateTime.plus(1L, ChronoUnit.HOURS);
                }

                this.endDate = TimeHelper.convertDateToString( dateTime );
            }
        }
        return this.endDate;
    }

    /**
     * @return the USGS endpoint used by this saver.
     */
    
    private USGSEndpoint getEndpoint()
    {
        return this.endpoint;
    }

    private Integer getVariableFeatureID(String gageID) throws SQLException
    {
        if (this.variableFeatureIDs == null)
        {
            this.variableFeatureIDs = new TreeMap<>(  );
        }

        FeatureDetails details;

        if (!this.variableFeatureIDs.containsKey( gageID ))
        {
            details =
                    wres.util.Collections.find( this.region,
                                                feature ->
                                                        feature.getGageID()
                                                        != null &&
                                                        feature.getGageID()
                                                               .equalsIgnoreCase(
                                                                       gageID ) );

            this.variableFeatureIDs.put( gageID,
                                          Features.getVariableFeatureByFeature(
                                                  details,
                                                  this.getVariableID()
                                          )
            );
        }

        return this.variableFeatureIDs.get( gageID );
    }

    private Integer getVariableID() throws SQLException
    {
        if (this.variableID == null)
        {
            this.variableID = Variables.getVariableID( this.dataSource.getVariable()
                                                                      .getValue() );
        }

        return this.variableID;
    }

    private void addValue(String gageID, Instant observationTime, Double value, Duration timeStep, int sourceID)
            throws SQLException, IngestException
    {
        TimeScale.TimeScaleFunction function = TimeScale.TimeScaleFunction.UNKNOWN;
        Duration period = null;

        // IV service endpoint only returns instantaneous values: #60265
        if( this.getEndpoint() == USGSEndpoint.INSTANTANEOUS_VALUE )
        {
            period = Duration.ofMinutes( 1 );
        }
        else if (this.getParameter().getAggregation().equalsIgnoreCase( "sum" ))
        {
            period = timeStep;
            function = TimeScale.TimeScaleFunction.TOTAL;
        }
        else if (this.getParameter().getAggregation().equalsIgnoreCase( "min" ))
        {
            period = timeStep;
            function = TimeScale.TimeScaleFunction.MINIMUM;
        }
        else if (this.getParameter().getAggregation().equalsIgnoreCase( "max" ))
        {
            period = timeStep;
            function = TimeScale.TimeScaleFunction.MAXIMUM;
        }

        if (timeStep == null)
        {
            LOGGER.info("{}{}{}", NEWLINE, this.requestURL, NEWLINE );
        }

        Pair<CountDownLatch,CountDownLatch> synchronizer =
                IngestedValues.observed(value)
                              .at( observationTime )
                              .forVariableAndFeatureID( this.getVariableFeatureID( gageID ) )
                              .measuredIn( this.getParameter().getMeasurementUnitID() )
                              .inSource( sourceID )
                              .scaleOf( period )
                              .scaledBy( function )
                              .add();
        this.latches.add( synchronizer );
    }

    private int saveResponse(Response usgsResponse, int sourceID) throws IOException
    {
        int readSeriesCount = 0;

        for ( TimeSeries series : usgsResponse.getValue().getTimeSeries() )
        {
            if (this.parameter == null)
            {
                // If this is a new variable gotten via a parameter code, we
                // won't have variable information yet. As a result, we need
                // to get it from the response.
                Variable responseVariable = series.getVariable();

                try
                {
                    this.parameter = USGSParameters.addRequestedParameter(
                            responseVariable.getVariableName(),
                            responseVariable.getVariableCode()[0].getValue(),
                            responseVariable.getVariableDescription(),
                            responseVariable.getUnit().getUnitCode() );
                }
                catch ( SQLException e )
                {
                    throw new IOException( "New variable metadata could not be saved.", e );
                }
            }

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
                                   valueSet.getTimeStep(),
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

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    @Override
    protected Logger getLogger()
    {
        return USGSRegionSaver.LOGGER;
    }
}
