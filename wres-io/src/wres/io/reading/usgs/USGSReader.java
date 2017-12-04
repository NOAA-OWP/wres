package wres.io.reading.usgs;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.LoggerFactory;

import wres.config.generated.DurationUnit;
import wres.io.concurrency.StatementRunner;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.USGSParameters;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.VariableDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.waterml.Response;
import wres.io.reading.waterml.timeseries.TimeSeries;
import wres.io.reading.waterml.timeseries.TimeSeriesValue;
import wres.io.reading.waterml.timeseries.TimeSeriesValues;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.Time;

public class USGSReader extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( USGSReader.class );

    // Hardcoded because this is the reader for USGS' services, not a generic waterml service
    // If a generic water ML service is required, one should be written for it.
    private static final String USGS_URL = "https://waterservices.usgs.gov/nwis/";
    private static final String INSTANTANEOUS_VALUE = "iv";
    private static final String DAILY_VALUE = "dv";

    private static final String EARLIEST_DATE = "epoch";
    private static final String LATEST_DATE = "now";
    /**
     * Epsilon value used to test floating point equivalency
     */
    private static final double EPSILON = 0.0000001;

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

    private class UpsertValue
    {
        public UpsertValue(String gageID, String observationTime, Double value)
        {
            this.gageID = gageID;
            this.observationTime = observationTime;
            this.value = value;
        }

        public Object[] getParameters() throws SQLException
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
    public List<String> saveObservation() throws IOException
    {
        this.operationStartTime = Time.convertDateToString( OffsetDateTime.now() );
        this.load();

        if (this.response == null ||
            wres.util.Collections.exists(
                    this.response.getValue().getTimeSeries(), (TimeSeries series) ->
                            series.getValues() == null || series.getValues().length == 0 ) )
        {
            throw new IOException( "No USGS data could be loaded with the given configuration." );
        }

        List<String> foundLocations = new ArrayList<>(  );

        for (TimeSeries series : this.response.getValue().getTimeSeries())
        {
            foundLocations.add(series.getSourceInfo().getSiteCode()[0].getValue());
            this.readSeries( series );
        }

        try
        {
            enforceValidFeatures( foundLocations );
        }
        catch ( SQLException e )
        {
            throw new IOException( "Locations without official data from the " +
                                   "USGS could not be removed from the total " +
                                   "list of locations to evaluate.", e );
        }

        try
        {
            this.performUpserts();
        }
        catch ( SQLException e )
        {
            throw new IOException( "USGS observations could not be saved.", e );
        }

        try
        {
            this.getProjectDetails().addSource( this.getHash(), this.getDataSourceConfig() );
        }
        catch ( SQLException e )
        {
            throw new IOException( "USGS observations could not be linked to this project.", e);
        }

        List<String> result = new ArrayList<>( 1 );
        result.add( this.getHash() );
        return Collections.unmodifiableList( result );
    }

    private void enforceValidFeatures(List<String> foundLocations)
            throws SQLException
    {
        List<FeatureDetails> invalidFeatures = new ArrayList<>();

        for (FeatureDetails details : this.getProjectDetails().getFeatures())
        {
            if (!foundLocations.contains( details.getGageID() ))
            {
                invalidFeatures.add( details );
            }
        }

        if (invalidFeatures.size() > 0)
        {
            this.getProjectDetails().getFeatures().removeAll( invalidFeatures );
        }
    }

    private void load() throws IOException
    {
        String requestURL = null;
        Client client = null;
        try
        {
            client = ClientBuilder.newClient();
            WebTarget webTarget = client.target( USGS_URL );

            if (this.dataSourceConfig.getExistingTimeAggregation() == null)
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
            webTarget = webTarget.queryParam( "sites", this.getGageID() );
            webTarget = webTarget.queryParam( "startDT", this.getStartDate() );
            webTarget = webTarget.queryParam( "endDT", this.getEndDate() );

            // We use "all" because we could theoretically need historical data
            // from now defunct sites
            webTarget = webTarget.queryParam( "siteStatus", "all" );
            webTarget = webTarget.queryParam( "parameterCd", this.getParameterCode());

            requestURL = webTarget.getUri().toURL().toString();

            Invocation.Builder invocationBuilder =
                    webTarget.request( MediaType.APPLICATION_JSON );

            this.response = invocationBuilder.get( Response.class );
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
            LOGGER.error( "The desired parameter could not be determined." );
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
    }

    // This is specifically for gages; we also need to support selecting by
    // latitude and longitude (WGS84; that is what is used by USGS)
    // TODO: It may make sense to do a gage at a time in order to fire off shorter requests more often
    private String getGageID() throws SQLException, NoDataException
    {
        String ID = null;

        if (this.getSpecifiedFeatures().size() > 0)
        {
            for (FeatureDetails feature : this.getProjectDetails().getFeatures())
            {
                if ( Strings.hasValue( feature.getGageID()) && !Strings.hasValue( ID ))
                {
                    ID = feature.getGageID();
                }
                else if (Strings.hasValue( feature.getGageID() ))
                {
                    ID += "," + feature.getGageID();
                }
            }
        }

        if (!Strings.hasValue( ID ))
        {
            throw new NoDataException( "No valid gageIDs could be found. USGS data could not be ingested." );
        }

        return ID;
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
        return this.getParameter().getParameterCode();
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
            else if (variableName.matches( "\\d{5}" ))
            {
                this.parameter = USGSParameters.getParameterByCode( variableName );
            }
            else
            {
                String unit;
                String aggregation;

                if ( this.getDataSourceConfig().getVariable().getUnit()
                     != null )
                {
                    unit = this.getDataSourceConfig().getVariable().getUnit();
                }
                else
                {
                    unit = this.getProjectDetails().getDesiredMeasurementUnit();

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

                if ( this.getDataSourceConfig()
                         .getExistingTimeAggregation()
                         .getUnit() == DurationUnit.INSTANT )
                {
                    aggregation = "instant";
                }
                else
                {
                    aggregation = this.getDataSourceConfig()
                                      .getExistingTimeAggregation()
                                      .getFunction()
                                      .value();
                }

                if ( unit != null )
                {
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

            if (this.dataSourceConfig.getExistingTimeAggregation() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            if (this.getProjectDetails().getEarliestDate() == null)
            {
                this.startDate = EARLIEST_DATE;
            }
            else
            {
                this.startDate = this.getProjectDetails().getEarliestDate();
            }

            this.startDate = Time.normalize( this.startDate );

            switch (this.dataSourceConfig.getExistingTimeAggregation().getUnit())
            {
                case DAY:
                    // No time or time zone information is allowed
                    this.startDate = Time.convertStringDateTimeToDate( this.startDate );
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
            if (this.dataSourceConfig.getExistingTimeAggregation() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            if (this.getProjectDetails().getEarliestDate() == null)
            {
                this.endDate = LATEST_DATE;
            }
            else
            {
                this.endDate = this.getProjectDetails().getLatestDate();
            }

            this.endDate = Time.normalize( this.endDate );

            switch (this.dataSourceConfig.getExistingTimeAggregation().getUnit())
            {
                case DAY:
                    // No time or time zone information is allowed
                    this.endDate = Time.convertStringDateTimeToDate( this.endDate );
                    break;
                default:
                    // The space inbetween the date and time needs to be split with a T
                    this.endDate = this.endDate.replaceAll( " ", "T" );
                    break;
            }
        }
        return this.endDate;
    }

    private String getValueType()
    {
        String valueType;

        if (this.dataSourceConfig.getExistingTimeAggregation().getUnit() == DurationUnit.DAY)
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

        FeatureDetails details = null;

        if (!this.variablePositionIDs.containsKey( gageID ))
        {
            try
            {
                details =
                        Collections.find( this.getProjectDetails()
                                              .getFeatures(),
                                          feature -> feature.getGageID() != null
                                                     &&
                                                     feature.getGageID()
                                                            .equalsIgnoreCase(
                                                                    gageID ) );
            FeatureDetails details =
                    wres.util.Collections.find( this.getProjectDetails().getFeatures(),
                                                feature -> feature.getGageID() != null &&
                                                           feature.getGageID().equalsIgnoreCase( gageID ) );

                this.variablePositionIDs.put( gageID,
                                              details.getVariablePositionID(
                                                      this.getVariableID() ) );
            }
            catch (Exception e)
            {
                LOGGER.debug( Strings.getStackTrace( e ) );
            }
        }

        return this.variablePositionIDs.get( gageID );
    }

    private Integer getVariableID() throws SQLException
    {
        if (this.variableID == null)
        {
            if (this.getProjectDetails().getLeft().equals( this.getDataSourceConfig() ))
            {
                this.variableID = this.getProjectDetails().getLeftVariableID();
            }
            else if (this.getProjectDetails().getRight().equals( this.getDataSourceConfig() ))
            {
                this.variableID = this.getProjectDetails().getRightVariableID();
            }
            else
            {
                this.variableID = this.getProjectDetails().getBaselineVariableID();
            }
        }

        return this.variableID;
    }

    private void addValue(String gageID, String observationTime, Double value)
            throws SQLException
    {
        this.upsertValues.add(
                new UpsertValue( gageID,
                                 Time.normalize( observationTime ),
                                 value )
        );

        if ( this.upsertValues.size() >= SystemSettings.maximumDatabaseInsertStatements())
        {
            this.performUpserts();
        }
    }

    private void readSeries(TimeSeries series) throws IOException
    {
        String gageID = series.getSourceInfo().getSiteCode()[0].getValue();

        if (series.getValues().length == 0)
        {
            FeatureDetails invalidFeature = null;

            try
            {
                for (FeatureDetails feature : this.getProjectDetails().getFeatures())
                {
                    if (Strings.hasValue( feature.getGageID() ) && feature.getGageID().equalsIgnoreCase( gageID ))
                    {
                        invalidFeature = feature;
                        break;
                    }
                }

                this.getProjectDetails().getFeatures().remove( invalidFeature );
                return;
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
                FeatureDetails invalidFeature = null;

                try
                {
                    for (FeatureDetails feature : this.getProjectDetails().getFeatures())
                    {
                        if (Strings.hasValue( feature.getGageID() ) && feature.getGageID().equalsIgnoreCase( gageID ))
                        {
                            invalidFeature = feature;
                            break;
                        }
                    }

                    this.getProjectDetails().getFeatures().remove( invalidFeature );
                    return;
                }
                catch ( SQLException e )
                {
                    throw new IOException( "Could not iterate through available " +
                                           "features in order to avoid processing " +
                                           "the invalid location '" + gageID + "'" ,
                                           e);
                }
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
        }
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
            statement.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            Database.ingest(statement);

            ProgressMonitor.increment();
        }
    }

    @Override
    public String getHash()
    {
        if (this.hash == null)
        {
            this.hash = Strings.getMD5Checksum(
                    (USGSReader.USGS_URL + "/" + getValueType() ).getBytes()
            );
        }
        return this.hash;
    }

    public Integer getSourceID() throws SQLException
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
    private Response response;

    private String operationStartTime;
}
