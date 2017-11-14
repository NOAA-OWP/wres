package wres.io.reading.usgs;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.LoggerFactory;

import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.USGSParameters;
import wres.io.data.caching.Variables;
import wres.io.data.details.VariableDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.usgs.waterml.Response;
import wres.io.reading.usgs.waterml.timeseries.TimeSeries;
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

    @Override
    public void saveObservation() throws IOException
    {
        this.load();

        if (this.response == null && this.response.getValue().getTimeSeries().length == 0)
        {
            throw new IOException( "USGS data could not be loaded and ingested." );
        }

        for (TimeSeries series : this.response.getValue().getTimeSeries())
        {

        }
    }

    private void load() throws IOException
    {
        String requestURL = null;
        try
        {
            Client client = ClientBuilder.newClient();
            WebTarget webTarget = client.target( USGS_URL );

            if (this.dataSourceConfig.getExistingTimeAggregation() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            switch (this.dataSourceConfig.getExistingTimeAggregation().getUnit())
            {
                case INSTANT:
                    webTarget = webTarget.path( INSTANTANEOUS_VALUE );
                    break;
                case DAY:
                    webTarget = webTarget.path( DAILY_VALUE);
                    break;
                default:
                    throw new IOException( "An aggregation type of '" +
                                           this.dataSourceConfig.getExistingTimeAggregation().getUnit() +
                                           "' is not currently allowable for USGS data." );
            }

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
            webTarget = webTarget.queryParam( "parameterCd", this.getParameter().getParameterCode());

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

        LOGGER.info("TimeSeres has been downloaded from USGS.");
    }

    // This is specifically for gages; we also need to support selecting by
    // latitude and longitude (WGS84; that is what is used by USGS)
    private String getGageID()
    {
        String ID = null;

        if (this.getSpecifiedFeatures().size() > 0)
        {
            for (Feature feature : this.getSpecifiedFeatures())
            {
                if ( Strings.hasValue(feature.getGageId()) && !Strings.hasValue( ID ))
                {
                    ID = feature.getGageId();
                }
                else if (Strings.hasValue( feature.getGageId() ))
                {
                    ID += "," + feature.getGageId();
                }
            }
        }

        Objects.requireNonNull( ID, "No valid gageIDs could be found." );

        return ID;
    }

    private String getParameterCode() throws SQLException, IngestException
    {
        return this.getParameter().getParameterCode();
        // Currently just returning what was in the config. Eventually, this
        // needs to be translated to the code without requiring the user to
        // know it up front. i.e. user says "discharge", the desired unit is
        // 'CFS', so it offers up "00060"
        //return this.getDataSourceConfig().getVariable().getValue();
    }

    private USGSParameters.USGSParameter getParameter()
            throws SQLException, IngestException
    {
        if (this.parameter == null)
        {
            String variableName = this.getDataSourceConfig().getVariable().getValue();
            String unit = null;

            if (this.getDataSourceConfig().getVariable().getUnit() != null)
            {
                unit = this.getDataSourceConfig().getVariable().getUnit();
            }
            else
            {
                VariableDetails
                        currentDetails = Variables.getByName( variableName );

                if (currentDetails != null && currentDetails.measurementunitId != null)
                {
                    unit = MeasurementUnits.getNameByID( currentDetails.measurementunitId );
                }
            }

            if (unit != null)
            {
                this.parameter =  USGSParameters.getParameter( variableName,
                                                               unit,
                                                               this.getDataSourceConfig()
                                                                   .getExistingTimeAggregation()
                                                                   .getFunction()
                                                                   .value() );
            }
            else
            {
                String message = "Not enough information was supplied to find " +
                                 "the requested USGS values. A variable name and " +
                                 "measurement are both required to find the right" +
                                 "data.";
                throw new IngestException( message );
            }
        }

        return this.parameter;
    }

    private String getStartDate() throws IOException
    {
        if (this.startDate == null)
        {
            Objects.requireNonNull( this.getProjectDetails().getEarliestDate(),
                                    "A start date must be defined for USGS data." );

            if (this.dataSourceConfig.getExistingTimeAggregation() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            switch (this.dataSourceConfig.getExistingTimeAggregation().getUnit())
            {
                case INSTANT:
                    this.startDate = Time.normalize( this.getProjectDetails().getEarliestDate());
                    // The space inbetween the date and time needs to be split with a T
                    this.startDate = this.startDate.replaceAll( " ", "T" );
                    break;
                case DAY:
                    // No time or time zone information is allowed
                    this.startDate = Time.convertStringDateTimeToDate( this.startDate );
                    break;
                default:
                    throw new IOException( "An aggregation type of '" +
                                           this.dataSourceConfig.getExistingTimeAggregation().getUnit() +
                                           "' is not currently allowable for USGS data." );
            }
        }
        return this.startDate;
    }

    private String getEndDate() throws IOException
    {
        if (this.endDate == null)
        {
            Objects.requireNonNull( this.getProjectDetails().getLatestDate(),
                                    "An end date must be defined for USGS data." );

            if (this.dataSourceConfig.getExistingTimeAggregation() == null)
            {
                throw new IOException( "An existing time aggregation must be defined to ingest USGS data." );
            }

            switch (this.dataSourceConfig.getExistingTimeAggregation().getUnit())
            {
                case INSTANT:
                    this.endDate = Time.normalize( this.getProjectDetails().getLatestDate());
                    // The space inbetween the date and time needs to be split with a T
                    this.endDate = this.endDate.replaceAll( " ", "T" );
                    break;
                case DAY:
                    // No time or time zone information is allowed
                    this.endDate = Time.convertStringDateTimeToDate( this.endDate );
                    break;
                default:
                    throw new IOException( "An aggregation type of '" +
                                           this.dataSourceConfig.getExistingTimeAggregation().getUnit() +
                                           "' is not currently allowable for USGS data." );
            }
        }
        return this.endDate;
    }

    private void readSeries(TimeSeries series)
    {
        return;
    }

    // This should be transformed from the feature table if the lid or anything
    // other than gageID is given
    private String gageID = "01646500";

    // If daily values were used, start and end date cannot have minutes or timezones
    // While these are hardcoded, they will need to be specified from the config
    // date must be separated by T, negative timezone will need to be separated by %2b
    //private String startDate = "2017-08-01T00:00%2b0000";
    //private String endDate = "2017-11-01T00:00%2b0000";

    private String startDate;
    private String endDate;

    // "00060" corresponds to a specific type of discharge. We need a list and
    // a way to transform user specifications about variable, time aggregation,
    // and measurement to find the correct code from the config
    private String parameterCode = "00060";

    private USGSParameters.USGSParameter parameter;

    // This should actually be "active"
    private String siteStatus = "all";
    private Response response;
}
