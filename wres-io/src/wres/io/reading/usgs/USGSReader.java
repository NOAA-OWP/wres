package wres.io.reading.usgs;

import java.io.IOException;

import org.slf4j.Logger;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.slf4j.LoggerFactory;

import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.io.reading.BasicSource;
import wres.io.reading.usgs.waterml.Response;
import wres.util.Strings;

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
    }

    private void load()
    {
        try
        {
            Client client = ClientBuilder.newClient();
            WebTarget webTarget = client.target( USGS_URL );

            if ( this.dataSourceConfig.getExistingTimeAggregation().getUnit() == DurationUnit.INSTANT)
            {
                webTarget = webTarget.path( INSTANTANEOUS_VALUE );
            }
            else if (this.dataSourceConfig.getExistingTimeAggregation().getUnit() == DurationUnit.DAY)
            {
                webTarget = webTarget.path( DAILY_VALUE);
            }

            // Not necessary, but aids with debugging
            webTarget = webTarget.queryParam( "indent", "on" );

            // The current object tree supports JSON; additional work will
            // need to be done to support XML.
            webTarget = webTarget.queryParam( "format", "json" );
            webTarget = webTarget.queryParam( "sites", this.getGageID() );
            webTarget = webTarget.queryParam( "startDT", this.startDate );
            webTarget = webTarget.queryParam( "endDT", this.endDate );

            // We use "all" because we could theoretically need historical data
            // from now defunct sites
            webTarget = webTarget.queryParam( "siteStatus", "all" );
            webTarget = webTarget.queryParam( "parameterCd", this.getParameterCode() );

            Invocation.Builder invocationBuilder =
                    webTarget.request( MediaType.APPLICATION_JSON );

            this.response = invocationBuilder.get( Response.class );
        }
        catch (Exception e)
        {
            LOGGER.debug( "Response could not be grabbed." );
            throw e;
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

        return ID;
    }

    private String getParameterCode()
    {
        // Currently just returning what was in the config. Eventually, this
        // needs to be translated to the code without requiring the user to
        // know it up front. i.e. user says "discharge", the desired unit is
        // 'CFS', so it offers up "00060"
        return this.getDataSourceConfig().getVariable().getValue();
    }

    // This should be transformed from the feature table if the lid or anything
    // other than gageID is given
    private String gageID = "01646500";

    // If daily values were used, start and end date cannot have minutes or timezones
    // While these are hardcoded, they will need to be specified from the config
    // date must be separated by T, negative timezone will need to be separated by %2b
    private String startDate = "2017-08-01T00:00%2b0000";
    private String endDate = "2017-11-01T00:00%2b0000";

    // "00060" corresponds to a specific type of discharge. We need a list and
    // a way to transform user specifications about variable, time aggregation,
    // and measurement to find the correct code from the config
    private String parameterCode = "00060";

    // This should actually be "active"
    private String siteStatus = "all";
    private Response response;
}
