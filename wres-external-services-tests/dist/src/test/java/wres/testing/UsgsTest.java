package wres.testing;

import java.io.IOException;
import java.net.URI;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.reading.usgs.ogc.response.Response;
import wres.http.WebClient;
import wres.http.WebClientUtils;

public class UsgsTest
{
    private static final WebClient WEB_CLIENT = new WebClient( WebClientUtils.defaultTimeoutHttpClient() );
    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder()
                      .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true )
                      .build();
    private static final URI USGS_URI_ONE = URI.create(
            "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=json&monitoring_location_id=USGS-09165000&parameter_code=00060&time=2018-10-01T00:00:00Z/2018-10-07T23:59:59Z&limit=5" );
    private static final URI USGS_URI_TWO = URI.create(
            "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items?f=json&monitoring_location_id=USGS-01631000&parameter_code=00060&time=2020-03-01T00:00:00Z/2020-04-30T23:59:59Z&limit=5" );

    @Test
    void canGetMinimalResponseFromUsgsWithWebClient() throws IOException
    {
        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( USGS_URI_ONE ) )
        {
            assertAll( () -> assertTrue( response.getStatusCode() >= 200
                                         && response.getStatusCode() < 300,
                                         "Expected HTTP 2XX response." ),
                       () -> assertNotNull( response.getResponse(),
                                            "Expected an InputStream" )
            );
        }
    }


    @Test
    void canGetAndParseResponseFromUsgsWithWebClientAndJacksonPojos()
            throws IOException
    {
        // GET
        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( USGS_URI_TWO ) )
        {
            // Parse
            Response document = OBJECT_MAPPER.readValue( response.getResponse(),
                                                         Response.class );
            assertAll( () -> assertTrue( response.getStatusCode() >= 200
                                         && response.getStatusCode() < 300,
                                         "Expected HTTP 2XX response." ),
                       () -> assertTrue( document.getValue()
                                                 .getFeatures()
                                                 .length > 0,
                                         "Expected at least one TimeSeries" )
            );
        }
    }
}
