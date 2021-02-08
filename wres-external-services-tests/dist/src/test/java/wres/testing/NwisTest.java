package wres.testing;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.reading.waterml.Response;
import wres.io.utilities.WebClient;

public class NwisTest
{
    private static final WebClient WEB_CLIENT = new WebClient();
    // Use an ObjectMapper like what is used in WaterMLBasicSource
    private static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );
    private static final URI NWIS_URI_ONE = URI.create( "https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&indent=on&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:00Z&endDT=2018-10-07T23:59:59Z" );
    private static final URI NWIS_URI_TWO = URI.create( "https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&indent=on&sites=01631000&parameterCd=00060&startDT=2020-03-01T00:00:00Z&endDT=2020-04-30T23:59:59Z" );

    @Test
    void canGetMinimalResponseFromNwisWithWebClient() throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();

        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( NWIS_URI_ONE,
                                                                         retryOnThese ) )
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
    void canGetAndParseResponseFromNwisWithWebClientAndJacksonPojos()
            throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();

        // GET
        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( NWIS_URI_TWO,
                                                                         retryOnThese ) )
        {
            // Parse
            Response document = OBJECT_MAPPER.readValue( response.getResponse(),
                                                         Response.class );
            assertAll( () -> assertTrue( response.getStatusCode() >= 200
                                         && response.getStatusCode() < 300,
                                         "Expected HTTP 2XX response." ),
                       () -> assertTrue( document.getValue()
                                                 .getTimeSeries()
                                                 .length > 0,
                                         "Expected at least one TimeSeries" )
            );
        }
    }
}
