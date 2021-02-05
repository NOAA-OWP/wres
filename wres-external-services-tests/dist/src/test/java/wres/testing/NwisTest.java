package wres.testing;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.utilities.WebClient;

public class NwisTest
{
    private static final URI NWIS_URI = URI.create( "https://nwis.waterservices.usgs.gov/nwis/iv/?format=json&indent=on&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:00Z&endDT=2018-10-07T23:59:59Z" );
    private final WebClient webClient = new WebClient();

    @Test
    void canGetMinimalResponseFromNwisWithWebClient() throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();

        try ( WebClient.ClientResponse response = webClient.getFromWeb( NWIS_URI,
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
}

