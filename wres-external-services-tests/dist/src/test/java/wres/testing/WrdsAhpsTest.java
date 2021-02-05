package wres.testing.nwis;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.reading.wrds.ReadValueManager;
import wres.io.utilities.WebClient;

public class WrdsAhpsTest
{
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT =
            ReadValueManager.getSslContextTrustingDodSigner();
    private static final URI WRDS_AHPS_URI = URI.create( "https://***REMOVED***.***REMOVED***.***REMOVED***/api/v1/forecasts/streamflow/ahps/nwsLocations/DRRC2?issuedTime=(2018-10-01T00%3A00%3A00Z%2C2018-10-07T23%3A23%3A59Z]&validTime=all&excludePast=none&groupInterval=none&groups=9999&groupStatistic=none&minForecastStatus=no_flooding&groupsRefTime=basisTime" );
    private final WebClient webClient = new WebClient( SSL_CONTEXT, true );

    @Test
    void canGetMinimalResponseFromWrdsAhpsWithWebClient() throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();

        try ( WebClient.ClientResponse response = webClient.getFromWeb( WRDS_AHPS_URI,
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

