package wres.testing;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;
import okhttp3.OkHttpClient;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.ingesting.PreIngestException;
import wres.io.reading.wrds.ahps.ForecastResponse;
import wres.io.reading.ReaderUtilities;
import wres.http.WebClient;
import wres.http.WebClientUtils;

public class WrdsAhpsTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsAhpsTest.class );
    private static final String WRDS_HOSTNAME;

    static
    {
        String wrdsHostname = System.getenv( "WRDS_HOSTNAME" );

        if ( Objects.nonNull( wrdsHostname ) && !wrdsHostname.isBlank() )
        {
            WRDS_HOSTNAME = wrdsHostname;
        }
        else
        {
            throw new ExceptionInInitializerError( "The environment variable WRDS_HOSTNAME must be set." );
        }
    }

    /** Custom HttpClient to use */
    private static final OkHttpClient OK_HTTP_CLIENT;

    static
    {
        try
        {
            Pair<SSLContext, X509TrustManager> sslContext = ReaderUtilities.getSslContextTrustingDodSignerForWrds();
            OK_HTTP_CLIENT = WebClientUtils.defaultTimeoutHttpClient()
                                           .newBuilder()
                                           .sslSocketFactory( sslContext.getKey().getSocketFactory(),
                                                              sslContext.getRight() )
                                           .build();
        }
        catch ( PreIngestException e )
        {
            throw new ExceptionInInitializerError( "Failed to acquire the TLS context for connecting to WRDS: "
                                                   + e.getMessage() );
        }
    }
    
    private static final WebClient WEB_CLIENT = new WebClient( true, OK_HTTP_CLIENT );
    private static final URI WRDS_AHPS_URI_ONE =
            URI.create( "https://" + WRDS_HOSTNAME
                        + "/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/DRRC2?issuedTime=(2018-10-01T00%3A00%3A00Z%2C2018-10-07T23%3A23%3A59Z]&proj=WRES_automated_testing" );
    private static final URI WRDS_AHPS_URI_TWO =
            URI.create( "https://" + WRDS_HOSTNAME
                        + "/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/FROV2?issuedTime=(2020-03-01T00%3A00%3A00Z%2C2020-04-30T23%3A23%3A59Z]&proj=WRES_automated_testing" );
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void canGetMinimalResponseFromWrdsAhpsWithWebClient() throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();

        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( WRDS_AHPS_URI_ONE,
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
    void canGetAndParseResponseFromWrdsAhpsWithWebClientAndJacksonPojos() throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();

        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( WRDS_AHPS_URI_TWO,
                                                                         retryOnThese ) )
        {
            ForecastResponse document = OBJECT_MAPPER.readValue( response.getResponse(),
                                                                 ForecastResponse.class );
            LOGGER.debug( "Document parsed: {}", document );
            assertAll( () -> assertTrue( response.getStatusCode() >= 200
                                         && response.getStatusCode() < 300,
                                         "Expected HTTP 2XX response." ),
                       () -> assertTrue( document.getForecasts().length > 0,
                                         "Expected at least one forecast." )
            );
        }
    }
}
