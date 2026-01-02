package wres.testing;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import okhttp3.OkHttpClient;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.reading.PreReadException;
import wres.reading.ReaderUtilities;
import wres.reading.wrds.nwm.NwmRootDocument;
import wres.http.WebClient;
import wres.http.WebClientUtils;

public class WrdsNwmTest
{
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

    private static final ObjectMapper JSON_OBJECT_MAPPER =
            JsonMapper.builder()
                      .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true )
                      .build();

    /** Custom HttpClient to use */
    private static final OkHttpClient OK_HTTP_CLIENT;

    static
    {
        try
        {
            Pair<SSLContext, X509TrustManager> sslContext = ReaderUtilities.getSslContextForWrds();
            OK_HTTP_CLIENT = WebClientUtils.defaultTimeoutHttpClient()
                                           .newBuilder()
                                           .sslSocketFactory( sslContext.getKey().getSocketFactory(),
                                                              sslContext.getRight() )
                                           .build();
        }
        catch ( PreReadException e )
        {
            throw new ExceptionInInitializerError( "Failed to acquire the TLS context for connecting to WRDS: "
                                                   + e.getMessage() );
        }
    }

    private static final WebClient WEB_CLIENT = new WebClient( true, OK_HTTP_CLIENT );
    private static final URI WRDS_NWM_URI_ONE =
            URI.create( "https://" + WRDS_HOSTNAME
                        + "/api/nwm3.0/v3.0/streamflow/medium_range/streamflow/nwm_feature_id/18384141/?forecast_type=ensemble" );
    private static final URI WRDS_NWM_URI_TWO =
            URI.create( "https://" + WRDS_HOSTNAME
                        + "/api/nwm3.0/v3.0/streamflow/medium_range/streamflow/nwm_feature_id/5907079/?forecast_type=ensemble" );

    @Test
    void canGetMinimalResponseFromWrdsNwmWithWebClient() throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();

        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( WRDS_NWM_URI_ONE,
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
    void canGetAndParseResponseFromWrdsNwmWithWebClient() throws IOException
    {
        List<Integer> retryOnThese = Collections.emptyList();
        NwmRootDocument document;

        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( WRDS_NWM_URI_TWO,
                                                                         retryOnThese ) )
        {
            // Parse the stream in the way WrdsNwmReader parses a document:
            document = JSON_OBJECT_MAPPER.readValue( response.getResponse(),
                                                     NwmRootDocument.class );
            assertAll( () -> assertTrue( response.getStatusCode() >= 200
                                         && response.getStatusCode() < 300,
                                         "Expected HTTP 2XX response." ),
                       () -> assertFalse( document.getForecasts()
                                                  .isEmpty(),
                                          "Expected more than zero forecasts" ),
                       () -> assertFalse( document.getVariable()
                                                  .isEmpty(),
                                          "Expected more than zero variables" )
            );
        }
    }
}

