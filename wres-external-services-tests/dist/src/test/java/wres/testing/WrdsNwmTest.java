package wres.testing;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.reading.wrds.ReadValueManager;
import wres.io.reading.wrds.nwm.NwmRootDocument;
import wres.io.utilities.WebClient;

public class WrdsNwmTest
{
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT =
            ReadValueManager.getSslContextTrustingDodSigner();
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT, true );
    private static final URI WRDS_NWM_URI_ONE = URI.create( "https://***REMOVED***.***REMOVED***.***REMOVED***/api/prod/nwm/ops/medium_range/streamflow/nwm_feature_id/18384141/?forecast_type=ensemble" );
    private static final URI WRDS_NWM_URI_TWO = URI.create( "https://***REMOVED***.***REMOVED***.***REMOVED***/api/prod/nwm/ops/medium_range/streamflow/nwm_feature_id/5907079/?forecast_type=ensemble" );

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

