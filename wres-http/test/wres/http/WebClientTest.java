package wres.http;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import okhttp3.Call;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link WebClient}.
 *
 * @author James Brown
 */

class WebClientTest
{
    @RegisterExtension
    private static final WireMockExtension WIREMOCK = WireMockExtension.newInstance()
                                                                       .options( WireMockConfiguration.wireMockConfig()
                                                                                                      .dynamicPort()
                                                                                                      .dynamicHttpsPort() )
                                                                       .build();

    /**
     * #77007
     */

    @Test
    void testGetTimingInformationDoesNotThrowArrayIndexOutOfBoundsExceptionWhenNothingRead()
    {
        WebClient client = new WebClient( true );

        String timingInformation = client.getTimingInformation();
        String firstPart = StringUtils.substringBefore( timingInformation, "," );

        assertEquals( "Out of request/response count 0", firstPart );
    }

    @Test
    void testExceptionWhenRetryPolicyTriggeredNoRetries() throws IOException
    {
        OkHttpClient mockClient = WebClientUtils.defaultHttpClient();
        Call mockCall = mock( Call.class );
        OkHttpClient spyClient = spy( mockClient );
        WebClient client = new WebClient( spyClient, RetryPolicy.builder()
                                                                .build() );
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test" );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        Mockito.when( mockCall.execute() )
               .thenThrow( new ConnectException() );

        assertThrows( IOException.class, () -> client.getFromWeb( uri ) );
    }

    @Test
    void testExceptionWhenRetryPolicyTriggered1Retries() throws IOException
    {
        OkHttpClient mockClient = WebClientUtils.defaultHttpClient();
        Call mockCall = mock( Call.class );
        OkHttpClient spyClient = spy( mockClient );
        WebClient client = new WebClient( spyClient, RetryPolicy.builder()
                                                                .maxRetryCount( 1 )
                                                                .build() );
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test" );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        Mockito.when( mockCall.execute() )
               .thenThrow( new ConnectException() )
               .thenThrow( new ConnectException() );

        assertThrows( IOException.class, () -> client.getFromWeb( uri ) );
    }

    @Test
    void testExceptionWhenRetryPolicyTriggered2RetriesEvenWithReconnect() throws IOException
    {
        OkHttpClient mockClient = WebClientUtils.defaultHttpClient();
        Call mockCall = mock( Call.class );
        OkHttpClient spyClient = spy( mockClient );
        WebClient client = new WebClient( spyClient, RetryPolicy.builder()
                                                                .maxRetryCount( 2 )
                                                                .build() );
        // Fake API key should be redacted in any non-debug logging: cannot assert this, but can witness it in test log
        // as [REDACTED], rather than "foo"
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test?api_key=foo" );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        Mockito.when( mockCall.execute() )
               .thenThrow( new ConnectException() )
               .thenThrow( new ConnectException() )
               .thenReturn( mock( Response.class ) );

        assertThrows( IOException.class, () -> client.getFromWeb( uri ) );
    }

    @Test
    void testRecoversWhenRetryPolicyTriggered() throws IOException
    {
        OkHttpClient mockClient = WebClientUtils.defaultHttpClient();
        Call mockCall = mock( Call.class );
        OkHttpClient spyClient = spy( mockClient );
        WebClient client = new WebClient( spyClient, RetryPolicy.builder()
                                                                .maxRetryCount( 4 )
                                                                .build() );
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test" );
        Response mockResponse = mock( Response.class );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        when( mockCall.execute() )
                .thenThrow( new ConnectException() )
                .thenThrow( new ConnectException() )
                .thenReturn( mockResponse );
        when( mockResponse.body() )
                .thenReturn( mock( ResponseBody.class ) );
        when( mockResponse.code() )
                .thenReturn( 200 );
        when( mockResponse.headers() )
                .thenReturn( mock( Headers.class ) );

        assertEquals( WebClient.ClientResponse.class,
                      ( client.getFromWeb( uri ) )
                              .getClass() );
    }

    @Test
    void testRecoversWhenRetryPolicyTriggeredWithHttpServerError() throws IOException
    {
        String path = "/foo/bar";
        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + path );

        WebClient client = new WebClient( WebClientUtils.defaultHttpClient(), RetryPolicy.builder()
                                                                                         .maxRetryCount( 2 )
                                                                                         .build() );

        // Return 503, then 200
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( path ) )
                                  .inScenario( "Retry Logic" )
                                  .whenScenarioStateIs( Scenario.STARTED )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 503 ) )
                                  .willSetStateTo( "Success State" ) );

        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( path ) )
                                  .inScenario( "Retry Logic" )
                                  .whenScenarioStateIs( "Success State" )
                                  .willReturn( WireMock.aResponse()
                                                       .withBody( "baz" )
                                                       .withStatus( 200 ) ) );

        WebClient.ClientResponse response = client.getFromWeb( fakeUri );
        assertEquals( "baz", new String( response.getResponse()
                                                 .readAllBytes() ) );
    }
}
