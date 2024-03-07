package wres.http;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;

import okhttp3.Call;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
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
        WebClient client = new WebClient(spyClient, new RetryPolicy.Builder().build() );
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test" );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        Mockito.when( mockCall.execute() ).thenThrow( new ConnectException() );

        assertThrows( IOException.class, () -> client.getFromWeb( uri, WebClientUtils.getDefaultRetryStates() ) );
    }

    @Test
    void testExceptionWhenRetryPolicyTriggered1Retries() throws IOException
    {
        OkHttpClient mockClient = WebClientUtils.defaultHttpClient();
        Call mockCall = mock( Call.class );
        OkHttpClient spyClient = spy( mockClient );
        WebClient client = new WebClient(spyClient, new RetryPolicy.Builder().maxRetryCount( 1 ).build() );
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test" );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        Mockito.when( mockCall.execute() )
               .thenThrow( new ConnectException() )
               .thenThrow( new ConnectException() );

        assertThrows( IOException.class, () -> client.getFromWeb( uri, WebClientUtils.getDefaultRetryStates() ) );
    }

    @Test
    void testExceptionWhenRetryPolicyTriggered2RetriesEvenWithReconnect() throws IOException
    {
        OkHttpClient mockClient = WebClientUtils.defaultHttpClient();
        Call mockCall = mock( Call.class );
        OkHttpClient spyClient = spy( mockClient );
        WebClient client = new WebClient(spyClient, new RetryPolicy.Builder().maxRetryCount( 2 ).build() );
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test" );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        Mockito.when( mockCall.execute() )
               .thenThrow( new ConnectException() )
               .thenThrow( new ConnectException() )
               .thenReturn( mock( Response.class ) );

        assertThrows( IOException.class, () -> client.getFromWeb( uri, WebClientUtils.getDefaultRetryStates() ) );
    }

    @Test
    void testRecoversWhenRetryPolicyTriggered() throws IOException
    {
        OkHttpClient mockClient = WebClientUtils.defaultHttpClient();
        Call mockCall = mock( Call.class );
        OkHttpClient spyClient = spy( mockClient );
        WebClient client = new WebClient(spyClient, new RetryPolicy.Builder().maxRetryCount( 4 ).build() );
        URI uri = URI.create( "http://localhost:8010/evaluation/status/test" );
        Response mockResponse = mock( Response.class );

        doReturn( mockCall ).when( spyClient ).newCall( any() );
        when( mockCall.execute() )
               .thenThrow( new ConnectException() )
               .thenThrow( new ConnectException() )
               .thenReturn( mockResponse );
        when( mockResponse.body() ).thenReturn( mock( ResponseBody.class ) );
        when( mockResponse.code() ).thenReturn( 200 );
        when( mockResponse.headers() ).thenReturn( mock( Headers.class ) );

        assertEquals( WebClient.ClientResponse.class, (client.getFromWeb( uri, WebClientUtils.getDefaultRetryStates() ) ).getClass() );
    }
}
