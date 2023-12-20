package wres.worker;

import java.io.IOException;
import java.util.Optional;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpError;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class WresEvaluationProcessorTest
{
    /** Mocker server instance. */
    private ClientAndServer mockServer;

    private final Connection mockConnection = mock( Connection.class );

    /** Mock job message for a smoke test job */
    private final byte[] mockJobMessage =
            { 34, -15, 4, 108, 97, 98, 101, 108, 58, 32, 119, 101, 98, 32, 100, 101, 109, 111, 13,
                    10, 111, 98, 115, 101, 114, 118, 101, 100, 58, 32, 47, 109, 110, 116, 47, 119, 114, 101, 115, 95,
                    115, 104, 97, 114, 101, 47, 115, 121, 115, 116, 101, 115, 116, 115
                    , 47, 100, 97, 116, 97, 47, 68, 82, 82, 67, 50, 81, 73, 78, 69, 46, 120, 109, 108, 13, 10, 112, 114,
                    101, 100, 105, 99, 116, 101, 100, 58, 13, 10, 32, 32, 108, 97, 98,
                    101, 108, 58, 32, 72, 69, 70, 83, 13, 10, 32, 32, 115, 111, 117, 114, 99, 101, 115, 58, 32, 47, 109,
                    110, 116, 47, 119, 114, 101, 115, 95, 115, 104, 97, 114, 101,
                    47, 115, 121, 115, 116, 101, 115, 116, 115, 47, 100, 97, 116, 97, 47, 100, 114, 114, 99, 50, 70,
                    111, 114, 101, 99, 97, 115, 116, 115, 79, 110, 101, 77, 111, 110, 116,
                    104, 47, 13, 10, 117, 110, 105, 116, 58, 32, 109, 51, 47, 115, 13, 10, 108, 101, 97, 100, 95, 116,
                    105, 109, 101, 115, 58, 13, 10, 32, 32, 109, 105, 110, 105, 109, 117,
                    109, 58, 32, 48, 13, 10, 32, 32, 109, 97, 120, 105, 109, 117, 109, 58, 32, 52, 56, 13, 10, 32, 32,
                    117, 110, 105, 116, 58, 32, 104, 111, 117, 114, 115, 13, 10, 112, 114,
                    111, 98, 97, 98, 105, 108, 105, 116, 121, 95, 116, 104, 114, 101, 115, 104, 111, 108, 100, 115, 58,
                    13, 10, 32, 32, 118, 97, 108, 117, 101, 115, 58, 32,
                    91, 48, 46, 48, 48, 50, 44, 32, 48, 46, 48, 49, 44, 32, 48, 46, 49, 44, 32, 48, 46, 57, 44, 32, 48,
                    46, 57, 57, 44, 32, 48, 46, 57, 57, 56, 93, 13, 10, 32, 32, 111,
                    112, 101, 114, 97, 116, 111, 114, 58, 32, 103, 114, 101, 97, 116, 101, 114, 32, 101, 113, 117, 97,
                    108, 13, 10, 109, 101, 116, 114, 105, 99, 115, 58, 13, 10, 32, 32
                    , 45, 32, 113, 117, 97, 110, 116, 105, 108, 101, 32, 113, 117, 97, 110, 116, 105, 108, 101, 32, 100,
                    105, 97, 103, 114, 97, 109, 13, 10, 32, 32, 45, 32, 114, 97, 110
                    , 107, 32, 104, 105, 115, 116, 111, 103, 114, 97, 109, 13, 10, 32, 32, 45, 32, 114, 101, 108, 97,
                    116, 105, 118, 101, 32, 111, 112, 101, 114, 97, 116, 105, 110, 103,
                    32, 99, 104, 97, 114, 97, 99, 116, 101, 114, 105, 115, 116, 105, 99, 32, 100, 105, 97, 103, 114, 97,
                    109, 13, 10, 32, 32, 45, 32, 98, 111, 120, 32, 112, 108, 111, 116,
                    32, 111, 102, 32, 101, 114, 114, 111, 114, 115, 32, 98, 121, 32, 102, 111, 114, 101, 99, 97, 115,
                    116, 32, 118, 97, 108, 117, 101, 13, 10, 32, 32, 45, 32, 114, 111,
                    111, 116, 32, 109, 101, 97, 110, 32, 115, 113, 117, 97, 114, 101, 32, 101, 114, 114, 111, 114, 13,
                    10, 32, 32, 45, 32, 115, 97, 109, 112, 108, 101, 32, 115, 105,
                    122, 101, 13, 10, 32, 32, 45, 32, 109, 101, 97, 110, 32, 97, 98, 115, 111, 108, 117, 116, 101, 32,
                    101, 114, 114, 111, 114, 13, 10, 32, 32, 45, 32, 98, 111, 120, 32
                    , 112, 108, 111, 116, 32, 111, 102, 32, 101, 114, 114, 111, 114, 115, 32, 98, 121, 32, 111, 98, 115,
                    101, 114, 118, 101, 100, 32, 118, 97, 108, 117, 101, 13, 10, 32,
                    32, 45, 32, 114, 101, 108, 105, 97, 98, 105, 108, 105, 116, 121, 32, 100, 105, 97, 103, 114, 97,
                    109, 13, 10, 32, 32, 32, 32, 58, 5, 119, 114, 101, 115, 50, 66, 20,
                    104, 111, 115, 116, 46, 100, 111, 99, 107, 101, 114, 46, 105, 110, 116, 101, 114, 110, 97, 108, 74,
                    4, 53, 52, 51, 50 };

    @BeforeEach
    void startServer()
    {
        this.mockServer = ClientAndServer.startClientAndServer( 8010 );
    }

    @AfterEach
    void stopServer() throws IOException
    {
        this.mockServer.stop();
        this.mockConnection.close();
    }

    @Test
    void testEvaluateCallSmokeTest() throws IOException
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/startEvaluation" )
                                         .withMethod( "POST" ), Times.once() )
                       .respond( HttpResponse.response( "123456" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/stdout/123456" )
                                         .withMethod( "GET" ), Times.once() )
                       .respond( HttpResponse.response( "500" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/stderr/123456" )
                                         .withMethod( "GET" ), Times.once() )
                       .respond( HttpResponse.response( "500" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/status/123456" )
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response("COMPLETED") );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/getEvaluation/123456" )
                                         .withMethod( "GET" ), Times.once() )
                       .respond( HttpResponse.response() );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/close" )
                                         .withMethod( "POST" ), Times.once() )
                       .respond( HttpResponse.response() );

        Envelope envelope = new Envelope( 1, false, "", "wres.job" );

        WresEvaluationProcessor wresEvaluationProcessor = new WresEvaluationProcessor( "wres.job.status",
                                                                                       "6923033347430537124",
                                                                                       this.mockConnection,
                                                                                       envelope,
                                                                                       this.mockJobMessage,
                                                                                       8010 );

        when( this.mockConnection.createChannel() ).thenReturn( mock( Channel.class ) );

        Integer call = wresEvaluationProcessor.call();

        Assertions.assertEquals( Optional.of( 200 ), Optional.ofNullable( call ) );
    }

    @Test
    void testEvaluateCallResiliency() throws IOException
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/startEvaluation" )
                                         .withMethod( "POST" ), Times.once() )
                       .respond( HttpResponse.response( "123456" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/stdout/123456" )
                                         .withMethod( "GET" ), Times.once() )
                       .respond( HttpResponse.response( "500" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/stderr/123456" )
                                         .withMethod( "GET" ), Times.once() )
                       .respond( HttpResponse.response( "500" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/status/123456" )
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response("COMPLETED") );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/getEvaluation/123456" )
                                         .withMethod( "GET" ), Times.exactly( 3 ) )
                       .error( HttpError.error().withDropConnection( true ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/getEvaluation/123456" )
                                         .withMethod( "GET" ), Times.once() )
                       .respond( HttpResponse.response() );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( "/evaluation/close" )
                                         .withMethod( "POST" ), Times.once() )
                       .respond( HttpResponse.response() );

        Envelope envelope = new Envelope( 1, false, "", "wres.job" );

        WresEvaluationProcessor wresEvaluationProcessor = new WresEvaluationProcessor( "wres.job.status",
                                                                                       "6923033347430537124",
                                                                                       this.mockConnection,
                                                                                       envelope,
                                                                                       this.mockJobMessage,
                                                                                       8010 );

        when( this.mockConnection.createChannel() ).thenReturn( mock( Channel.class ) );

        Integer call = wresEvaluationProcessor.call();

        Assertions.assertEquals( Optional.of( 200 ), Optional.ofNullable( call ) );
    }
}
