package wres.tasker;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.BrokerHelper;
import wres.messages.generated.Job;

@Path( "/job")
public class WresJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJob.class );

    private static final String SEND_QUEUE_NAME = "wres.job";

    // Using a member variable fails, make it same across instances.
    private static final ConnectionFactory CONNECTION_FACTORY = new ConnectionFactory();

    private static final Random RANDOM = new Random( System.currentTimeMillis() );

    static
    {
        // Determine the actual broker name, whether from -D or default
        String brokerHost = BrokerHelper.getBrokerHost();
        String brokerVhost = BrokerHelper.getBrokerVhost();
        int brokerPort = BrokerHelper.getBrokerPort();
        CONNECTION_FACTORY.setHost( brokerHost );
        CONNECTION_FACTORY.setVirtualHost( brokerVhost );
        CONNECTION_FACTORY.setPort( brokerPort );
        CONNECTION_FACTORY.setSaslConfig( DefaultSaslConfig.EXTERNAL );
        SSLContext sslContext =
                BrokerHelper.getSSLContextWithClientCertificate( BrokerHelper.Role.TASKER );
        CONNECTION_FACTORY.useSslProtocol( sslContext );
    }

    private static final JobResults JOB_RESULTS = new JobResults( CONNECTION_FACTORY );

    private static Connection connection = null;

    /** Guards connection */
    private static final Object CONNECTION_LOCK = new Object();

    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public String getWresJob()
    {
        return "Up";
    }


    @POST
    @Consumes( "application/x-www-form-urlencoded")
    @Produces( "text/html")
    public Response postWresJob( @FormParam( "projectPath" ) String projectPath,
                                 @FormParam( "databaseUrl" ) String databaseUrl,
                                 @FormParam( "databaseName" ) String databaseName,
                                 @FormParam( "databaseUser" ) String databaseUser )
    {
        Job.job jobMessage = Job.job.newBuilder()
                                    .setDatabaseHostname( databaseUrl )
                                    .setDatabaseName( databaseName )
                                    .setDatabaseUsername( databaseUser )
                                    .setProjectConfig( projectPath )
                                    .build();
        String jobId;

        try
        {
            jobId = sendMessage( jobMessage.toByteArray() );
        }
        catch ( IOException | TimeoutException e )
        {
            LOGGER.error( "Attempt to send message failed.", e );
            return this.internalServerError();
        }

        String statusUrl = "/job/" + jobId + "/status";
        String stdoutUrl = "/job/" + jobId + "/stdout";
        String stderrUrl = "/job/" + jobId + "/stderr";

        return Response.ok( "<!DOCTYPE html><html><head><title>Job received.</title></head><body><h1>Your job has been received for processing.</h1><p>See <a href=\""
                            + statusUrl + "\">" + statusUrl + "</a></p>"
                            + "<p>For more details, see <a href=\"" + stdoutUrl + "\">" + stdoutUrl + "</a>"
                            + " or <a href=\"" + stderrUrl + "\">" + stderrUrl + "</a>"
                            + "</body></html>" )
                       .build();
    }


    /**
     *
     * @param message
     * @throws IOException when connectivity, queue declaration, or publication fails
     * @throws TimeoutException
     */
    private String sendMessage( byte[] message )
            throws IOException, TimeoutException
    {
        // Use a shared connection across requests.
        Connection connection = WresJob.getConnection();

        try ( Channel channel = connection.createChannel() )
        {
            long someRandomNumber = RANDOM.nextLong();
            String jobId = String.valueOf( someRandomNumber );

            channel.queueDeclare( SEND_QUEUE_NAME, false, false, false, null );

            // Tell the worker where to send results.
            String jobStatusExchange = JobResults.getJobStatusExchangeName();
            AMQP.BasicProperties properties =
                    new AMQP.BasicProperties
                            .Builder()
                            .replyTo( jobStatusExchange )
                            .correlationId( jobId )
                            .build();

            // Inform the JobResults class to start looking for correlationId.
            // Share a connection, but not a channel, aim for channel-per-thread.
            // I think something needs to be watching the queue or else messages
            // end up dropping on the floor, that is why this is called prior
            // to even publishing the job at all. JobResults is a bag-o-state.

            JOB_RESULTS.registerjobId( jobStatusExchange,
                                       jobId );

            channel.basicPublish( "",
                                  SEND_QUEUE_NAME,
                                  properties,
                                  message );

            LOGGER.info( "I sent this message to queue '{}' with properties '{}': {}.",
                         SEND_QUEUE_NAME, properties, message );
            return jobId;
        }
    }

    private Response internalServerError()
    {
        return Response.serverError()
                       .entity("<!DOCTYPE html><html><head><title>Our mistake</title></head><body><h1>Internal Server Error</h1><p>An issue occurred that is not your fault.</p></body></html>")
                       .build();
    }

    private static Connection getConnection()
            throws IOException, TimeoutException
    {
        synchronized( CONNECTION_LOCK )
        {
            if ( WresJob.connection == null )
            {
                WresJob.connection = CONNECTION_FACTORY.newConnection();
            }
        }

        return WresJob.connection;
    }
}
