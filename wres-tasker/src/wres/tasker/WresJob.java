package wres.tasker;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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

/**
 * Web services related to wres jobs.
 * A job is modeled as a resource.
 * A wres job is an evaluation.
 * To request an evaluation, one POSTs to /job.
 * A job id representing the evaluation is created by the server.
 * More services are available for each job: "status", "stdout", "stderr" ...
 * To request the status, one GETs /job/{jobid}/status
 * To request the stdout, one GETs /job/{jobid}/stdout
 * and so forth.
 * As of 2018-10, there are only plain text and/or html responses.
 */
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
    public Response postWresJob( @FormParam( "projectConfig" ) String projectConfig,
                                 @FormParam( "userName" ) String wresUser )
    {
        String databaseUrl = Users.getDatabaseHost( wresUser );
        String databaseName = Users.getDatabaseName( wresUser );
        String databaseUser = Users.getDatabaseUser( wresUser );

        // If all three are missing, call it a 404 not found
        if ( databaseUrl == null && databaseName == null && databaseUser == null )
        {
            return WresJob.notFound( "Could not find any record of user '"
                                     + wresUser
                                     + "' in the system. Please correct the user name or contact WRES team." );
        }
        // If only one of the three is missing, mistake on our end 500
        else if ( databaseUrl == null || databaseName == null || databaseUser == null )
        {
            return WresJob.internalServerError( "Not your fault. Found incomplete information on user '"
                                                + wresUser
                                                + "' in the system. Please let WRES team know." );
        }

        Job.job jobMessage = Job.job.newBuilder()
                                    .setDatabaseHostname( databaseUrl )
                                    .setDatabaseName( databaseName )
                                    .setDatabaseUsername( databaseUser )
                                    .setProjectConfig( projectConfig )
                                    .build();
        String jobId;

        try
        {
            jobId = sendMessage( jobMessage.toByteArray() );
        }
        catch ( IOException | TimeoutException e )
        {
            LOGGER.error( "Attempt to send message failed.", e );
            return WresJob.internalServerError();
        }

        String urlCreated= "/job/" + jobId;

        String statusUrl = urlCreated + "/status";
        String stdoutUrl = urlCreated + "/stdout";
        String stderrUrl = urlCreated + "/stderr";
        String outputUrl = urlCreated + "/output";

        URI resourceCreated;
        try
        {
            resourceCreated = new URI( urlCreated );
        }
        catch ( URISyntaxException use )
        {
            LOGGER.error( "Failed to create uri using {}", urlCreated, use );
            return WresJob.internalServerError();
        }

        return Response.created( resourceCreated )
                       .entity( "<!DOCTYPE html><html><head><title>Job received.</title></head>"
                            + "<body><h1>Your job has been received for processing.</h1>"
                            + "<p>To check whether your job has completed/succeeded/failed, GET (poll) <a href=\""
                            + statusUrl + "\">" + statusUrl + "</a></p>"
                            + "<p>For job evaluation results, GET <a href=\""
                            + outputUrl + "\">" + outputUrl
                            + "</a> <strong>after</strong> status shows that the job completed successfully (exited 0).</p>"
                            + "<p>Please DELETE <a href=\""
                            + outputUrl + "\">" + outputUrl
                            + "</a> <strong>after</strong> you have finished reading evaluation results (by doing GET as described)."
                            + " One option (of many) is to use curl: <code>curl -X DELETE --cacert \"/path/to/wres_ca_x509_cert.pem\" https://[servername]/"
                            + outputUrl
                            + "</code> Another option is to use your client library to do the same or use developer tools in your browser to edit the /output GET request to become a DELETE request.</p>"
                            + "<p>For detailed progress (debug) information, GET <a href=\""
                            + stdoutUrl + "\">" + stdoutUrl + "</a>"
                            + " or <a href=\"" + stderrUrl + "\">" + stderrUrl
                            + "</a></p></body></html>" )
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

    private static Response internalServerError()
    {
        return WresJob.internalServerError( "An issue occurred that is not your fault." );
    }

    private static Response internalServerError( String message )
    {
        return Response.serverError()
                       .entity("<!DOCTYPE html><html><head><title>Our mistake</title></head><body><h1>Internal Server Error</h1><p>"
                               + message
                               + "</p></body></html>")
                       .build();
    }

    private static Response notFound( String message )
    {
        return Response.status( Response.Status.NOT_FOUND )
                       .entity( "<!DOCTYPE html><html><head><title>Not found</title></head><body><h1>Not Found</h1><p>"
                                + message
                                + "</p></body></html>" )
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


    /**
     * Abruptly stops all listening for job results that this class listens for,
     * and closes open connections, and only warns on exceptions thrown on
     * connection close.
     */

    static void shutdownNow()
    {
        JOB_RESULTS.shutdownNow();

        synchronized ( CONNECTION_LOCK )
        {
            if ( WresJob.connection != null )
            {
                try
                {
                    WresJob.connection.close();
                }
                catch ( IOException ioe )
                {
                    LOGGER.warn( "Exception while closing broker connection.",
                                 ioe );
                }
            }
        }
    }
}
