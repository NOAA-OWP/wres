package wres.tasker;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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

import static javax.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static javax.ws.rs.core.MediaType.TEXT_HTML;

import wres.messages.BrokerHelper;
import wres.messages.generated.Job;

import static wres.messages.generated.Job.job.Verb;

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

    /**
     * The count of evaluations combined with the maximum length below (which
     * is around 1x-2.5x the bytes) that could be handled by broker with current
     * broker memory limits minus 100MiB usually used by broker.
     */
    private static final short MAXIMUM_EVALUATION_COUNT = 50;

    /**
     * A maximum length less than the largest-seen successful project sent
     * to a worker-shim with the current worker-shim heap limits. E.g. no OOME.
     */
    private static final int MAXIMUM_PROJECT_DECLARATION_LENGTH = 2_500_000;

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
    @Consumes( APPLICATION_FORM_URLENCODED )
    @Produces( TEXT_HTML )
    public Response postWresJob( @FormParam( "projectConfig" ) String projectConfig,
                                 @Deprecated @FormParam( "userName" ) String wresUser,
                                 @FormParam( "verb" ) String verb )
    {
        int lengthOfProjectDeclaration = projectConfig.length();

        // Limit project config to less than 1.6 million characters
        if ( lengthOfProjectDeclaration > MAXIMUM_PROJECT_DECLARATION_LENGTH )
        {
            String projectConfigFirstChars = projectConfig.substring( 0, 1000 );
            LOGGER.warn( "Received a project declaration of length {} starting with {}",
                         lengthOfProjectDeclaration, projectConfigFirstChars );
            return WresJob.badRequest( "The project declaration has "
                                       + lengthOfProjectDeclaration
                                       + " characters, which is more than "
                                       + MAXIMUM_PROJECT_DECLARATION_LENGTH
                                       + ", please find a way to shrink the "
                                       + " project declaration and re-send." );
        }

        // Default to execute per tradition and majority case.
        Verb actualVerb = null;

        // Search through allowed values
        if ( verb != null && !verb.isBlank() )
        {
            for ( Verb allowedValue : Verb.values() )
            {
                String allowed = allowedValue.name()
                                             .toLowerCase();
                if ( verb.toLowerCase()
                         .equals( allowed ) )
                {
                    actualVerb = allowedValue;
                    break;
                }
            }

            if ( actualVerb == null )
            {
                return WresJob.badRequest( "Verb '"
                                         + verb
                                         + "' not available." );
            }
        }
        else
        {
            // Default to "execute"
            actualVerb = Verb.EXECUTE;
        }

        Job.job jobMessage = Job.job.newBuilder()
                                    .setProjectConfig( projectConfig )
                                    .setVerb( actualVerb )
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
        catch ( TooManyEvaluationsInQueueException tmeiqe )
        {
            LOGGER.warn( "Did not send job, returning 503.", tmeiqe );
            return WresJob.serviceUnavailable( "Too many evaluations are in the queue, try again in a moment." );
        }

        String urlCreated= "/job/" + jobId;
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
                       .entity( "<!DOCTYPE html><html><head><title>Evaluation job received.</title></head>"
                            + "<body><h1>Evaluation job " + jobId + " has been received for processing.</h1>"
                            + "<p>See <a href=\""
                            + urlCreated + "\">" + urlCreated
                            + "</a></p></body></html>" )
                       .build();
    }


    /**
     * Provide guidance on which urls are available for evaluation information,
     * including status, output, and debug information.
     * @param jobId the job to look for
     * @return a response including more specific URLs for a job
     */
    @GET
    @Path( "/{jobId}" )
    @Produces( TEXT_HTML )
    public Response getWresJobInfo( @PathParam( "jobId" ) String jobId )
    {
        Integer jobResult = JobResults.getJobResultRaw( jobId );

        if ( Objects.isNull( jobResult ) )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "<!DOCTYPE html><html><head><title>Not Found</title></head>"
                                    + " <body><h1>Not found</h1><p>Could not find job "
                                    + jobId + "</p></body></html>")
                               .build();
        }

        String jobUrl= "/job/" + jobId;

        String statusUrl = jobUrl + "/status";
        String stdoutUrl = jobUrl + "/stdout";
        String stderrUrl = jobUrl + "/stderr";
        String outputUrl = jobUrl + "/output";

        // Create a list of actual job states from the enum that affords them.
        StringJoiner jobStates = new StringJoiner( ", " );

        for ( JobResults.JobState jobState : JobResults.JobState.values() )
        {
            jobStates.add( jobState.toString() );
        }

        return Response.ok( "<!DOCTYPE html><html><head><title>About job "
                            + jobId + "</title></head>"
                            + "<body><h1>How to proceed with job "
                            + jobId + " using the WRES HTTP API</h1>"
                            + "<p>To check whether your job has completed/succeeded/failed, GET (poll) <a href=\""
                            + statusUrl + "\">" + statusUrl + "</a>.</p>"
                            + "<p>The possible job states are: "
                            + jobStates.toString() + ".</p>"
                            + "<p>For job evaluation results, GET <a href=\""
                            + outputUrl + "\">" + outputUrl
                            + "</a> <strong>after</strong> status shows that the job completed successfully (exited 0).</p>"
                            + "<p>When you GET the above output url, you may specify media type text/plain in the Accept header to get a newline-delimited list of outputs.</p>"
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
     * @throws TooManyEvaluationsInQueueException when queue has lots of jobs
     * @throws TimeoutException
     */
    private String sendMessage( byte[] message )
            throws IOException, TimeoutException
    {
        // Use a shared connection across requests.
        Connection connection = WresJob.getConnection();

        try ( Channel channel = connection.createChannel() )
        {
            // Guarantee a positive number. Using Math.abs would open up failure
            // in edge cases. A while loop seems complex. Thanks to Ted Hopp
            // on StackOverflow question id 5827023.
            long someRandomNumber = RANDOM.nextLong() & Long.MAX_VALUE;

            String jobId = String.valueOf( someRandomNumber );

            AMQP.Queue.DeclareOk declareOk =
                    channel.queueDeclare( SEND_QUEUE_NAME,
                                          false,
                                          false,
                                          false,
                                          null );

            int queueLength = declareOk.getMessageCount();

            if ( queueLength > MAXIMUM_EVALUATION_COUNT )
            {
                throw new TooManyEvaluationsInQueueException( "Too many evaluations in the queue. "
                                                              + queueLength
                                                              + " found, will continue to reject evaluations until "
                                                              + MAXIMUM_EVALUATION_COUNT
                                                              + " or fewer are in the queue." );
            }

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

    private static Response serviceUnavailable( String message )
    {
        return Response.status( Response.Status.SERVICE_UNAVAILABLE )
                       .entity("<!DOCTYPE html><html><head><title>Service temporarily unavailable</title></head><body><h1>Service Unavailable</h1><p>"
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

    private static Response badRequest( String message )
    {
        return Response.status( Response.Status.BAD_REQUEST )
                       .entity( "<!DOCTYPE html><html><head><title>Bad Request</title></head><body><h1>Bad Request</h1><p>"
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
