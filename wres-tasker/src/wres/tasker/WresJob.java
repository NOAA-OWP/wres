package wres.tasker;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;
import static jakarta.ws.rs.core.MediaType.TEXT_HTML;

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
    private static final String REDIS_HOST_SYSTEM_PROPERTY_NAME = "wres.redisHost";
    private static final String REDIS_PORT_SYSTEM_PROPERTY_NAME = "wres.redisPort";

    /** To disable the queue length check, e.g. for development or testing */
    private static final String SKIP_QUEUE_LENGTH_CHECK_SYSTEM_PROPERTY_NAME =
            "wres.tasker.skipQueueLengthCheck";
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static final RedissonClient REDISSON_CLIENT;

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

        Config redissonConfig = new Config();
        String redisHost = null;
        int redisPort = DEFAULT_REDIS_PORT;
        String specifiedRedisHost = System.getProperty( REDIS_HOST_SYSTEM_PROPERTY_NAME );
        String specifiedRedisPortRaw = System.getProperty( REDIS_PORT_SYSTEM_PROPERTY_NAME );

        if ( Objects.nonNull( specifiedRedisHost ) )
        {
            redisHost = specifiedRedisHost;
        }

        if ( Objects.nonNull( specifiedRedisPortRaw ) )
        {
            redisPort = Integer.parseInt( specifiedRedisPortRaw );
        }

        if ( Objects.nonNull( redisHost ) )
        {
            String redisAddress = "redis://" + redisHost + ":" + redisPort;
            LOGGER.info( "Redis host specified: {}, using redis at {}",
                         specifiedRedisHost, redisAddress );
            redissonConfig.useSingleServer()
                          .setAddress( redisAddress );

            // The reasoning here is any server thread can cause access of an
            // object in redis (e.g. output, stdout, etc), regardless of whether
            // the job is currently active. So at least that number. Then there
            // are internal threads writing to active jobs, which includes all
            // jobs in the queue, and there are four objects per job. Do not
            // forget to update docker memory limits to account for the stack
            // space required per thread here.
            redissonConfig.setNettyThreads( Tasker.MAX_SERVER_THREADS
                                            + MAXIMUM_EVALUATION_COUNT * 4 );
            REDISSON_CLIENT = Redisson.create( redissonConfig );
        }
        else
        {
            REDISSON_CLIENT = null;
            LOGGER.info( "No redis host specified, using local JVM objects." );
        }
    }

    /** Shared job result state, exposed below */
    private static final JobResults JOB_RESULTS = new JobResults( CONNECTION_FACTORY,
                                                                  REDISSON_CLIENT );

    private static Connection connection = null;

    /** Guards connection */
    private static final Object CONNECTION_LOCK = new Object();

    @GET
    @Produces( MediaType.TEXT_PLAIN )
    public String getWresJob()
    {
        // Test connectivity to broker
        try ( Connection conn = CONNECTION_FACTORY.newConnection() )
        {
            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "Successfully connected to broker at {}:{}",
                             conn.getAddress(), conn.getPort() );
            }
        }
        catch( IOException | TimeoutException e )
        {
            throw new ConnectivityException( "broker",
                                             CONNECTION_FACTORY.getHost(),
                                             CONNECTION_FACTORY.getPort(),
                                             e );
        }

        // TODO add test for connectivity to redis via redisson

        return "Up";
    }


    /**
     * Post a declaration to start a new WRES job.
     * @param projectConfig The declaration to use in a new job.
     * @param wresUser Do not use. Deprecated field.
     * @param verb The verb to run on the declaration, default is execute.
     * @param postInput If the caller wishes to post input, true, default false.
     * @return HTTP 201 on success, 4XX on client error, 5XX on server error.
     */

    @POST
    @Consumes( APPLICATION_FORM_URLENCODED )
    @Produces( TEXT_HTML )
    public Response postWresJob( @FormParam( "projectConfig" )
                                 @DefaultValue( "" )
                                 String projectConfig,
                                 @Deprecated
                                 @FormParam( "userName" )
                                 String wresUser,
                                 @FormParam( "verb" )
                                 @DefaultValue( "execute" )
                                 String verb,
                                 @FormParam( "postInput" )
                                 @DefaultValue( "false" )
                                 boolean postInput )
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

        // Before registering a new job, see if there are already too many.
        try
        {
            int queueLength = this.getJobQueueLength();
            this.validateQueueLength( queueLength );
        }
        catch ( IOException | TimeoutException e )
        {
            LOGGER.error( "Attempt to check queue length failed.", e );
            return WresJob.internalServerError();
        }
        catch ( TooManyEvaluationsInQueueException tmeiqe )
        {
            LOGGER.warn( "Did not send job, returning 503.", tmeiqe );
            return WresJob.serviceUnavailable( "Too many evaluations are in the queue, try again in a moment." );
        }

        String jobId = JOB_RESULTS.registerNewJob();

        String urlCreated = "/job/" + jobId;
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

        Job.job jobMessage = Job.job.newBuilder()
                                    .setProjectConfig( projectConfig )
                                    .setVerb( actualVerb )
                                    .build();

        // If the caller wishes to post input data: parameter postInput=true
        if ( postInput )
        {
            // Pause before sending. Parse the declaration and add inputs before
            // sending along. This request will result in 201 created response
            // and the caller must send another request saying "I have finished
            // posting input."
            JOB_RESULTS.setJobMessage( jobId, jobMessage.toByteArray() );
            JOB_RESULTS.setAwaitingPostInputData( jobId );
            return Response.created( resourceCreated )
                           .entity( "<!DOCTYPE html><html><head><title>Evaluation job received.</title></head>"
                                    + "<body><h1>Evaluation job "
                                    + jobId + " has been received, the next step is to post input data.</h1>"
                                    + "<p>See <a href=\""
                                    + urlCreated + "\">" + urlCreated
                                    + "</a></p></body></html>" )
                           .build();
        }

        try
        {
            sendDeclarationMessage( jobId, jobMessage.toByteArray() );
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

        JOB_RESULTS.setInQueue( jobId );

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
        Integer jobResult = JOB_RESULTS.getJobResultRaw( jobId );

        if ( Objects.isNull( jobResult ) )
        {
            return WresJob.notFound( "Could not find job " + jobId );
        }

        String jobUrl= "/job/" + jobId;

        String statusUrl = jobUrl + "/status";
        String stdoutUrl = jobUrl + "/stdout";
        String stderrUrl = jobUrl + "/stderr";
        String outputUrl = jobUrl + "/output";

        // Create a list of actual job states from the enum that affords them.
        StringJoiner jobStates = new StringJoiner( ", " );

        for ( JobMetadata.JobState jobState : JobMetadata.JobState.values() )
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
     * @throws IllegalStateException when the job does not exist in shared state
     * @throws TimeoutException
     */
    static void sendDeclarationMessage( String jobId, byte[] message )
            throws IOException, TimeoutException
    {
        // Use a shared connection across requests.
        Connection connection = WresJob.getConnection();

        try ( Channel channel = connection.createChannel() )
        {
            AMQP.Queue.DeclareOk declareOk =
                    channel.queueDeclare( SEND_QUEUE_NAME,
                                          false,
                                          false,
                                          false,
                                          null );

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

            JOB_RESULTS.watchForJobFeedback( jobId,
                                             jobStatusExchange );

            channel.basicPublish( "",
                                  SEND_QUEUE_NAME,
                                  properties,
                                  message );

            LOGGER.info( "Sent a message to queue '{}' with properties '{}'",
                         SEND_QUEUE_NAME, properties );
            LOGGER.debug( "I sent this message to queue '{}' with properties '{}': {}.",
                          SEND_QUEUE_NAME, properties, message );
        }
    }


    /**
     * Get the length of the job queue.
     * @return The length of the job queue, or 0 when System Property
     * wres.tasker.skipQueueLengthCheck is set to true.
     * @throws IOException When communication with broker fails.
     * @throws TimeoutException When connection to broker times out.
     */
    private int getJobQueueLength() throws IOException, TimeoutException
    {
        String skipCheck = System.getProperty( SKIP_QUEUE_LENGTH_CHECK_SYSTEM_PROPERTY_NAME );

        if ( skipCheck != null && skipCheck.equalsIgnoreCase( "true" ) )
        {
            return 0;
        }

        Connection connection = WresJob.getConnection();

        try ( Channel channel = connection.createChannel() )
        {
            AMQP.Queue.DeclareOk declareOk =
                    channel.queueDeclare( SEND_QUEUE_NAME,
                                          false,
                                          false,
                                          false,
                                          null );
            return declareOk.getMessageCount();
        }
    }


    /**
     * Check to see if there are too many actively worked jobs in the job queue.
     * @param queueLength The length of the queue.
     * @throws TooManyEvaluationsInQueueException When too many jobs queued.
     */

    private void validateQueueLength( int queueLength )
    {
        if ( queueLength > MAXIMUM_EVALUATION_COUNT )
        {
            throw new TooManyEvaluationsInQueueException( "Too many evaluations in the queue. "
                                                          + queueLength
                                                          + " found, will continue to reject evaluations until "
                                                          + MAXIMUM_EVALUATION_COUNT
                                                          + " or fewer are in the queue." );
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

    static JobResults getSharedJobResults()
    {
        return WresJob.JOB_RESULTS;
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

        if ( Objects.nonNull( REDISSON_CLIENT ) )
        {
            REDISSON_CLIENT.shutdown();
        }
    }

    static final class ConnectivityException extends RuntimeException
    {
        private ConnectivityException( String serviceName,
                                       String host,
                                       int port,
                                       Throwable cause )
        {
            super( "Failed to connect to " + serviceName + " at " + host + ":"
                   + port, cause );
        }
    }
}
