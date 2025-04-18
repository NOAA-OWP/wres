package wres.tasker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultSaslConfig;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RLiveObjectService;
import org.redisson.api.RedissonClient;
import org.redisson.codec.KryoCodec;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static jakarta.ws.rs.core.MediaType.APPLICATION_FORM_URLENCODED;

import wres.config.yaml.DeclarationValidator;
import wres.messages.BrokerHelper;
import wres.messages.generated.Job;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;

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
@Path( "/job" )
public class WresJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WresJob.class );

    private static final String A_P_BODY_HTML = "</a></p></body></html>";
    private static final String P_BODY_HTML = "</p></body></html>";
    private static final String SEND_QUEUE_NAME = "wres.job";

    //Broker connection factory.
    private static final ConnectionFactory CONNECTION_FACTORY = new ConnectionFactory();

    //Redis constants and variables. Note that REDISSON_CLIENT is effectively
    //a constant, but cannot be final due to a quirk of how its initialized in the 
    //static block.
    private static final String REDIS_HOST_SYSTEM_PROPERTY_NAME = "wres.redisHost";
    private static final String REDIS_PORT_SYSTEM_PROPERTY_NAME = "wres.redisPort";
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static RedissonClient REDISSON_CLIENT = null;
    private static String redisHost = null;
    private static int redisPort = DEFAULT_REDIS_PORT;

    //Exception and error message texts.
    private static final String UNABLE_TO_VALIDATE_PROJECT_CONFIGURATION_DUE_TO_INTERNAL_ERROR =
            "Unable to validate project configuration due to internal error.";
    private static final String FULL_EXCEPTION_TRACE = "Full exception trace: ";
    private static final String UNABLE_TO_WRITE_PROTOBUF_RESPONSE_BYTE_ARRAY_DUE_TO_I_O_EXCEPTION =
            "Unable to write protobuf response byte array due to I/O exception.";

    //Admin authentication
    private static final String ADMIN_TOKEN_SYSTEM_PROPERTY_NAME = "wres.adminToken";
    private static final byte[] SALT = new byte[16];
    private static byte[] adminTokenHash = null; //Empty means password not specified.

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
    private static final int MAXIMUM_PROJECT_DECLARATION_LENGTH = 5_000_000;


    /** Property that allows for turning off broker queue length checking. **/
    private static final String SKIP_QUEUE_LENGTH_CHECK_SYSTEM_PROPERTY_NAME =
            "wres.tasker.skipQueueLengthCheck";

    //Stores active database information.
    private static String activeDatabaseName = "";
    private static String activeDatabaseHost = "";
    private static String activeDatabasePort = "";


    static
    {
        // Initialize storage of the admin token for some functions.
        // This method should not except out. If problems occur storing it,
        // then the tasker runs without using an admin token.
        intializeWresAdminToken();

        // If the broker connection factory fails to initialize, log an error and
        // pass up the exception so that this static block fails out.
        try
        {
            initializeBrokerConnectionFactory();
        }
        catch ( IllegalStateException ise )
        {
            LOGGER.error( "Failed to initialize the broker connection factory; message: " + ise.getMessage()
                          + ". Aborting WresJob static block.",
                          ise );
            throw ise;
        }

        // This intializes the Redis/persister client. Note that, any attempt to fail
        // out later in this static **MUST** call REDISSON_CLIENT.shutdown to ensure that
        // the client closes out and the application exits appropriately.
        try
        {
            initializeRedissonClient();
        }
        catch ( RuntimeException re )
        {
            LOGGER.error( "Failed to initialize persister/Redis. Aborting WresJob static block.", re );
            throw re;
        }

        //If the redis client was created, set it up for recovering/storing
        //database information.  It might be possible to turn the below into
        //three calls of a generic static method.
        try
        {
            initializeRedissonDatabaseBuckets();
        }
        catch ( RuntimeException re )
        {
            LOGGER.error( "Failed to initialize Redis database buckets. Aborting WresJob static block.", re );
            if ( REDISSON_CLIENT != null )
            {
                REDISSON_CLIENT.shutdown();
            }
            throw re;
        }
    }

    /** 
     * Shared job results, stores information about jobs maintained in the Redis persister. 
     * This must be declared *after* the REDISSON_CLIENT is initialized in teh static block.
     */
    private static final JobResults JOB_RESULTS = new JobResults( CONNECTION_FACTORY,
                                                                  REDISSON_CLIENT );

    /**
     * The broker connection, created once and reused other times.
     */
    private static Connection connection = null;

    /** 
     * Guards the broker connection.
    */
    private static final Object CONNECTION_LOCK = new Object();

    /**
     * Check for connectivity to the broker and persister.
     * @throws ConnectivityException if check fails.
     */
    public void checkComponentConnectivity()
    {
        // Test connectivity to broker
        try ( Connection conn = CONNECTION_FACTORY.newConnection() )
        {
            if ( LOGGER.isInfoEnabled() )
            {
                LOGGER.info( "Successfully connected to broker at {}:{}",
                             conn.getAddress(),
                             conn.getPort() );
            }
        }
        catch ( IOException | TimeoutException e )
        {
            throw new ConnectivityException( "broker",
                                             CONNECTION_FACTORY.getHost(),
                                             CONNECTION_FACTORY.getPort(),
                                             e );
        }
        // Test connectivity to persister.
        if ( REDISSON_CLIENT != null )
        {
            String dummyId = "dummyObjectId" + System.currentTimeMillis();
            try
            {
                RLiveObjectService liveObjectService = REDISSON_CLIENT.getLiveObjectService();
                DummyLiveObject dummyLiveObject = new DummyLiveObject( dummyId );
                DummyLiveObject liveObject = liveObjectService.attach( dummyLiveObject );
                Object idRaw = liveObject.getId();
                String id = idRaw.toString();
                liveObjectService.delete( liveObject );
                LOGGER.info( "Successfully used live object service via {}:{}, got id {}",
                             redisHost,
                             redisPort,
                             id );
            }
            catch ( RuntimeException re )
            {
                throw new ConnectivityException( "redis",
                                                 redisHost,
                                                 redisPort,
                                                 re );
            }
        }
        // Test the ability to connect to the broker connections API.
        try
        {
            int workerCount = BrokerManagerHelper.getBrokerWorkerConnectionCount();
        }
        catch ( IOException | RuntimeException e )
        {
            LOGGER.error( "Attempt to get worker count failed.", e );
            throw new ConnectivityException( "Unable to connect to broker for a worker "
                                             + "count as a test. Check the broker logs. "
                                             + "Exception message: "
                                             + e.getMessage()
                                             + "." );
        }
    }

    @GET
    @Produces( "text/plain; charset=utf-8" )
    public String getWresJob()
    {
        // Test connectivity to other components 
        try
        {
            checkComponentConnectivity();
        }
        catch ( ConnectivityException ce )
        {
            LOGGER.warn( "Unable to connect to either the broker or persister. Reporting 'Down'. "
                         + "Exception message: {}",
                         ce.getMessage() );
            LOGGER.debug( FULL_EXCEPTION_TRACE, ce );
            return "Down";
        }

        //If there are no workers connected to the broker, then the service is considered down.
        try
        {
            int workerCount = BrokerManagerHelper.getBrokerWorkerConnectionCount();
            if ( workerCount <= 0 )
            {
                LOGGER.warn( "No workers are connected to the broker. Reporting 'Down'." );
                return "Down";
            }
            LOGGER.info( "Found {} workers connected to the broker", workerCount );
        }
        catch ( IOException e )
        {
            LOGGER.warn( "Unable to obtain a worker count from the broker manager. Reporting 'Down'. "
                         + " Exception message: {}",
                         e.getMessage() );
            LOGGER.debug( FULL_EXCEPTION_TRACE, e );
            return "Down";
        }
        catch ( RuntimeException re )
        {
            LOGGER.warn( "RuntimeException obtaining worker count from the broker manager. Reporting 'Down'. "
                         + " Exception message: {}",
                         re.getMessage() );
            LOGGER.debug( FULL_EXCEPTION_TRACE, re );
            return "Down";
        }

        return "Up";
    }

    @GET
    @Path( "/info" )
    @Produces( "text/html; charset=utf-8" )
    public Response getEvaluationInQueue()
    {
        int inQueueCount = JOB_RESULTS.getJobStatusCount( JobMetadata.JobState.IN_QUEUE );
        double queueUsePercentage = ( (double) inQueueCount / MAXIMUM_EVALUATION_COUNT ) * 100;
        String totalWorkers = System.getProperty( "wres.numberOfWorkers" );
        int totalWorkersNumber = 0;
        try
        {
            totalWorkersNumber = Integer.parseInt( totalWorkers );
        }
        catch ( NumberFormatException e )
        {
            LOGGER.warn( "Discovered an invalid 'wres.numberOfWorkers'. Expected a number, but got: "
                         + " {}.",
                         totalWorkers );
        }
        int inProgressCount = JOB_RESULTS.getJobStatusCount( JobMetadata.JobState.IN_PROGRESS );
        double workersUsePercentage = 0;
        if ( totalWorkersNumber != 0 )
        {
            workersUsePercentage = ( (double) inProgressCount / totalWorkersNumber ) * 100;
        }
        DecimalFormat df = new DecimalFormat( "0.00" );

        String htmlResponse = "<html><body><h1>Evaluations in Queue and In Progress</h1>"
                              + "<p>IN_QUEUE Count: "
                              + inQueueCount
                              + "</p>"
                              + "<p>IN_PROGRESS Count: "
                              + inProgressCount
                              + "</p>"
                              + "<p>Queue Used Percentage: "
                              + df.format( queueUsePercentage )
                              + "%</p>"
                              + "<p>Worker Used Percentage: "
                              + df.format( workersUsePercentage )
                              + "%</p>"
                              + "</body></html>";

        return Response.ok( htmlResponse ).build();
    }

    /**
     * Post a declaration to be validated. This calls the same validation functionality used by the 
     * core WRES and does a schema-based validation as well as some business logic checks.
     * @param projectConfig The declaration to be validated.
     * @return HTTP 200 on success with status events encoded in byte array as content. 4XX or 5XX 
     * otherwise as appropriate.
     */
    @POST
    @Path( "/validate" )
    @Consumes( APPLICATION_FORM_URLENCODED )
    @Produces( "application/octet-stream" )
    public Response postWresValidate( @FormParam( "projectConfig" ) @DefaultValue( "" ) String projectConfig )
    {
        projectConfig = reformatConfig( projectConfig );

        // Obtain the evaluation status events.
        List<EvaluationStatusEvent> events;
        try
        {
            events = DeclarationValidator.validate( projectConfig );
        }
        catch ( IOException e1 )
        {
            LOGGER.warn( UNABLE_TO_VALIDATE_PROJECT_CONFIGURATION_DUE_TO_INTERNAL_ERROR, e1 );
            return WresJob.internalServerError( UNABLE_TO_VALIDATE_PROJECT_CONFIGURATION_DUE_TO_INTERNAL_ERROR );
        }
        //Write the events to a delimited byte stream.
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try
        {
            for ( EvaluationStatusEvent event : events )
            {
                event.writeDelimitedTo( byteStream );
            }
        }
        catch ( IOException e2 )
        {
            LOGGER.warn( UNABLE_TO_WRITE_PROTOBUF_RESPONSE_BYTE_ARRAY_DUE_TO_I_O_EXCEPTION,
                         e2 );
            return WresJob.internalServerError( UNABLE_TO_WRITE_PROTOBUF_RESPONSE_BYTE_ARRAY_DUE_TO_I_O_EXCEPTION );
        }
        //Return an OK response with the byte array as the content.
        return Response.ok( byteStream.toByteArray() )
                       .build();
    }

    /**
     * Post a declaration to be validated. This calls the same validation functionality used by the
     * core WRES and does a schema-based validation as well as some business logic checks.
     * @param projectConfig The declaration to be validated.
     * @return HTTP 200 on success with an html page to display. 4XX or 5XX
     * otherwise as appropriate.
     */
    @POST
    @Path( "/validate/html" )
    @Consumes( APPLICATION_FORM_URLENCODED )
    @Produces( "text/html; charset=utf-8" )
    public Response postWresValidateHtml( @FormParam( "projectConfig" ) @DefaultValue( "" ) String projectConfig )
    {
        projectConfig = reformatConfig( projectConfig );
        try
        {
            // Obtain the evaluation status events.
            List<EvaluationStatusEvent> events = DeclarationValidator.validate( projectConfig );

            StringBuilder stringBuilder = new StringBuilder();
            events.forEach( event -> stringBuilder.append( "- " )
                                                  .append( event.getStatusLevel() )
                                                  .append( ": " )
                                                  .append( event.getEventMessage() )
                                                  .append( "\n" ) );

            return Response.status( Response.Status.OK )
                           .entity( "<!DOCTYPE html><html>"
                                    + "<head>"
                                    + "    <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\">"
                                    + "    <title>Water Resources Evaluation Service</title>"
                                    + "    <style type=\"text/css\">"
                                    + "        html, body {"
                                    + "            height: 100%;"
                                    + "            margin: 0;"
                                    + "            padding: 0.5em; }"
                                    + "        .evaluation {"
                                    + "            width: 60em;"
                                    + "            height: 55em; }"
                                    + "        p"
                                    + "        {"
                                    + "            max-width: 40em;"
                                    + "        }"
                                    + "        p, form, h1"
                                    + "        {"
                                    + "            margin: 16pt;"
                                    + "        }"
                                    + "    </style>"
                                    + "</head>"
                                    + "<title>Validation Results</title>"
                                    + "<body><h1>Declaration</h1>"
                                    + "<form action=\"/job\" method=\"post\">"
                                    + "    Evaluation project declaration full text:<br />"
                                    + "    <textarea id=\"projectConfig\" class=\"evaluation\" name=\"projectConfig\" required=\"true\">"
                                    + projectConfig
                                    + "    </textarea>"
                                    + "    <br />"
                                    + "    <button type=\"submit\" name=\"Submit\" formaction=\"/job\">Submit</button>"
                                    + "    <button type=\"submit\" name=\"Validate\" formaction=\"/job/validate/html\">Check</button><br/>"
                                    + "</form>"
                                    + "<h1> Validation Issues </h1><pre>"
                                    + stringBuilder
                                    + "</pre></body></html>" )
                           .build();
        }
        catch ( IOException e1 )
        {
            LOGGER.warn( "Unable to validate project configuration due to internal I/O error.", e1 );
            return WresJob.internalServerError( UNABLE_TO_VALIDATE_PROJECT_CONFIGURATION_DUE_TO_INTERNAL_ERROR );
        }
    }

    /**
     * Post a declaration to start a new WRES job.
     * @param projectConfig The declaration to use in a new job.
     * @param wresUser Do not use. Deprecated field.
     * @param adminToken Token required for admin commands.
     * @param verb The verb to run on the declaration, default is execute.
     * @param postInput If the caller wishes to post input, true, default false.
     * @param additionalArguments Additional arguments when no projectConfig given.
     * @return HTTP 201 on success, 4XX on client error, 5XX on server error.
     */
    @POST
    @Consumes( APPLICATION_FORM_URLENCODED )
    @Produces( "text/html; charset=utf-8" )
    public Response postWresJob( @FormParam( "projectConfig" ) @DefaultValue( "" ) String projectConfig,
                                 @Deprecated @FormParam( "userName" ) String wresUser,
                                 @FormParam( "adminToken" ) @DefaultValue( "" ) String adminToken,
                                 @FormParam( "verb" ) @DefaultValue( "execute" ) String verb,
                                 @FormParam( "postInput" ) @DefaultValue( "false" ) boolean postInput,
                                 @FormParam( "keepInput" ) @DefaultValue( "false" ) boolean keepInput,
                                 @FormParam( "additionalArguments" ) List<String> additionalArguments )
    {
        LOGGER.debug( "additionalArguments: {}", additionalArguments );
        LOGGER.info( "==========> REQUEST POSTED: verb = '{}'; postInput = {}; additional arguments = '{}'.",
                     verb,
                     postInput,
                     additionalArguments );
        
        // Reformat the configuration. 
        projectConfig = reformatConfig( projectConfig );

        // We'll be referring to response in various places.
        Response response;

        // Default priority is 0 for all tasks. Admin tasks will be given a 1 priority.
        int messagePriority = 0;

        // Identify the verb and check that its valid. If no verb
        // is specified, it will default to EXECUTE
        Pair<Verb, Response> verbOrResponse = checkAndObtainActualVerb( verb );
        if ( Objects.nonNull( verbOrResponse.getRight() ) )
        {
            return verbOrResponse.getRight();
        }
        Verb actualVerb = verbOrResponse.getLeft();

        // Check admin token if necessary.  If the admin token hash is blank, meaning no token was
        // configured via system property, then the adminToken is not necessary for any command.
        // The requires method call must be done independently of the check, because of messagePriority.
        if ( requiresAdminToken( actualVerb ) )
        {
            messagePriority = 1; //Admin priority task.
            response = checkAdminToken( actualVerb, adminToken );
            if ( Objects.nonNull( response ) )
            {
                return response;
            }
        }

        // Check the declaration if necessary.
        response = checkDeclaration( actualVerb, projectConfig, postInput );
        if ( Objects.nonNull( response ) )
        {
            return response;
        }

        //For switchdatabase, cleandatabase, and migratedatabase, I need to parse the database info
        //from the additional arguments. This extracts that info if present, or records null.
        //Regardless, if no problem occurs (i.e., the response is null), then just use list returned.
        Pair<List<String>, Response> databaseInfoOrResponse =
                checkAndObtainDatabaseSettings( actualVerb, additionalArguments );
        if ( Objects.nonNull( databaseInfoOrResponse.getRight() ) )
        {
            return databaseInfoOrResponse.getRight();
        }
        String usedDatabaseHost = databaseInfoOrResponse.getLeft().get( 0 );
        String usedDatabasePort = databaseInfoOrResponse.getLeft().get( 1 );
        String usedDatabaseName = databaseInfoOrResponse.getLeft().get( 2 );

        // A switchdatabase is handled at this point using the additional arguments settings.
        if ( actualVerb == Verb.SWITCHDATABASE )
        {
            return handleSwitchDatabase( usedDatabaseHost, usedDatabasePort, usedDatabaseName );
        }

        // For all other verbs, either use what as provided or the active database component
        // if nothing is provided.
        if ( StringUtils.isBlank( usedDatabaseHost ) )
        {
            usedDatabaseHost = activeDatabaseHost;
        }
        if ( StringUtils.isBlank( usedDatabasePort ) )
        {
            usedDatabasePort = activeDatabasePort;
        }
        if ( StringUtils.isBlank( usedDatabaseName ) )
        {
            usedDatabaseName = activeDatabaseName;
        }

        // Before registering a new job, see if there are already too many.
        Pair<Integer, Response> queueLengthOrResponse = checkAndObtainQueueLengthBeforePosting();
        if ( Objects.nonNull( queueLengthOrResponse.getRight() ) )
        {
            return queueLengthOrResponse.getRight();
        }
        int queueLength = queueLengthOrResponse.getLeft();

        // Register the new job and create the URL and URI.
        String jobId = JOB_RESULTS.registerNewJob();
        String urlCreated = "/job/" + jobId;
        URI resourceCreated;
        try
        {
            resourceCreated = new URI( urlCreated );
        }
        catch ( URISyntaxException use )
        {
            //This should only happen if there is a coding error, above.
            LOGGER.error( "Failed to create uri using {}", urlCreated, use );
            return WresJob.internalServerError();
        }

        // Build the job message to be sent to the broker.
        Job.job jobMessage = buildJobMessage( actualVerb,
                                              usedDatabaseHost,
                                              usedDatabasePort,
                                              usedDatabaseName,
                                              projectConfig,
                                              additionalArguments );

        // If the caller wishes to post input data, postInput=true, then call the
        // handle method and return its response. 
        if ( postInput )
        {
            return handlePostedInputJob( resourceCreated, urlCreated, jobId, keepInput, jobMessage );
        }

        // The rest of this method handles evaluations without posted inputs.
        // Send the declaration message to the broker. 
        response = sendDeclarationMessageOrGiveResponse( jobId, jobMessage, messagePriority );
        if ( Objects.nonNull( response ))
        {
            return response;
        }

        // Push the database info into the underlying job metadata and mark it in queue.
        JOB_RESULTS.setDatabaseName( jobId, usedDatabaseName );
        JOB_RESULTS.setDatabaseHost( jobId, usedDatabaseHost );
        JOB_RESULTS.setDatabasePort( jobId, usedDatabasePort );
        JOB_RESULTS.setKeepPostedInputData( jobId, keepInput );
        JOB_RESULTS.setInQueue( jobId );

        // Log that the declaration message was sent and return a response.
        LOGGER.info( "For verb {}, the declaration message was sent with job id {}, priority {}, and "
                     + "database host='{}', port='{}', and name='{}'. There {} jobs preceding it in the queue.",
                     actualVerb,
                     jobId,
                     messagePriority,
                     usedDatabaseHost,
                     usedDatabasePort,
                     usedDatabaseName,
                     queueLength );
        return Response.created( resourceCreated )
                       .entity( "<!DOCTYPE html><html><head><title>Evaluation job received.</title></head>"
                                + "<body><h1>Evaluation job "
                                + jobId
                                + " has been received for processing.</h1>"
                                + "<p>See <a href=\""
                                + urlCreated
                                + "\">"
                                + urlCreated
                                + A_P_BODY_HTML )
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
    @Produces( "text/html; charset=utf-8" )
    public Response getWresJobInfo( @PathParam( "jobId" ) String jobId )
    {
        Integer jobResult = JOB_RESULTS.getJobResultRaw( jobId );
        if ( Objects.isNull( jobResult ) )
        {
            return WresJob.notFound( "Could not find job " + jobId );
        }
        String jobUrl = "/job/" + jobId;
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
                            + jobId
                            + "</title></head>"
                            + "<body><h1>How to proceed with job "
                            + jobId
                            + " using the WRES HTTP API</h1>"
                            + "<p>To check whether your job has completed/succeeded/failed, GET (poll) <a href=\""
                            + statusUrl
                            + "\">"
                            + statusUrl
                            + "</a>.</p>"
                            + "<p>The possible job states are: "
                            + jobStates
                            + ".</p>"
                            + "<p>For job evaluation results, GET <a href=\""
                            + outputUrl
                            + "\">"
                            + outputUrl
                            + "</a> <strong>after</strong> status shows that the job completed successfully (exited 0).</p>"
                            + "<p>When you GET the above output url, you may specify media type text/plain in the Accept header to get a newline-delimited list of outputs.</p>"
                            + "<p>Please DELETE <a href=\""
                            + outputUrl
                            + "\">"
                            + outputUrl
                            + "</a> <strong>after</strong> you have finished reading evaluation results (by doing GET as described)."
                            + " One option (of many) is to use curl: <code>curl -X DELETE --cacert \"/path/to/wres_ca_x509_cert.pem\" https://[servername]/"
                            + outputUrl
                            + "</code> Another option is to use your client library to do the same or use developer tools in your browser to edit the /output GET request to become a DELETE request.</p>"
                            + "<p>For detailed progress (debug) information, GET <a href=\""
                            + stdoutUrl
                            + "\">"
                            + stdoutUrl
                            + "</a>"
                            + " or <a href=\""
                            + stderrUrl
                            + "\">"
                            + stderrUrl
                            + A_P_BODY_HTML )
                       .build();
    }

    /**
     * <p>Afford the client the ability to remove all resources after the client is
     * finished reading the resources it cares about.
     *
     * <p>It is important that the client not specify an arbitrary path, and that
     * the server here do the job of looking for the resources the *server* has
     * associated with the job. Otherwise, you could imagine a malicious client
     * deleting anything on the server machine that the server process has
     * access to. (The server process also should have limited privilege.)
     *
     * @param id the id of the job whose outputs to delete
     * @return 200 when successful (idempotent), 5## when problems occur
     */
    @DELETE
    @Path( "/{jobId}" )
    @Produces( "text/plain; charset=utf-8" )
    public Response deleteProjectOutputResourcesPlain( @PathParam( "jobId" ) String id )
    {
        LOGGER.debug( "Deleting input (if any) and output resources from job {}", id );

        try
        {
            WresJob.getSharedJobResults().deleteOutputs( id );

            //Make sure to force delete inputs, since the user requested it.
            WresJob.getSharedJobResults().deleteInputs( id, true );
        }
        catch ( JobNotFoundException jnfe )
        {
            LOGGER.error( "Could not find metadata for the job {}.", id, jnfe );
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }
        catch ( IOException ioe )
        {
            LOGGER.error( "Failed to delete resources for job {} at request of client.",
                          id,
                          ioe );
            return Response.serverError()
                           .entity( "Failed to delete all resources for job " + id
                                    +
                                    " though some may have been deleted before the exception occurred." )
                           .build();
        }
        catch ( IllegalStateException ise )
        {
            LOGGER.error( "Internal error deleting resources for job {}.",
                          id,
                          ise );
            return Response.serverError()
                           .entity( "Internal error deleting all resources for job " + id
                                    +
                                    " though some may have been deleted before the exception occurred." )
                           .build();
        }

        return Response.ok( "Successfully deleted input (if any) and output resources for job " + id )
                       .build();
    }

    /**
     *
     * @param message the declaration message
     * @param priority The higher the value, the higher the priority.
     * @throws IOException when connectivity, queue declaration, or publication fails
     * @throws IllegalStateException when the job does not exist in shared state
     * @throws TimeoutException if the evaluation times out
     */
    static void sendDeclarationMessage( String jobId, byte[] message, int priority )
            throws IOException, TimeoutException
    {
        // Use a shared connection across requests.
        Connection connection = WresJob.getConnection();
        try ( Channel channel = connection.createChannel() )
        {
            if ( Objects.isNull( channel ) )
            {
                LOGGER.warn( "Channel was unable to be created. There might be a leak" );
                throw new IOException( "Unable to connect to broker" );
            }

            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put( "x-max-priority", 2 );
            channel.queueDeclare( SEND_QUEUE_NAME,
                                  true,
                                  false,
                                  false,
                                  queueArgs );
            // Tell the worker where to send results.
            String jobStatusExchange = JobResults.getJobStatusExchangeName();
            AMQP.BasicProperties properties =
                    new AMQP.BasicProperties.Builder()
                                                      .replyTo( jobStatusExchange )
                                                      .correlationId( jobId )
                                                      .deliveryMode( 2 )
                                                      .priority( priority )
                                                      .build();
            // Inform the JobResults class to start looking for correlationId.
            // Share a connection, but not a channel, aim for channel-per-thread.
            // I think something needs to be watching the queue or else messages
            // end up dropping on the floor, that is why this is called prior
            // to even publishing the job at all. JobResults is a bag-o-state.
            CountDownLatch latch = JOB_RESULTS.watchForJobFeedback( jobId,
                                                                    jobStatusExchange );
            // Block until the last listener is ready and then publish the job.
            boolean await = latch.await( 30, TimeUnit.SECONDS );

            if ( !await )
            {
                JOB_RESULTS.shutdownNow();
                LOGGER.warn( "Some Channel was not able to be created. There might be a leak" );
                throw new IOException( "Unable to connect to broker" );
            }
            channel.basicPublish( "",
                                  SEND_QUEUE_NAME,
                                  properties,
                                  message );
            LOGGER.info( "Sent a message to queue '{}' with properties '{}'",
                         SEND_QUEUE_NAME,
                         properties );
            LOGGER.debug( "I sent this message to queue '{}' with properties '{}': {}.",
                          SEND_QUEUE_NAME,
                          properties,
                          message );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while waiting for job feedback watchers to bind.",
                         ie );
            Thread.currentThread().interrupt();
        }
    }

    private static Response internalServerError()
    {
        return WresJob.internalServerError( "An issue occurred that is not your fault." );
    }

    private static Response internalServerError( String message )
    {
        return Response.serverError()
                       .entity(
                                "<!DOCTYPE html><html><head><title>Our mistake</title></head><body><h1>Internal Server Error</h1><p>"
                                + message
                                + P_BODY_HTML )
                       .build();
    }

    private static Response serviceUnavailable( String message )
    {
        return Response.status( Response.Status.SERVICE_UNAVAILABLE )
                       .entity(
                                "<!DOCTYPE html><html><head><title>Service temporarily unavailable</title></head><body><h1>Service Unavailable</h1><p>"
                                + message
                                + P_BODY_HTML )
                       .build();
    }

    private static Response notFound( String message )
    {
        return Response.status( Response.Status.NOT_FOUND )
                       .entity( "<!DOCTYPE html><html><head><title>Not found</title></head><body><h1>Not Found</h1><p>"
                                + message
                                + P_BODY_HTML )
                       .build();
    }

    private static Response badRequest( String message )
    {
        return Response.status( Response.Status.BAD_REQUEST )
                       .entity( "<!DOCTYPE html><html><head><title>Bad Request</title></head><body><h1>Bad Request"
                                + "</h1><p>"
                                + message
                                + P_BODY_HTML )
                       .build();
    }

    private static Response unauthorized( String message )
    {
        return Response.status( Response.Status.UNAUTHORIZED )
                       .entity( "<!DOCTYPE html><html><head><title>Unauthorized</title></head><body><h1>Unauthorized"
                                + "</h1><p>"
                                + message
                                + P_BODY_HTML )
                       .build();
    }

    private static Connection getConnection()
            throws IOException, TimeoutException
    {
        synchronized ( CONNECTION_LOCK )
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

    private static void setDatabaseName( String databaseName )
    {
        activeDatabaseName = databaseName;
        if ( REDISSON_CLIENT != null )
        {
            RBucket<String> bucket = REDISSON_CLIENT.getBucket( "databaseName" );
            bucket.set( databaseName );
        }
    }

    private static void setDatabaseHost( String databaseHost )
    {
        activeDatabaseHost = databaseHost;
        if ( REDISSON_CLIENT != null )
        {
            RBucket<String> bucket = REDISSON_CLIENT.getBucket( "databaseHost" );
            bucket.set( databaseHost );
        }
    }

    private static void setDatabasePort( String databasePort )
    {
        activeDatabasePort = databasePort;
        if ( REDISSON_CLIENT != null )
        {
            RBucket<String> bucket = REDISSON_CLIENT.getBucket( "databasePort" );
            bucket.set( databasePort );
        }
    }

    /**
     * Initialize storage of the admin token by hashing it into memory. If hashing
     * fails, then a warning is output indicating that no admin token is needed.
     * That is not an error state, however.
     */
    private static void intializeWresAdminToken()
    {
        // If present, record the admin's token hashed.
        String adminToken = System.getProperty( ADMIN_TOKEN_SYSTEM_PROPERTY_NAME );
        if ( ( adminToken != null ) && ( !adminToken.isEmpty() ) )
        {
            try
            {
                //Create the salt
                SecureRandom random = new SecureRandom();
                random.nextBytes( WresJob.SALT );
                //Hash the token using the salt.
                KeySpec spec = new PBEKeySpec( adminToken.toCharArray(), WresJob.SALT, 65536, 128 );
                SecretKeyFactory factory = SecretKeyFactory.getInstance( "PBKDF2WithHmacSHA1" );
                WresJob.adminTokenHash = factory.generateSecret( spec ).getEncoded();
                LOGGER.info( "Admin token read from system properties and hashed successfully." );
            }
            catch ( NoSuchAlgorithmException | InvalidKeySpecException e )
            {
                LOGGER.warn( "Unable to create a hash of the amdmin token. "
                             + "Admin token will be left undefined and no admin token"
                             + " required for any verb.",
                             e );
            }
        }
    }

    /**
     * Initialize the broker connection factory static variable.
     * @throws IllegalStateException when a problem is encountered getting the SSL
     * context. The caller must then decide how to respond to that failure.
     */
    private static void initializeBrokerConnectionFactory() throws IllegalStateException
    {
        // Check for the client P12 path name. That P12 is handed off to the broker to authenticate the Tasker user.
        String p12Path = System.getProperty( Tasker.PATH_TO_CLIENT_P12_PNAME );
        if ( p12Path == null || p12Path.isEmpty() )
        {
            throw new IllegalStateException( "The system property " + Tasker.PATH_TO_CLIENT_P12_PNAME
                                             + " is not specified. It must be provided and non-empty." );
        }

        // Determine the actual broker name, whether from -D or default.
        // This initializes the broker connection factory.
        String brokerHost = BrokerHelper.getBrokerHost();
        String brokerVhost = BrokerHelper.getBrokerVhost();
        int brokerPort = BrokerHelper.getBrokerPort();
        CONNECTION_FACTORY.setHost( brokerHost );
        CONNECTION_FACTORY.setVirtualHost( brokerVhost );
        CONNECTION_FACTORY.setPort( brokerPort );
        CONNECTION_FACTORY.setSaslConfig( DefaultSaslConfig.EXTERNAL );
        SSLContext sslContext =
                BrokerHelper.getSSLContextWithClientCertificate( p12Path,
                                                                 System.getProperty( Tasker.PASSWORD_TO_CLIENT_P12 ) );
        CONNECTION_FACTORY.useSslProtocol( sslContext );
    }

    /**
     * Initialize the Redisson client to be used. If it can't be initialized, 
     * then local JVM objects are used. However, if the parameters are specified,
     * but the client fails to initialize, then an exception may be thrown. Be ready
     * to catch any RuntimeException instances, since it may require clean up. 
     * See where this is called, above.
     */
    private static void initializeRedissonClient()
    {
        List<Class<?>> registeredClasses = new ArrayList<>();
        registeredClasses.add(wres.tasker.JobMetadata.JobState.class);
        registeredClasses.add(org.redisson.RedissonReference.class);
        registeredClasses.add(byte[].class);

        Config redissonConfig = new Config();
        redissonConfig.setCodec( new KryoCodec( registeredClasses ) );
        String specifiedRedisHost = System.getProperty( REDIS_HOST_SYSTEM_PROPERTY_NAME );
        String specifiedRedisPortRaw = System.getProperty( REDIS_PORT_SYSTEM_PROPERTY_NAME );
        if ( Objects.nonNull( specifiedRedisHost ) )
        {
            WresJob.redisHost = specifiedRedisHost;
        }
        if ( Objects.nonNull( specifiedRedisPortRaw ) )
        {
            WresJob.redisPort = Integer.parseInt( specifiedRedisPortRaw );
        }
        if ( Objects.nonNull( redisHost ) )
        {
            String redisAddress = "redis://" + redisHost + ":" + redisPort;
            LOGGER.info( "Redis host specified: {}, using redis at {}",
                         specifiedRedisHost,
                         redisAddress );
            redissonConfig.useSingleServer()
                          .setAddress( redisAddress )
                          // Triple the default timeout to 9 seconds:
                          .setTimeout( 9000 )
                          // Triple the retry attempts to 9:
                          .setRetryAttempts( 9 )
                          // Triple the retry interval to 4.5 seconds:
                          .setRetryInterval( 4500 )
                          // PING ten times more frequently than default:
                          .setPingConnectionInterval( 3000 )
                          // Set SO_KEEPALIVE for what it's worth:
                          .setKeepAlive( true );
            // The reasoning here is any server thread can cause access of an
            // object in redis (e.g. output, stdout, etc), regardless of whether
            // the job is currently active. So at least that number. Then there
            // are internal threads writing to active jobs, which includes all
            // jobs in the queue, and there are four objects per job. Do not
            // forget to update docker memory limits to account for the stack
            // space required per thread here.
            redissonConfig.setNettyThreads( Tasker.MAX_SERVER_THREADS
                                            + MAXIMUM_EVALUATION_COUNT * 7 );

            //This **MUST** be the last call in the portion of this initialization
            //of the Redisson client. Once the create is called, any later attempt to 
            //fail out during start up must "clean up" the client by calling 
            //REDISSON_CLIENT.shutdown!!!
            REDISSON_CLIENT = Redisson.create( redissonConfig );
        }
        else
        {
            REDISSON_CLIENT = null;
            LOGGER.info( "No redis host specified, using local JVM objects." );
        }
    }

    /**
     * Initializes buckets that recover and store active database information.
     * At start up, this will allow the tasker to identify the active database
     * from when the tasker was previously shutdown.  An exception can be expected 
     * if there are issues communicating with the Redisson client, but they will
     * be RuntimeException instances. When called, be sure to catch those exceptions
     * and clean up by shutting down the Redisson client.
     */
    private static void initializeRedissonDatabaseBuckets()
    {
        if ( REDISSON_CLIENT != null )
        {
            RBucket<String> bucket = REDISSON_CLIENT.getBucket( "databaseName" );
            if ( bucket.get() != null && !bucket.get().isBlank() )
            {
                WresJob.activeDatabaseName = bucket.get();
            }
            else
            {
                bucket.set( activeDatabaseName );
            }
            RBucket<String> hostBucket = REDISSON_CLIENT.getBucket( "databaseHost" );
            if ( hostBucket.get() != null && !hostBucket.get().isBlank() )
            {
                WresJob.activeDatabaseHost = hostBucket.get();
            }
            else
            {
                hostBucket.set( activeDatabaseHost );
            }
            RBucket<String> portBucket = REDISSON_CLIENT.getBucket( "databasePort" );
            if ( portBucket.get() != null && !portBucket.get().isBlank() )
            {
                activeDatabasePort = portBucket.get();
            }
            else
            {
                portBucket.set( activeDatabasePort );
            }
        }
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

    /**
     * Reformat the declaration String, trimming it and making other changes as needed.
     * @param projectConfig The posted declaration String.
     * @return A reformatted version ready for use with the WRES.
    */
    private String reformatConfig( String projectConfig )
    {
        return projectConfig.trim();
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
        Connection innerConnection = WresJob.getConnection();
        try ( Channel channel = innerConnection.createChannel() )
        {
            Map<String, Object> queueArgs = new HashMap<>();
            queueArgs.put( "x-max-priority", 2 );
            AMQP.Queue.DeclareOk declareOk =
                    channel.queueDeclare( SEND_QUEUE_NAME,
                                          true,
                                          false,
                                          false,
                                          queueArgs );
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

    /**
     * Validates the supplied declaration string. Responds with a 400 BAD REQUEST if validation errors were encountered,
     * otherwise a null response.
     * response when no errors were encountered.
     * @param declarationString the declaration string
     * @param omitSources is true to omit the sources (i.e., when posting data)
     * @return the response, which contains any validation errors encountered
     */
    private Response validateDeclaration( String declarationString, boolean omitSources )
    {
        // Obtain the evaluation status events.
        try
        {
            List<EvaluationStatusEvent> events = DeclarationValidator.validate( declarationString, omitSources );

            return this.getResponseFromStatusEvents( events );
        }
        catch ( IOException e1 )
        {
            LOGGER.warn( UNABLE_TO_VALIDATE_PROJECT_CONFIGURATION_DUE_TO_INTERNAL_ERROR, e1 );
            return WresJob.internalServerError( UNABLE_TO_VALIDATE_PROJECT_CONFIGURATION_DUE_TO_INTERNAL_ERROR );
        }
    }

    /**
     * @param verb The verb specified by the user.
     * @return A Pair containing the verb to be used (left) or a Response (right). 
     * If the response is not null, then the user specified verb is invalid; return
     * the response to the user. Otherwise, use the verb identified in the pair as
     * the left value. Note that if the user provided verb is blank, then the 
     * default execute is used.
     */
    private Pair<Verb, Response> checkAndObtainActualVerb( String verb )
    {
        if ( !StringUtils.isBlank( verb )
             && Arrays.stream( Verb.values() )
                      .noneMatch( v -> verb.equalsIgnoreCase( v.name() ) ) )
        {
            return Pair.of( null,
                            WresJob.badRequest( "Verb '" + verb
                                                + "' not available." ) );
        }
        Verb actualVerb = Verb.EXECUTE;
        if ( Objects.nonNull( verb ) )
        {
            actualVerb = Verb.valueOf( verb.toUpperCase() );
        }
        return Pair.of( actualVerb, null );
    }

    /**
     * 
     * @param actualVerb The verb to check.
     * @return True if the verb requires an admin token or the admin token
     * hash at start was provided. False otherwise.
     */
    private boolean requiresAdminToken( Verb actualVerb )
    {
        // Determine if an admin token is needed.
        Set<Verb> verbsNeedingAdminToken = Set.of( Verb.CLEANDATABASE,
                                                   Verb.CONNECTTODB,
                                                   Verb.REFRESHDATABASE,
                                                   Verb.SWITCHDATABASE,
                                                   Verb.MIGRATEDATABASE );
        return adminTokenHash != null && verbsNeedingAdminToken.contains( actualVerb );
    }

    /**
     * Check the admin token against the hashed version provided at startup.
     * @param actualVerb The verb being used. Admin token is only required for a subset,
     * identified within this method. No check will be performed if the token at startup
     * was not provided or could not be hashed.
     * @param adminToken The token provided by the user.
     * @return A Response with an error if authentication fails.
     */
    private Response checkAdminToken( Verb actualVerb, String adminToken )
    {
        // Check admin token if necessary.
        if ( requiresAdminToken( actualVerb ) )
        {
            if ( adminToken == null || adminToken.isEmpty() )
            {
                String message = "The verb " + actualVerb + " requires adminToken, which was not given or was blank.";
                LOGGER.warn( message );
                return WresJob.badRequest( message );
            }
            try
            {
                KeySpec spec = new PBEKeySpec( adminToken.toCharArray(), SALT, 65536, 128 );
                SecretKeyFactory factory = SecretKeyFactory.getInstance( "PBKDF2WithHmacSHA1" );
                byte[] hash = factory.generateSecret( spec ).getEncoded();
                if ( !Arrays.equals( adminTokenHash, hash ) )
                {
                    String message = "The adminToken provided for the verb " + actualVerb
                                     + " did not match that required.  The operation is not authorized.";
                    LOGGER.warn( message );
                    return WresJob.unauthorized( message );
                }
                LOGGER.info( "For the verb, {}, the admin token matched expectations. Continuing.", actualVerb );
            }
            catch ( NoSuchAlgorithmException | InvalidKeySpecException e )
            {
                String message = "Error creating hash of adminToken; this worked before "
                                 + "it should work now. Operation "
                                 + actualVerb
                                 + " not authorized."
                                 + " Contact user support.";
                LOGGER.warn( message, e );
                return WresJob.unauthorized( message );
            }
        }
        return null;
    }

    /**
     * 
     * @param actualVerb The verb to check.
     * @return True if the verb requires declaration; false otherwise.
     */
    private boolean requiresDeclaration( Verb actualVerb )
    {
        // Determine if declaration is expected.
        Set<Verb> verbsNeedingDeclaration = Set.of( Verb.EXECUTE,
                                                    Verb.INGEST,
                                                    Verb.VALIDATE );
        return verbsNeedingDeclaration.contains( actualVerb );
    }

    /**
     * Checks for issues with the provided declaration/config. 
     * @param actualVerb The user provided verb.
     * @param projectConfig The declaration provided.
     * @param postInput True if input is to be posted, false otherwise.
     * @return A response capturing any problems discovered. If null is 
     * returned, no problems were found.
     */
    private Response checkDeclaration( Verb actualVerb, String projectConfig, boolean postInput )
    {
        // If a declaration is necessary...
        if ( requiresDeclaration( actualVerb ) )
        {
            int lengthOfProjectDeclaration = projectConfig.length();
            // Limit project config to avoid heap overflow in worker-shim
            if ( lengthOfProjectDeclaration > MAXIMUM_PROJECT_DECLARATION_LENGTH )
            {
                String projectConfigFirstChars =
                        projectConfig.substring( 0, 1000 );
                LOGGER.warn( "Received a project declaration of length {} starting with {}",
                             lengthOfProjectDeclaration,
                             projectConfigFirstChars );
                return WresJob.badRequest( "The project declaration has "
                                           + lengthOfProjectDeclaration
                                           + " characters, which is more than "
                                           + MAXIMUM_PROJECT_DECLARATION_LENGTH
                                           + ", please find a way to shrink the"
                                           + " project declaration and re-send." );
            }
            else if ( projectConfig.getBytes().length <= 1 )
            {
                LOGGER.warn( "Received a project declaration that appears to be "
                             + "one character or less." );
                return WresJob.badRequest( "The project declaration is less "
                                           + "than or equal to one byte long, "
                                           + "which is not allowed. "
                                           + "Please double-check that you "
                                           + "set the form parameter "
                                           + "'projectConfig' correctly and "
                                           + "re-send." );
            }
            else
            {
                Response error = this.validateDeclaration( projectConfig, postInput );

                if ( Objects.nonNull( error ) )
                {
                    return error;
                }
            }
        }
        return null;
    }

    /**
     * @return The queue length and a Response if a problem occurs. Return the Response
     * (right) if its not null. If it is null, then use the queue length Integer (left)
     * which is guaranteed to not be null. Note that the system property with name 
     * SKIP_QUEUE_LENGTH_CHECK_SYSTEM_PROPERTY_NAME allows for this check to always 
     * return null, ignoring the queue length.
     */
    private Pair<Integer, Response> checkAndObtainQueueLengthBeforePosting()
    {
        int queueLength;
        try
        {
            queueLength = this.getJobQueueLength();
            this.validateQueueLength( queueLength );
            return Pair.of( Integer.valueOf( queueLength ), null );
        }
        catch ( IOException | TimeoutException e )
        {
            LOGGER.error( "Attempt to check queue length failed.", e );
            return Pair.of( Integer.valueOf( -1 ), WresJob.internalServerError() );
        }
        catch ( TooManyEvaluationsInQueueException tmeiqe )
        {
            LOGGER.warn( "Did not send job, returning 503.", tmeiqe );
            return Pair.of( Integer.valueOf( -1 ),
                            WresJob.serviceUnavailable( "Too many evaluations are in the queue, try again in a moment." ) );
        }
    }

    /**
     * @param actualVerb The verb being used. Only switchdatabase, cleandatabase, and 
     * migrateddatabase allow the user to specify database information in additional 
     * arguments. 
     * @param additionalArguments The additional arguments specified by the user in the
     * request.
     * @return Both a list of strings and a response. If the response (right) is null, 
     * everything is good: use the list of strings appropriately. If its not null, then
     * a problem occurred; return the response to the user. The list is guaranteed to
     * to have three strings specifying the host, port, and database name, in that 
     * order. Any string that is null indicates the corresponding active database
     * component is used.
     */
    private Pair<List<String>, Response> checkAndObtainDatabaseSettings( Verb actualVerb,
                                                                         List<String> additionalArguments )
    {
        if ( actualVerb == Verb.SWITCHDATABASE
             || ( actualVerb == Verb.CLEANDATABASE && !additionalArguments.isEmpty() )
             || ( actualVerb == Verb.MIGRATEDATABASE && !additionalArguments.isEmpty() ) )
        {
            LOGGER.info( "Switch, clean, or migrate requested. Parsing additional arguments." );
            if ( additionalArguments.size() != 3 )
            {
                String message = "Request with verb " + actualVerb
                                 + " requires 3 additionalArguments, host, port, name, but "
                                 + additionalArguments.size()
                                 + " were provided.";
                LOGGER.warn( message );
                return Pair.of( additionalArguments, WresJob.badRequest( message ) );
            }
            return Pair.of( additionalArguments, null );
        }

        //Additional arguments are ignored, so just return a list of three null strings.
        return Pair.of( Arrays.asList( (String) null, (String) null, (String) null ), null );
    }

    /**
     * @param usedDatabaseHost The host to use, or empty to use default.
     * @param usedDatabasePort The port to use, or empty to use default.
     * @param usedDatabaseName The name ot use, or empty to use default.
     * @return The response of the switch database.
     */
    private Response handleSwitchDatabase( String usedDatabaseHost, String usedDatabasePort, String usedDatabaseName )
    {
        setDatabaseHost( usedDatabaseHost );
        setDatabasePort( usedDatabasePort );
        setDatabaseName( usedDatabaseName );
        LOGGER.info( "Database has been switched to host = '{}', port = '{}', name = '{}'.",
                     activeDatabaseHost,
                     activeDatabasePort,
                     activeDatabaseName );
        return Response.status( Response.Status.OK )
                       .entity( "<!DOCTYPE html><html><head><title>Database switched.</title></head>"
                                + "<body><h1>New database has host '"
                                + activeDatabaseHost
                                + "', port '"
                                + activeDatabasePort
                                + "', and name '"
                                + activeDatabaseName
                                + "'. Empty strings or null imply default from .yml will be used."
                                + "</h1></body></html>" )
                       .build();
    }

    /**
     * Handles a job that includes posted input, updating JOB_RESULTS appropriately and providing a
     * Response that can be returned to the user.
     * @param resourceCreated The URI of the evaluation job resource created.
     * @param urlCreated Its URL.
     * @param jobId The job id.
     * @param keepInput A flag specified by the user indicating if input is to be kept.
     * @param jobMessage The job message to sent to the broker.
     * @return The response to return to the user.
     */
    private Response handlePostedInputJob( URI resourceCreated,
                                           String urlCreated,
                                           String jobId,
                                           boolean keepInput,
                                           Job.job jobMessage )
    {
        // Pause before sending. Parse the declaration and add inputs before
        // sending along. This request will result in 201 created response
        // and the caller must send another request saying "I have finished
        // posting input."
        JOB_RESULTS.setJobMessage( jobId, jobMessage.toByteArray() );
        JOB_RESULTS.setAwaitingPostInputData( jobId );
        JOB_RESULTS.setKeepPostedInputData( jobId, keepInput );
        return Response.created( resourceCreated )
                       .entity( "<!DOCTYPE html><html><head><title>Evaluation job received.</title></head>"
                                + "<body><h1>Evaluation job "
                                + jobId
                                + " has been received, the next step is to post input data.</h1>"
                                + "<p>See <a href=\""
                                + urlCreated
                                + "\">"
                                + urlCreated
                                + A_P_BODY_HTML )
                       .build();
    }

    /**
     * @param actualVerb The verb.
     * @param usedDatabaseHost The database server handling the job.
     * @param usedDatabasePort The port of the database server handling the job.
     * @param usedDatabaseName The name of the database.
     * @param projectConfig The declaration the user provided.
     * @param additionalArguments Additional arguments, which will be added only if the 
     * job is not a clean or migrated (additional arguments are handled elsewhere in those
     * two cases).
     * @return A Job ready to be sent to the broker.
     */
    private Job.job buildJobMessage( Verb actualVerb,
                                     String usedDatabaseHost,
                                     String usedDatabasePort,
                                     String usedDatabaseName,
                                     String projectConfig,
                                     List<String> additionalArguments )
    {
        Job.job.Builder builder = Job.job.newBuilder()
                                         .setVerb( actualVerb )
                                         .setDatabaseName( usedDatabaseName )
                                         .setDatabaseHost( usedDatabaseHost )
                                         .setDatabasePort( usedDatabasePort );
        if ( requiresDeclaration( actualVerb ) )
        {
            builder.setProjectConfig( projectConfig );
        }
        else if ( actualVerb != Verb.CLEANDATABASE && actualVerb != Verb.MIGRATEDATABASE )
        {
            builder.addAllAdditionalArguments( additionalArguments );
        }
        return builder.build();
    }
    
    /**
     * Calls sendJobMessage, but catches the exception and transforms them into a response.
     * @param jobId The job id.
     * @param jobMessage The message
     * @param messagePriority Its priority.
     * @return A Response if a problem was encountered which can be forwarded to the user, or null
     * if everything was good.
     */
    private Response sendDeclarationMessageOrGiveResponse(String jobId, Job.job jobMessage, int messagePriority)
    {
        try
        {
            sendDeclarationMessage( jobId, jobMessage.toByteArray(), messagePriority );
        }
        catch ( IOException | TimeoutException e )
        {
            LOGGER.error( "Attempt to send message failed.", e );
            return WresJob.internalServerError();
        }
        return null;
    }

    /**
     * Creates a response from a collection of evaluation status events or null if no errors were encountered.
     * @param events the events
     * @return the response or null
     */

    private Response getResponseFromStatusEvents( List<EvaluationStatusEvent> events )
    {
        // Send a non-null response if errors were encountered
        if ( events.stream()
                   .noneMatch( e -> e.getStatusLevel() == EvaluationStatusEvent.StatusLevel.ERROR ) )
        {
            LOGGER.debug( "No errors were encountered when validating the declaration." );
            return null;
        }

        // Write the events to a delimited byte stream.
        try
        {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            for ( EvaluationStatusEvent event : events )
            {
                event.writeDelimitedTo( byteStream );
            }

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append( "<br>The project declaration contained the following errors, which must be fixed:</br>" );
            events.forEach( event -> stringBuilder.append( "<br>" )
                                                  .append( "- " )
                                                  .append( event.getEventMessage() )
                                                  .append( "</br>" ) );

            return WresJob.badRequest( stringBuilder.toString() );
        }
        catch ( IOException e2 )
        {
            LOGGER.warn( UNABLE_TO_WRITE_PROTOBUF_RESPONSE_BYTE_ARRAY_DUE_TO_I_O_EXCEPTION,
                         e2 );
            return WresJob.internalServerError( UNABLE_TO_WRITE_PROTOBUF_RESPONSE_BYTE_ARRAY_DUE_TO_I_O_EXCEPTION );
        }
    }

    static final class ConnectivityException extends RuntimeException
    {
        @Serial
        private static final long serialVersionUID = 4143746909778499341L;

        private ConnectivityException( String customMessage )
        {
            super( customMessage );
        }

        private ConnectivityException( String serviceName,
                                       String host,
                                       int port,
                                       Throwable cause )
        {
            super( "Failed to connect to " + serviceName
                   + " at "
                   + host
                   + ":"
                   + port,
                   cause );
        }
    }
}

