package wres.tasker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.spec.KeySpec;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.SecureRandom;


import com.google.protobuf.InvalidProtocolBufferException;
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
import jakarta.ws.rs.core.Response;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RLiveObjectService;
import org.redisson.api.RedissonClient;
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
    private static final String A_P_BODY_HTML = "</a></p></body></html>";
    private static final String P_BODY_HTML = "</p></body></html>";
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
    private static String redisHost = null;
    private static int redisPort = DEFAULT_REDIS_PORT;
    //Admin authentication
    private static final String ADMIN_TOKEN_SYSTEM_PROPERTY_NAME = "wres.adminToken";
    private static final byte[] salt = new byte[16];
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
    /**
     * A smaller-than-minimum number of bytes expected in a project declaration.
     */
    private static final int MINIMUM_PROJECT_DECLARATION_LENGTH = 100;
    //Database information
    private static String activeDatabaseName = "";
    private static String activeDatabaseHost = "";
    private static String activeDatabasePort = "";

    static
    {
        // If present, record the admin's token hashed.
        String adminToken = System.getProperty( ADMIN_TOKEN_SYSTEM_PROPERTY_NAME );
        if ( ( adminToken != null ) && ( !adminToken.isEmpty() ) )
        {
            try
            {
                //Create the salt
                SecureRandom random = new SecureRandom();
                random.nextBytes( WresJob.salt );
                //Hash the token using the salt.
                KeySpec spec = new PBEKeySpec( adminToken.toCharArray(), WresJob.salt, 65536, 128 );
                SecretKeyFactory factory = SecretKeyFactory.getInstance( "PBKDF2WithHmacSHA1" );
                WresJob.adminTokenHash = factory.generateSecret( spec ).getEncoded();
                LOGGER.info( "Admin token read from system properties and hashed successfully." );
            }
            catch ( Exception e )
            {
                LOGGER.warn( "Unable to create a hash of the amdmin token. "
                             + "Admin token will be left undefined and no admin token"
                             + " required for any verb.",
                             e );
            }
        }
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
            REDISSON_CLIENT = Redisson.create( redissonConfig );
        }
        else
        {
            REDISSON_CLIENT = null;
            LOGGER.info( "No redis host specified, using local JVM objects." );
        }
        //If the redis client was created, set it up for recovering/storing
        //database information.  It might be possible to turn the below into
        //three calls of a generic static method.
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

    /** Shared job result state, exposed below */
    private static final JobResults JOB_RESULTS = new JobResults( CONNECTION_FACTORY,
                                                                  REDISSON_CLIENT );
    private static Connection connection = null;
    /** Guards connection */
    private static final Object CONNECTION_LOCK = new Object();

    @GET
    @Produces( "text/plain; charset=utf-8" )
    public String getWresJob()
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
        return "Up";
    }

    @GET
    @Path( "/info" )
    @Produces( "text/html; charset=utf-8" )
    public Response getEvaluationInQueue()
    {
        int inQueueCount = JOB_RESULTS.getJobStatusCount( JobMetadata.JobState.IN_QUEUE );
        double queueUsePercentage = ( ( double ) inQueueCount / MAXIMUM_EVALUATION_COUNT ) * 100;
        String totalWorkers = System.getProperty( "wres.numberOfWorkers" );
        int totalWorkersNumber = 0;
        try
        {
            totalWorkersNumber = Integer.parseInt( totalWorkers );
        }
        catch ( NumberFormatException e )
        {
            LOGGER.warn( "Discovered an invalid 'wres.numberOfWorkers'. Expected a number, but got: "
                         + " {}.", totalWorkers );
        }
        int inProgressCount = JOB_RESULTS.getJobStatusCount( JobMetadata.JobState.IN_PROGRESS );
        double workersUsePercentage = 0;
        if ( totalWorkersNumber != 0 )
        {
            workersUsePercentage = ( ( double ) inProgressCount / totalWorkersNumber ) * 100;
        }
        DecimalFormat df = new DecimalFormat( "0.00" );

        String htmlResponse = "<html><body><h1>Evaluations in Queue and In Progress</h1>"
                              + "<p>IN_QUEUE Count: " + inQueueCount + "</p>"
                              + "<p>IN_PROGRESS Count: " + inProgressCount + "</p>"
                              + "<p>Queue Used Percentage: " + df.format( queueUsePercentage ) + "%</p>"
                              + "<p>Worker Used Percentage: " + df.format( workersUsePercentage ) + "%</p>"
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
        //Obtain the evaluation status events.
        Set<EvaluationStatusEvent> events;
        try
        {
            events = DeclarationValidator.validate( projectConfig );
        }
        catch ( IOException e1 )
        {
            LOGGER.warn(
                         "Unable to validate project configuration due to internal error.",
                         e1 );
            return WresJob.internalServerError(
                                                "Unable to validate project configuration due to internal error." );
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
            LOGGER.warn(
                         "Unable to write protobuf response byte array due to I/O exception.",
                         e2 );
            return WresJob.internalServerError(
                                                "Unable to write protobuf response byte array due to I/O exception." );
        }

        //Return an OK response with the byte array as the content.
        return Response.ok( byteStream.toByteArray() ).build();
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
                                 @FormParam( "additionalArguments" ) List<String> additionalArguments )
    {
        LOGGER.debug( "additionalArguments: {}", additionalArguments );
        LOGGER.info( "==========> REQUEST POSTED: verb = '{}'; postInput = {}; additional arguments = '{}'.",
                     verb,
                     postInput,
                     additionalArguments );
        // Default priority is 0 for all tasks.
        int messagePriority = 0;
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
                return WresJob.badRequest( "Verb '" + verb
                                           + "' not available." );
            }
        }
        else
        {
            // Default to "execute"
            actualVerb = Verb.EXECUTE;
        }
        // Check admin token if necessary.  If the admin token hash is blank, meaning no token was
        // configured via system property, then the adminToken is not necessary for any command.
        Set<Verb> verbsNeedingAdminToken = Set.of( Verb.CLEANDATABASE,
                                                   Verb.CONNECTTODB,
                                                   Verb.REFRESHDATABASE,
                                                   Verb.SWITCHDATABASE,
                                                   Verb.MIGRATEDATABASE );
        boolean usingToken = verbsNeedingAdminToken.contains( actualVerb );
        if ( ( adminTokenHash != null ) && ( usingToken ) )
        {
            messagePriority = 1; //Admin priority task.
            if ( adminToken == null || adminToken.isEmpty() )
            {
                String message = "The verb " + actualVerb + " requires adminToken, which was not given or was blank.";
                LOGGER.warn( message );
                return WresJob.badRequest( message );
            }
            try
            {
                KeySpec spec = new PBEKeySpec( adminToken.toCharArray(), salt, 65536, 128 );
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
            catch ( Exception e )
            {
                String message = "Error creating has of adminToken; this worked before "
                                 + "it should work now. Operation "
                                 + actualVerb
                                 + " not authorized."
                                 + " Contact user support.";
                LOGGER.warn( message, e );
                return WresJob.unauthorized( message );
            }
        }
        // Check declaration if necessary.
        Set<Verb> verbsNeedingDeclaration = Set.of( Verb.EXECUTE,
                                                    Verb.INGEST,
                                                    Verb.VALIDATE );
        boolean usingDeclaration = verbsNeedingDeclaration.contains( actualVerb );
        if ( usingDeclaration )
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
            else if ( lengthOfProjectDeclaration < MINIMUM_PROJECT_DECLARATION_LENGTH )
            {
                LOGGER.warn( "Received a project declaration of length {} (smaller than {}).",
                             lengthOfProjectDeclaration,
                             MINIMUM_PROJECT_DECLARATION_LENGTH );
                return WresJob.badRequest( "The project declaration has "
                                           + lengthOfProjectDeclaration
                                           + " characters, which too small. "
                                           + "Please double-check that you set "
                                           + "the form parameter "
                                           + "'projectConfig' correctly and "
                                           + "re-send." );
            }
        }
        //For switchdatabase, cleandatabase, and migratedatabase, I need to record the database info
        //from the additional arguments.  Running clean or migrate database with no arguments
        //is fine, however.  Thus, only parse the arguments for a cleandatabase or migratedatabase if
        //some are given.
        String usedDatabaseName = null;
        String usedDatabaseHost = null;
        String usedDatabasePort = null;
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
                return WresJob.badRequest( message );
            }
            usedDatabaseHost = additionalArguments.get( 0 );
            usedDatabasePort = additionalArguments.get( 1 );
            usedDatabaseName = additionalArguments.get( 2 );
        }
        // A switchdatabase is handled completely here.
        if ( actualVerb == Verb.SWITCHDATABASE )
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
        // All other verbs are passed through to a job handled by a worker. Set up the
        // used database information based on the ACTIVE variables.  For a clean, the user
        // can override the used database information, optionally. Those were parsed
        // above.  Thus, only set the used value if its either null or blank.
        // The used value won't be null after this, but could still be empty. If empty,
        // that means the default value configured in the .yml is being used.
        if ( usedDatabaseHost == null || usedDatabaseHost.isBlank() )
        {
            usedDatabaseHost = activeDatabaseHost;
        }
        if ( usedDatabasePort == null || usedDatabasePort.isBlank() )
        {
            usedDatabasePort = activeDatabasePort;
        }
        if ( usedDatabaseName == null || usedDatabaseName.isBlank() )
        {
            usedDatabaseName = activeDatabaseName;
        }
        // Before registering a new job, see if there are already too many.
        int queueLength = -1;
        try
        {
            queueLength = this.getJobQueueLength();
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
        Job.job jobMessage;
        // For commands EXECUTE, INGEST, VALIDATE...
        if ( usingDeclaration )
        {
            jobMessage = Job.job.newBuilder()
                                .setProjectConfig( projectConfig )
                                .setVerb( actualVerb )
                                .setDatabaseName( usedDatabaseName )
                                .setDatabaseHost( usedDatabaseHost )
                                .setDatabasePort( usedDatabasePort )
                                .build();
        }
        // All others, including CLEANDATABASE, CONNECTTODB, others?
        else
        {
            // Skip the declaration entirely, it's not needed and was not
            // validated.
            Job.job.Builder builder = Job.job.newBuilder()
                                             .setVerb( actualVerb )
                                             .setDatabaseName( usedDatabaseName )
                                             .setDatabaseHost( usedDatabaseHost )
                                             .setDatabasePort( usedDatabasePort );
            // Additional arguments are already handled when cleaning and migrating, per above.
            // No additional arguments beyond database ones are allowed in that case.
            if ( actualVerb != Verb.CLEANDATABASE && actualVerb != Verb.MIGRATEDATABASE )
            {
                for ( String arg : additionalArguments )
                {
                    builder.addAdditionalArguments( arg );
                }
            }
            jobMessage = builder.build();
        }
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
                                    + jobId
                                    + " has been received, the next step is to post input data.</h1>"
                                    + "<p>See <a href=\""
                                    + urlCreated
                                    + "\">"
                                    + urlCreated
                                    + A_P_BODY_HTML )
                           .build();
        }
        try
        {
            sendDeclarationMessage( jobId, jobMessage.toByteArray(), messagePriority );
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
        // Push the database info into the underlying job metadata and mark it in queue.
        JOB_RESULTS.setDatabaseName( jobId, usedDatabaseName );
        JOB_RESULTS.setDatabaseHost( jobId, usedDatabaseHost );
        JOB_RESULTS.setDatabasePort( jobId, usedDatabasePort );
        JOB_RESULTS.setInQueue( jobId );
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
                            + jobStates.toString()
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
     *
     * @param message
     * @param priority The higher the value, the higher the priority.
     * @throws IOException when connectivity, queue declaration, or publication fails
     * @throws IllegalStateException when the job does not exist in shared state
     * @throws TimeoutException
     */
    static void sendDeclarationMessage( String jobId, byte[] message, int priority )
            throws IOException, TimeoutException
    {
        // Use a shared connection across requests.
        Connection connection = WresJob.getConnection();
        try ( Channel channel = connection.createChannel() )
        {
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
            latch.await();
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
                       .entity(
                               "<!DOCTYPE html><html><head><title>Bad Request</title></head><body><h1>Bad Request</h1><p>"
                               + message
                               + P_BODY_HTML )
                       .build();
    }

    private static Response unauthorized( String message )
    {
        return Response.status( Response.Status.UNAUTHORIZED )
                       .entity(
                               "<!DOCTYPE html><html><head><title>Unauthorized</title></head><body><h1>Unauthorized</h1><p>"
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
        private static final long serialVersionUID = 4143746909778499341L;

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
