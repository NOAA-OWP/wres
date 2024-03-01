package wres.server;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.inject.Singleton;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.server.ChunkedOutput;
import org.redisson.Redisson;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.messages.generated.EvaluationStatusOuterClass.EvaluationStatus.*;

import wres.ExecutionResult;
import wres.Functions;
import wres.Main;
import wres.io.database.locking.DatabaseLockFailed;
import wres.messages.generated.EvaluationStatusOuterClass;
import wres.pipeline.Canceller;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.system.DatabaseSettings;
import wres.system.SettingsFactory;
import wres.system.SystemSettings;

/**
 * There are two main functions supported by this resource:
 * - Simple Evaluations
 * evaluation/evaluate allows a user to pass along a project config and receive output
 * - Complete Evaluations
 * evaluation/open
 * evaluation/start
 * evaluation/close
 * Takes in a job message as a byte[] and can support features like sending std out/error and database management
 * but it must be opened and closed by the caller and sent as a job instead of just a project config
 * NOTE: Currently there is a 1:1 relationship between a server and an evaluation. That is to say each server can only
 * serve 1 evaluation at a time. For this reason we are currently using Atomic variables to track certain states and
 * information on an evaluation.
 */

@Path( "/evaluation" )
@Singleton
public class EvaluationService implements ServletContextListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationService.class );

    private static final Random RANDOM =
            new Random( System.currentTimeMillis() );

    private static final String REDIS_HOST_SYSTEM_PROPERTY_NAME = "wres.redisHost";
    private static final String REDIS_PORT_SYSTEM_PROPERTY_NAME = "wres.redisPort";
    private static final String REDIS_TIMEOUT_IN_HOURS_SYSTEM_PROPERTY_NAME = "wres.redisTimeoutInHours";

    /** A shared map of job metadata by ID */
    private static final RMapCache<String, EvaluationMetadata> EVALUATION_METADATA_MAP;

    /** Stream identifier. */
    public enum WhichStream
    {
        /** Standard error stream. */
        STDERR,
        /** Standard output stream. */
        STDOUT
    }

    private static final AtomicReference<EvaluationStatusOuterClass.EvaluationStatus> EVALUATION_STAGE =
            new AtomicReference<>( AWAITING );

    private static final AtomicLong EVALUATION_ID = new AtomicLong( -1 );

    private static final AtomicReference<Canceller> EVALUATION_CANCELLER = new AtomicReference<>();

    private Future<Response> evaluationResponse;

    private static final int ONE_MINUTE_IN_MILLISECONDS = 60000;

    private static Thread timeoutThread;

    private ChunkedOutput<String> errorStream;

    private ByteArrayOutputStream byteErrorStream;

    private ChunkedOutput<String> outStream;

    private ByteArrayOutputStream byteOutputStream;

    /** A shared bag of output resource references by request id */
    // The cache is here for expedience, this information could be persisted
    // elsewhere, such as a database or a local file. Cleanup of outputs is a
    // related concern.
    private static final Cache<Long, EvaluationMetadata> CAFFEINE_CACHE;

    private static RedissonClient redissonClient;
    private static final int DEFAULT_REDIS_PORT = 6379;
    private static String redisHost = null;
    private static int redisPort = DEFAULT_REDIS_PORT;

    private static int redisEntryTimeoutInHours = 12;

    static
    {
        Config redissonConfig = new Config();
        String specifiedRedisHost = System.getProperty( REDIS_HOST_SYSTEM_PROPERTY_NAME );
        String specifiedRedisPortRaw = System.getProperty( REDIS_PORT_SYSTEM_PROPERTY_NAME );
        String timeout = System.getProperty( REDIS_TIMEOUT_IN_HOURS_SYSTEM_PROPERTY_NAME );
        if ( Objects.nonNull( timeout ) )
        {
            EvaluationService.redisEntryTimeoutInHours = Integer.parseInt( timeout );
        }
        if ( Objects.nonNull( specifiedRedisHost ) )
        {
            EvaluationService.redisHost = specifiedRedisHost;
        }
        if ( Objects.nonNull( specifiedRedisPortRaw ) )
        {
            EvaluationService.redisPort = Integer.parseInt( specifiedRedisPortRaw );
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

            redissonClient = Redisson.create( redissonConfig );
            EVALUATION_METADATA_MAP = redissonClient.getMapCache( "evalMetadataById" );
            EVALUATION_METADATA_MAP.setMaxSize( 50 );
            CAFFEINE_CACHE = null;
        }
        else
        {
            CAFFEINE_CACHE = Caffeine.newBuilder()
                                     .maximumSize( 100 ).build();
            EVALUATION_METADATA_MAP = null;
            LOGGER.info( "No redis host specified, using local Caffeine cache." );
        }
    }

    private static SystemSettings systemSettings = SettingsFactory.createSettingsFromDefaultXml();

    /**
     * Public constructor to allow registration.
     */
    public EvaluationService()
    {
        //Needed to register this servlet
    }

    /**
     * Returns a help page for usage examples of a personal server
     * @return Help message
     */
    @GET
    @Produces( MediaType.TEXT_HTML )
    public Response help()
    {
        InputStream resourceAsStream =
                EvaluationService.class.getResourceAsStream( URI.create( "/evaluationHelp.html" ).getPath() );

        return Response.ok( resourceAsStream )
                       .build();
    }

    /**
     * Function to simply track the good status of the server
     * @return Good Response
     */
    @GET
    @Path( "/heartbeat" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response heartbeat()
    {
        return Response.ok( "The Server is Up \n" )
                       .build();
    }

    /**
     * Function to simply track the good status of the server
     * @return Good Response
     */
    @GET
    @Path( "/readyForWork" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response readyForWork()
    {
        if ( ( EVALUATION_STAGE.get().equals( CLOSED ) || EVALUATION_STAGE.get().equals( AWAITING ) )
             && EVALUATION_ID.get() == -1 )
        {
            return Response.ok( "The Server can accept a new job \n" )
                           .build();
        }

        return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                       .entity( "The Server has not cleaned up from the previous job \n" )
                       .build();
    }

    /**
     * Gets the status of a specific Evaluation
     * @param id The ID of the Evaluation to check the status of (Must exist)
     * @return The Atomic evaluationStage of the Evaluation
     */
    @GET
    @Path( "/status/{id}" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response getStatus( @PathParam( "id" ) Long id )
    {
        // Check if the request is for an ongoing evaluation or one that's cached
        if ( EVALUATION_ID.get() != id )
        {
            // If evaluation is still in cache, pull results from there
            EvaluationMetadata cachedEntry = getCachedEntry( id );
            if ( Objects.nonNull( cachedEntry.getStatus() ) )
            {
                LOGGER.info( "Returning status from cache" );
                return Response.ok( cachedEntry.getStatus() ).build();
            }
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Unable to find project status with that ID. Check the persisted logs " + id )
                           .build();
        }
        return Response.ok( EVALUATION_STAGE.get().toString() )
                       .build();
    }

    /**
     * Redirect the standard out stream and return that to the user in a ChunkedOutput
     * @param id ID of the evaluation we are trying to track with this
     * @return the response
     */
    @GET
    @Path( "/stdout/{id}" )
    @Produces( "application/octet-stream" )
    public Response getOutStream( @PathParam( "id" ) Long id )
    {
        // Check if the request is for an ongoing evaluation or one that's cached
        if ( EVALUATION_ID.get() != id )
        {
            // If evaluation is still in cache, just pull results from there
            EvaluationMetadata cachedEntry = getCachedEntry( id );
            if ( Objects.nonNull( cachedEntry.getStdout() ) )
            {
                LOGGER.info( "Returning stdout from cache" );
                return Response.ok( cachedEntry.getStdout() ).build();
            }
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Unable to find project stdout logs with that ID. Check the persisted logs " + id )
                           .build();
        }

        return Response.ok( outStream )
                       .build();
    }

    /**
     * Redirect the standard err stream and return that to the user in a ChunkedOutput
     * @param id ID of the evaluation we are trying to track with this
     * @return the response
     */
    @GET
    @Path( "/stderr/{id}" )
    @Produces( "application/octet-stream" )
    public Response getErrorStream( @PathParam( "id" ) Long id )
    {
        // Check if the request is for an ongoing evaluation or one that's cached
        if ( EVALUATION_ID.get() != id )
        {
            // If evaluation is still in cache, just pull results from there
            EvaluationMetadata cachedEntry = getCachedEntry( id );
            if ( Objects.nonNull( cachedEntry.getStderr() ) )
            {
                LOGGER.info( "Returning stderr from cache" );
                return Response.ok( cachedEntry.getStderr() ).build();
            }
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Unable to find project stderr logs with that ID. Check the persisted logs " + id )
                           .build();
        }

        return Response.ok( errorStream )
                       .build();
    }

    /**
     * Starts an evaluation and returns the ID of the evaluation created.
     * @param projectDeclaration the evaluation declaration we are exectuing
     * @param host the database host if a user wants to specify
     * @param name the database name if a user wants to specify
     * @param port the database port if a user wants to specify
     * @return ID of evaluation kicked off
     */
    @POST
    @Path( "/startEvaluation" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response createEvaluation( String projectDeclaration,
                                      @QueryParam( "dbHost" ) String host,
                                      @QueryParam( "dbName" ) String name,
                                      @QueryParam( "dbPort" ) String port )
    {
        if ( EVALUATION_ID.get() > 0
             || !( EVALUATION_STAGE.get().equals( CLOSED )
                   || EVALUATION_STAGE.get().equals( AWAITING ) ) )
        {
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity( String.format(
                                   "There was a project found with id: %d in state: %s. Please /close/{ID} this project first",
                                   EVALUATION_ID.get(),
                                   EVALUATION_STAGE.get() ) )
                           .build();
        }

        // Guarantee a positive number. Using Math.abs would open up failure
        // in edge cases. A while loop seems complex. Thanks to Ted Hopp
        // on StackOverflow question id 5827023.
        long projectId = RANDOM.nextLong() & Long.MAX_VALUE;
        EVALUATION_STAGE.set( OPENED );
        EVALUATION_ID.set( projectId );

        // Sets up stream redirect to avoid missing log statements
        streamRedirectSetup();

        // Start a timer to prevent abandoned jobs from blocking the queue
        evaluationTimeoutThread();

        updateSystemSettingsIfNeeded( host, name, port );

        evaluationResponse = startEvaluation( projectDeclaration, projectId );

        EvaluationMetadata evaluationMetadata = new EvaluationMetadata( String.valueOf( projectId ) );
        evaluationMetadata.setStatus( OPENED );
        persistInformation( projectId, evaluationMetadata );

        return Response.ok( Response.Status.CREATED )
                       .entity( projectId )
                       .build();
    }

    /**
     * Closes an evaluation that was Opened (Wont interupt ongoing evaluations)
     * @return Good Response
     */
    @POST
    @Path( "/close" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response closeEvaluation()
    {
        if ( !EVALUATION_STAGE.get().equals( COMPLETED ) )
        {
            LOGGER.info( "No Evaluation to close" );
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "There is no evaluation that needs to be closed" )
                           .build();
        }
        close();
        return Response.ok( "Evaluation closed" )
                       .build();
    }

    /**
     * Attempts to get the Response of the current ongoing evaluation will hang till task finishes
     * @param id The ID of the evaluation to get
     * @return The output paths of the evaluation
     */
    @GET
    @Path( "/getEvaluation/{id}" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response getEvaluationResult( @PathParam( "id" ) Long id )
    {
        if ( EVALUATION_ID.get() != id || Objects.isNull( evaluationResponse ) )
        {
            EvaluationMetadata cachedEntry = getCachedEntry( id );
            if ( Objects.nonNull( cachedEntry.getOutputs() ) )
            {
                LOGGER.info( "Returning outputs from cache" );
                StreamingOutput streamingOutput = outputStream -> {
                    Writer writer = new BufferedWriter( new OutputStreamWriter( outputStream ) );
                    for ( java.nio.file.Path path : cachedEntry.getOutputs() )
                    {
                        writer.write( path.toString() + "\n" );
                    }
                    writer.flush();
                    writer.close();
                };

                return Response.ok( streamingOutput ).build();
            }
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity( "There was not an evaluation with that ID running or persisted in cache." )
                           .build();
        }

        try
        {
            return evaluationResponse.get();
        }
        catch ( ExecutionException e )
        {
            String message = "Unable to get the evaluation response due to unhandled exception: " + e.getCause();
            LOGGER.warn( message );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Unable to get the evaluation response" )
                           .build();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Get request was interrupted" )
                           .build();
        }
    }

    /**
     * cancel an ongoing Evaluation
     * @param id the id of a job to be canceled
     * @return the state of the cancel
     */
    @POST
    @Path( "/cancel/{id}" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response cancelEvaluation( @PathParam( "id" ) Long id )
    {

        // Check that there is an ongoing evaluation to be canceled
        if ( EVALUATION_ID.get() != id || !EVALUATION_STAGE.get().equals( ONGOING ) )
        {
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity( "There was not an ongoing evaluation with that ID to cancel" )
                           .build();
        }

        // Cancel evaluation tied to this canceler
        EVALUATION_CANCELLER.get().cancel();

        return Response.status( Response.Status.OK )
                       .entity( "Successfully canceled evaluation" )
                       .build();
    }

    /**
     * Kicks off a database Migration
     * @param host the database host if a user wants to specify
     * @param name the database name if a user wants to specify
     * @param port the database port if a user wants to specify
     * @return Good response
     */
    @POST
    @Path( "/migrateDatabase" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response migrateDatabase( @QueryParam( "dbHost" ) String host,
                                     @QueryParam( "dbName" ) String name,
                                     @QueryParam( "dbPort" ) String port )
    {
        if ( !systemSettings.isUseDatabase() )
        {
            throw new IllegalArgumentException(
                    "This is an in-memory execution. Cannot migrate a database because there "
                    + "is no database to migrate." );
        }

        // Checks if database information has changed in the jobMessage and swap to that database
        updateSystemSettingsIfNeeded( host, name, port );

        Functions.SharedResources sharedResources =
                new Functions.SharedResources( systemSettings,
                                               "migratedatabase",
                                               Collections.emptyList() );

        logJobHeaderInformation();
        ExecutionResult result = Functions.migrateDatabase( sharedResources );

        if ( result.failed() )
        {
            String errorMessage = "Failed to migrate the database.";
            LOGGER.info( errorMessage );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Unable to migrate the database with the error: " )
                           .build();
        }

        return Response.ok( "Database Migrated" )
                       .build();
    }

    /**
     * Kicks off a database Clean
     * @param host the database host if a user wants to specify
     * @param name the database name if a user wants to specify
     * @param port the database port if a user wants to specify
     * @return Good response
     */
    @POST
    @Path( "/cleanDatabase" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response cleanDatabase( @QueryParam( "dbHost" ) String host,
                                   @QueryParam( "dbName" ) String name,
                                   @QueryParam( "dbPort" ) String port )
    {
        if ( !systemSettings.isUseDatabase() )
        {
            throw new IllegalArgumentException( "This is an in-memory execution. Cannot clean a database because there "
                                                + "is no database to clean." );
        }

        // Checks if database information has changed in the jobMessage and swap to that database
        updateSystemSettingsIfNeeded( host, name, port );

        try
        {
            Functions.SharedResources sharedResources =
                    new Functions.SharedResources( systemSettings,
                                                   "cleandatabase",
                                                   Collections.emptyList() );

            logJobHeaderInformation();
            Functions.cleanDatabase( sharedResources );
        }
        catch ( IllegalStateException | DatabaseLockFailed se )
        {
            String errorMessage = "Failed to clean the database. Unable to acquire lock or communicate with database";
            InternalWresException e = new InternalWresException( errorMessage, se );
            LOGGER.info( errorMessage, se );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Unable to clean the database with the error: "
                                    + e.getMessage() )
                           .build();
        }

        return Response.ok( "Database cleaned" )
                       .build();
    }

    /**
     * @param id the evaluation job identifier
     * @param resourceName the resource name
     * @return the resource
     */

    @GET
    @Path( "/{id}/{resourceName}" )
    public Response getProjectResource( @PathParam( "id" ) Long id,
                                        @PathParam( "resourceName" ) String resourceName )
    {
        Set<java.nio.file.Path> paths = getCachedEntry( id ).getOutputs();
        String type = MediaType.TEXT_PLAIN_TYPE.getType();

        if ( paths == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find project " + id )
                           .build();
        }

        for ( java.nio.file.Path path : paths )
        {
            if ( path.getFileName().toString().equals( resourceName ) )
            {
                File actualFile = path.toFile();

                if ( !actualFile.exists() )
                {
                    return Response.status( Response.Status.NOT_FOUND )
                                   .entity( "Could not see resource "
                                            + resourceName
                                            + "." )
                                   .build();
                }

                if ( !actualFile.canRead() )
                {
                    return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                                   .entity( "Found but could not read resource "
                                            + resourceName
                                            + "." )
                                   .build();
                }

                type = EvaluationService.getContentType( path );

                return Response.ok( actualFile )
                               .type( type )
                               .build();
            }
        }

        return Response.status( Response.Status.NOT_FOUND )
                       .type( type )
                       .entity( "Could not find resource " + resourceName
                                + " from project "
                                + id )
                       .build();
    }

    /**
     * Starts an opened Evaluation
     * @param projectDeclaration the declaration we are evaluating
     * @param id the evaluation identifier
     * @return a Future<Response> of the evaluation
     */
    private Future<Response> startEvaluation( String projectDeclaration, Long id )
    {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        return executorService.submit( () -> {
            EVALUATION_STAGE.set( ONGOING );
            EvaluationMetadata evaluationMetadata = getCachedEntry( id );
            evaluationMetadata.setStatus( ONGOING );
            persistInformation( id, evaluationMetadata );

            LOGGER.info( "Kicking off evaluation on server with the internal ID of: {}", id );
            Set<java.nio.file.Path> outputPaths;
            Canceller canceller = Canceller.of();
            try
            {
                // Print system setting at the top of the job log to help debug
                logJobHeaderInformation();

                // Execute an evaluation
                EVALUATION_CANCELLER.set( canceller );

                Functions.SharedResources sharedResources =
                        new Functions.SharedResources( systemSettings,
                                                       "execute",
                                                       List.of( projectDeclaration ) );

                ExecutionResult result = Functions.evaluate( sharedResources, EVALUATION_CANCELLER.get() );

                // We rely on these log statements for tying IDs together easier while debugging.
                // Check findJobID.sh in the scripts directory to see how this is used before changing/removing
                LOGGER.info( "Evaluation with internal ID {} and evaluation ID of {} has returned",
                             id,
                             result.getEvaluationId() );

                // get files written
                outputPaths = result.getResources();

                // Persist output into cache
                evaluationMetadata = getCachedEntry( id );
                evaluationMetadata.setStatus( COMPLETED );
                evaluationMetadata.setOutputs( outputPaths );
                persistInformation( id, evaluationMetadata );

                // Check if evaluation was canceled or failed
                if ( result.cancelled() )
                {
                    String failureMessage = "The evaluation was canceled";
                    // Print the stack exception so it is stored in the stdOut of the job
                    LOGGER.info( failureMessage );
                    EVALUATION_STAGE.set( COMPLETED );

                    return Response.status( Response.Status.CONFLICT )
                                   .entity( failureMessage )
                                   .build();
                }
                else if ( result.failed() )
                {
                    String failureMessage = "The evaluation failed with the following stack trace: ";
                    // Print the stack exception so it is stored in the stdOut of the job
                    LOGGER.info( failureMessage, result.getException() );

                    EVALUATION_STAGE.set( COMPLETED );
                    return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                                   .entity( failureMessage + result.getException() )
                                   .build();
                }
            }
            catch ( UserInputException e )
            {
                String failureMessage = "I received something I could not parse. The top-level exception was";
                LOGGER.info( failureMessage, e );
                // Persist output into cache
                evaluationMetadata = getCachedEntry( id );
                evaluationMetadata.setStatus( COMPLETED );
                persistInformation( id, evaluationMetadata );
                return Response.status( Response.Status.BAD_REQUEST )
                               .entity( failureMessage + e.getMessage() )
                               .build();
            }
            catch ( InternalWresException iwe )
            {
                String failureMessage = "WRES experienced an internal issue. The top-level exception was";
                LOGGER.info( failureMessage, iwe );
                // Persist output into cache
                evaluationMetadata = getCachedEntry( id );
                evaluationMetadata.setStatus( COMPLETED );
                persistInformation( id, evaluationMetadata );
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                               .entity( failureMessage + iwe.getMessage() )
                               .build();
            }

            // Put output paths in a stream to send to user
            StreamingOutput streamingOutput = outputStream -> {
                Writer writer = new BufferedWriter( new OutputStreamWriter( outputStream ) );
                for ( java.nio.file.Path path : outputPaths )
                {
                    writer.write( path.toString() + "\n" );
                }
                writer.flush();
                writer.close();
            };

            EVALUATION_STAGE.set( COMPLETED );

            return Response.ok( streamingOutput )
                           .build();
        } );
    }

    /**
     * Sets up the redirect of the std out and err stream during the opening of the job
     */
    private void streamRedirectSetup()
    {
        // Create new stream to redirect output to
        byteOutputStream = new ByteArrayOutputStream();
        PrintStream outPrintStream = new PrintStream( byteOutputStream );
        outStream = new ChunkedOutput<>( String.class );

        // Create new stream to redirect output to
        byteErrorStream = new ByteArrayOutputStream();
        PrintStream errorPrintStream = new PrintStream( byteErrorStream );
        errorStream = new ChunkedOutput<>( String.class );

        // redirect the error stream
        System.setErr( errorPrintStream );

        // redirect the out stream
        System.setOut( outPrintStream );

        // Start the thread that will send the information from the byteArrayOutputStream to the output ChunkedOutput we are returning
        startChunkedOutputThread( byteOutputStream, outStream, WhichStream.STDOUT );

        // Start the thread that will send the information from the byteArrayOutputStream to the output ChunkedOutput we are returning
        startChunkedOutputThread( byteErrorStream, errorStream, WhichStream.STDERR );
        LOGGER.info( "Thread redirect setup finished" );
    }

    /**
     * Creates a thread that will send messages from the stream to the provided ChunkedOutput.
     * @param redirectStream the redirect stream
     * @param output the chunked outputs stream
     * @param whichStream if this is the stdout or stderr thread
     */
    private void startChunkedOutputThread( ByteArrayOutputStream redirectStream,
                                           ChunkedOutput<String> output,
                                           WhichStream whichStream )
    {
        new Thread( () -> {
            try ( redirectStream; output )
            {
                int offset = 0;

                while ( !EVALUATION_STAGE.get().equals( CLOSED ) && !EVALUATION_STAGE.get().equals( AWAITING ) )
                {
                    offset = writeOutput( redirectStream, output, offset, whichStream );
                }

                // After the evaluation is closed, send any more information missed in the last loop
                // This helps avoid logs being cut off if alot of information is sent at the end of an evaluation
                writeOutput( redirectStream, output, offset, whichStream );
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Unable to start a chunked output thread with the exception:", e );
            }
        } ).start();
    }

    /**
     * Helper to write output from the redirectStream to the output skipping the offset
     * @param redirectStream the stream we are taking information from
     * @param output the place we are writting the output
     * @param offset How much of the redirectStream to skip
     * @param whichStream if this is the stdout or stderr out stream
     * @return the new offset
     * @throws IOException IOexception when writting to the output
     */
    private int writeOutput( ByteArrayOutputStream redirectStream,
                             ChunkedOutput<String> output,
                             int offset,
                             WhichStream whichStream )
            throws IOException
    {
        // Write the cache message to the ChunkedOutput being sent to the shim
        ByteArrayInputStream byteArrayInputStream =
                new ByteArrayInputStream( redirectStream.toByteArray() );
        // Skip the content in the message already sent
        long skip = byteArrayInputStream.skip( offset );
        String message = new String( byteArrayInputStream.readAllBytes(), StandardCharsets.UTF_8 );

        // If we skipped successfully and the resulting string isn't empty send SSE
        if ( offset == skip && !message.isEmpty() )
        {
            // Persist the log in cache
            EvaluationMetadata evaluationMetadata = getCachedEntry( EVALUATION_ID.get() );
            if ( whichStream.equals( WhichStream.STDOUT ) )
            {
                evaluationMetadata.setStdout( redirectStream.toString( StandardCharsets.UTF_8 ) );
            }
            else
            {
                evaluationMetadata.setStderr( redirectStream.toString( StandardCharsets.UTF_8 ) );
            }
            persistInformation( EVALUATION_ID.get(), evaluationMetadata );

            offset += message.length();
            output.write( message );
        }
        return offset;
    }

    /**
     * Closes the current evaluation by setting all atomic values to expected states and resetting standard streams
     */
    private static void close()
    {
        LOGGER.info( "Closing Evaluation" );
        // Project closed gracefully, stop the timeout thread
        if (Objects.nonNull( timeoutThread ) )
        {
            timeoutThread.interrupt();
        }

        // Set Atomic values to a state to accept new projects
        EVALUATION_STAGE.set( CLOSED );
        EVALUATION_ID.set( -1 );

        // Reset Standard Streams
        System.setOut( new PrintStream( new FileOutputStream( FileDescriptor.out ) ) );
        System.setErr( new PrintStream( new FileOutputStream( FileDescriptor.err ) ) );
    }

    /**
     * Creates a thread that will time out stale evaluations if they do not change status as expected in a prompt manner
     * This is used to stop worker servers from being occupied by an errant Evaluation
     */
    private static void evaluationTimeoutThread()
    {
        Runnable timeoutRunnable = () -> {
            while ( !EVALUATION_STAGE.get().equals( CLOSED ) )
            {
                try
                {
                    Thread.sleep( ONE_MINUTE_IN_MILLISECONDS );
                    if ( EVALUATION_STAGE.get().equals( OPENED ) || EVALUATION_STAGE.get().equals( COMPLETED ) )
                    {
                        close();
                    }
                }
                catch ( InterruptedException interruptedException )
                {
                    LOGGER.info( "Evaluation was closed so thread was interrupted.", interruptedException );
                    Thread.currentThread().interrupt();
                }
            }
        };

        timeoutThread = new Thread( timeoutRunnable );
        timeoutThread.start();
    }

    private void logJobHeaderInformation()
    {
        // Print some information about the software version and runtime
        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( Main.getVersionDescription() );
            LOGGER.info( Main.getVerboseRuntimeDescription( systemSettings ) );
        }
    }

    /**
     * If the job contains database information different from the current database then change the systemSettings
     * @param host the database host if a user wants to specify
     * @param name the database name if a user wants to specify
     * @param port the database port if a user wants to specify
     */
    private static void updateSystemSettingsIfNeeded( String host, String name, String port )
    {
        boolean databaseChangeDetected = false;

        DatabaseSettings databaseConfiguration = systemSettings.getDatabaseConfiguration();
        DatabaseSettings.DatabaseSettingsBuilder databaseBuilder = databaseConfiguration.toBuilder();

        // Attempt to apply any changes detected
        if ( Objects.nonNull( name ) && !name.isEmpty() && !name.equals( databaseConfiguration.getDatabaseName() ) )
        {
            databaseBuilder.databaseName( name );
            databaseChangeDetected = true;
        }
        if ( Objects.nonNull( host ) && !host.isEmpty() && !host.equals( databaseConfiguration.getHost() ) )
        {
            databaseBuilder.host( host );
            databaseChangeDetected = true;
        }
        if ( Objects.nonNull( port ) && !port.isEmpty()
             && !port.equals( String.valueOf( databaseConfiguration.getPort() ) ) )
        {
            databaseBuilder.port( Integer.parseInt( port ) );
            databaseChangeDetected = true;
        }

        // If we did change something, update Evaluator
        if ( databaseChangeDetected )
        {
            // DatabaseSettings have fields that are conditionally created in respect to host, port, and name; update those here
            String passwordOverrides = SettingsFactory.getPasswordOverrides( databaseBuilder.build() );
            if ( passwordOverrides != null )
            {
                databaseBuilder.password( passwordOverrides );
            }

            systemSettings = systemSettings.toBuilder().databaseConfiguration( databaseBuilder.build() ).build();
        }
    }

    /**
     * Helper method to persist an EvaluationMetadata into cache
     * @param id key of the EvaluationMetadata
     * @param evaluationMetadata The new information to persist with this associated key
     */
    private static void persistInformation( long id, EvaluationMetadata evaluationMetadata )
    {
        if ( Objects.isNull( EVALUATION_METADATA_MAP ) )
        {
            CAFFEINE_CACHE.put( id, evaluationMetadata );
        }
        // RedissonClient is present, use that
        else
        {
            EVALUATION_METADATA_MAP.fastPut( String.valueOf( id ),
                                             evaluationMetadata,
                                             EvaluationService.redisEntryTimeoutInHours,
                                             TimeUnit.HOURS );
        }
    }

    /**
     * Helper method to persist an EvaluationMetadata into cache
     * @param id key of the EvaluationMetadata
     * @return EvaluationMetadata A collection of stdout/err and outputs
     */
    private static EvaluationMetadata getCachedEntry( long id )
    {
        EvaluationMetadata cachedEntry;
        if ( Objects.isNull( EVALUATION_METADATA_MAP ) )
        {
            cachedEntry = CAFFEINE_CACHE.getIfPresent( id );
        }
        else
        {
            cachedEntry = EVALUATION_METADATA_MAP.get( String.valueOf( id ) );
        }
        return Objects.nonNull( cachedEntry ) ? cachedEntry : new EvaluationMetadata( String.valueOf( id ) );
    }

    /**
     * Examines the content type of the resource at the path.
     * @param path the path
     * @return the content type
     */

    private static String getContentType( java.nio.file.Path path )
    {
        String type = null;

        try
        {
            // Successfully translates .nc to application/x-netcdf
            // Successfully translates .csv to text/csv
            String probedType = Files.probeContentType( path );

            if ( probedType != null )
            {
                type = probedType;
            }
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Could not probe content type of {}", path, ioe );
        }

        return type;
    }

    /**
     * Since the database can be different than the one provided at creation through the database clean and swap
     * tear down the current database upon the destruction of this servlet
     *
     * @param event the ServletContextEvent containing the ServletContext that is being destroyed
     *
     */
    @Override
    public void contextDestroyed( ServletContextEvent event )
    {
        // Shuts down cache instance (Not server)
        if ( Objects.nonNull( redissonClient ) )
        {
            redissonClient.shutdown();
        }

        // Close ChunkedOutput streams
        try
        {
            if ( !outStream.isClosed() )
            {
                outStream.close();
                byteOutputStream.close();
            }
            if ( !errorStream.isClosed() )
            {
                errorStream.close();
                byteErrorStream.close();
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
