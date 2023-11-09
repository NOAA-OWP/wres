package wres.server;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.inject.Singleton;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.server.ChunkedOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.messages.generated.EvaluationStatusOuterClass.EvaluationStatus.*;

import wres.ExecutionResult;
import wres.Main;
import wres.events.broker.BrokerConnectionFactory;
import wres.io.database.ConnectionSupplier;
import wres.io.database.Database;
import wres.io.database.DatabaseOperations;
import wres.io.database.locking.DatabaseLockFailed;
import wres.io.database.locking.DatabaseLockManager;
import wres.messages.generated.EvaluationStatusOuterClass;
import wres.messages.generated.Job;
import wres.pipeline.Canceller;
import wres.pipeline.Evaluator;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
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
 * TODO: Swap to using a persistant cache to support async/multiple simultaneous evaluations
 */

@Path( "/evaluation" )
@Singleton
public class EvaluationService implements ServletContextListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger( EvaluationService.class );

    private static final Random RANDOM =
            new Random( System.currentTimeMillis() );

    private static final AtomicReference<EvaluationStatusOuterClass.EvaluationStatus> EVALUATION_STAGE =
            new AtomicReference<>( AWAITING );
    private static final AtomicLong EVALUATION_ID = new AtomicLong( -1 );

    private static final int ONE_MINUTE_IN_MILLISECONDS = 60000;

    private static Thread timeoutThread;

    /** A shared bag of output resource references by request id */
    // The cache is here for expedience, this information could be persisted
    // elsewhere, such as a database or a local file. Cleanup of outputs is a
    // related concern.
    private static final Cache<Long, Set<java.nio.file.Path>> OUTPUTS = Caffeine.newBuilder()
                                                                                .maximumSize( 100 )
                                                                                .build();

    private SystemSettings systemSettings;

    private Database database;

    private final BrokerConnectionFactory broker;

    private Evaluator evaluator;

    /**
     * Constructor
     * @param systemSettings The system settings passed along from the server
     * @param database The initial database created for us by the server
     * @param broker The broker to deal with connections
     */
    public EvaluationService( SystemSettings systemSettings,
                              Database database,
                              BrokerConnectionFactory broker )
    {
        this.systemSettings = systemSettings;
        this.database = database;
        this.broker = broker;
        this.evaluator = new Evaluator( systemSettings,
                                        database,
                                        broker );
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
        // We heartbeat the server before accepting a new job and periodically through docker
        // Set status to AWAITING if the last project has been CLOSED
        if ( EVALUATION_STAGE.get().equals( CLOSED ) )
        {
            EVALUATION_STAGE.set( AWAITING );
        }
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
        // TODO activate when we have a persistant cache and async evaluations

        return Response.ok( EVALUATION_STAGE.get().toString() )
                       .build();
    }

    /**
     * Redirect the standard out stream and return that to the user in a ChunkedOutput
     * @param id ID of the evaluation we are trying to track with this
     */
    @GET
    @Path( "/stdout/{id}" )
    public ChunkedOutput<String> getOutStream( @PathParam( "id" ) Long id )
    {
        //TODO Get the stream for the evaluation ID if we allow async executions in the future
        //TODO Possibly add evaluation status check here as well. No need to check now as you can have unlimited listeners

        final ChunkedOutput<String> output = new ChunkedOutput<>( String.class );

        // Create new stream to redirect output to
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( byteArrayOutputStream );

        // redirect the out stream
        System.setOut( printStream );

        // Start the thread that will send the information from the byteArrayOutputStream to the output ChunkedOutput we are returning
        startChunkedOutputThread( byteArrayOutputStream, output );
        return output;
    }

    /**
     * Redirect the standard err stream and return that to the user in a ChunkedOutput
     * @param id ID of the evaluation we are trying to track with this
     */
    @GET
    @Path( "/stderr/{id}" )
    public ChunkedOutput<String> getErrorStream( @PathParam( "id" ) Long id )
    {
        // TODO Get the stream for the evaluation ID if we allow async executions in the future
        // TODO Possibly add evaluation status check here as well. No need to check now as you can have unlimited listeners

        final ChunkedOutput<String> output = new ChunkedOutput<>( String.class );

        // Create new stream to redirect output to
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream( byteArrayOutputStream );

        // redirect the error stream
        System.setErr( printStream );

        // Start the thread that will send the information from the byteArrayOutputStream to the output ChunkedOutput we are returning
        startChunkedOutputThread( byteArrayOutputStream, output );
        return output;
    }

    /**
     * Creates an ID for an evaluation we plan to execute and sets status to OPEN
     * If a project is not started within 1 minutes, a thread will put the status to closed
     *
     * @return ID of evaluation to run
     */
    @POST
    @Path( "/open" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response openEvaluation()
    {
        if ( EVALUATION_ID.get() > 0
             || !( EVALUATION_STAGE.get().equals( CLOSED )
                   || EVALUATION_STAGE.get()
                                      .equals( AWAITING ) ) )
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

        // Start a timer to prevent abandonded jobs from blocking the queue
        evaluationTimeoutThread();

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
        close();
        return Response.ok( "Evaluation closed" )
                       .build();
    }

    /**
     * Starts an opened Evaluation
     * @param message job message containing the evaluation and database settings
     * @return the state of the evaluation
     */
    @POST
    @Path( "/start/{id}" )
    @Produces( "application/octet-stream" )
    public Response startEvaluation( byte[] message, @PathParam( "id" ) Long id )
    {
        // Convert the raw message into a job
        Job.job jobMessage = createJobFromByteArray( message );

        // Check that this evaluation should be ran
        if ( EVALUATION_ID.get() != id && EVALUATION_STAGE.get().equals( OPENED ) )
        {
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity(
                                   "There was not an evaluation opened to start. Please /close/{ID} any projects first and open a new one" )
                           .build();
        }

        //Used for logging
        Instant beganExecution = Instant.now();

        EVALUATION_STAGE.set( ONGOING );
        Set<java.nio.file.Path> outputPaths;
        try
        {
            // Checks if database information has changed in the jobMessage and swap to that database
            swapDatabaseIfNeeded( jobMessage );

            // Print system setting at the top of the job log to help debug
            logJobHeaderInformation();

            // Execute an evaluation
            Canceller canceller = Canceller.of();
            ExecutionResult result = evaluator.evaluate( jobMessage.getProjectConfig(), canceller );

            logJobFinishInformation( jobMessage, result, beganExecution );

            // If the result failed, print the stack trace and return the exception
            if ( result.failed() )
            {
                String failureMessage = "The evaluation failed with the following stack trace: ";
                // Print the stack exception so it is stored in the stdOut of the job
                LOGGER.info( failureMessage, result.getException() );

                EVALUATION_STAGE.set( COMPLETED );
                return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                               .entity( failureMessage + result.getException() )
                               .build();
            }

            // get files written
            outputPaths = result.getResources();
        }
        catch ( UserInputException e )
        {
            String failureMessage = "I received something I could not parse. The top-level exception was";
            logJobFinishInformation( jobMessage, ExecutionResult.failure( e ), beganExecution );
            LOGGER.info( failureMessage, e );
            EVALUATION_STAGE.set( COMPLETED );
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity( failureMessage + e.getMessage() )
                           .build();
        }
        catch ( InternalWresException iwe )
        {
            String failureMessage = "WRES experienced an internal issue. The top-level exception was";
            logJobFinishInformation( jobMessage, ExecutionResult.failure( iwe ), beganExecution );
            LOGGER.info( failureMessage, iwe );
            EVALUATION_STAGE.set( COMPLETED );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( failureMessage + iwe.getMessage() )
                           .build();
        }

        // Put output paths in a stream to send to user
        StreamingOutput streamingOutput = outputSream -> {
            Writer writer = new BufferedWriter( new OutputStreamWriter( outputSream ) );
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
    }

    /**
     * Kicks off a database Migration
     * @return Good response
     */
    @POST
    @Path( "/migrateDatabase" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response migrateDatabase( byte[] message )
    {
        if ( !systemSettings.isUseDatabase() )
        {
            throw new IllegalArgumentException(
                    "This is an in-memory execution. Cannot migrate a database because there "
                    + "is no database to migrate." );
        }

        //Used for logging
        Instant beganExecution = Instant.now();

        // Convert the raw message into a job
        Job.job jobMessage = createJobFromByteArray( message );

        try
        {
            // Checks if database information has changed in the jobMessage and swap to that database
            swapDatabaseIfNeeded( jobMessage );

            logJobHeaderInformation();
            //The migrateDatabase method deals with database locking, so we don't need to worry about that here
            DatabaseOperations.migrateDatabase( database );
            logJobFinishInformation( jobMessage, ExecutionResult.success(), beganExecution );
        }
        catch ( SQLException se )
        {
            String errorMessage = "Failed to migrate the database.";
            LOGGER.info( errorMessage, se );
            InternalWresException e = new InternalWresException( errorMessage, se );
            logJobFinishInformation( jobMessage, ExecutionResult.failure( se ), beganExecution );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Unable to migrate the database with the error: "
                                    + e.getMessage() )
                           .build();
        }

        return Response.ok( "Database Migrated" )
                       .build();
    }

    /**
     * Kicks off a database Clean
     * @return Good response
     */
    @POST
    @Path( "/cleanDatabase" )
    @Produces( MediaType.TEXT_PLAIN )
    public Response cleanDatabase( byte[] message )
    {
        if ( !systemSettings.isUseDatabase() )
        {
            throw new IllegalArgumentException( "This is an in-memory execution. Cannot clean a database because there "
                                                + "is no database to clean." );
        }

        EVALUATION_STAGE.set( ONGOING );

        //Used for logging
        Instant beganExecution = Instant.now();

        // Convert the raw message into a job
        Job.job jobMessage = createJobFromByteArray( message );

        // Checks if database information has changed in the jobMessage and swap to that database
        swapDatabaseIfNeeded( jobMessage );

        DatabaseLockManager lockManager =
                DatabaseLockManager.from( systemSettings,
                                          () -> database.getRawConnection() );
        try
        {
            logJobHeaderInformation();
            lockManager.lockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            DatabaseOperations.cleanDatabase( database );
            lockManager.unlockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            logJobFinishInformation( jobMessage, ExecutionResult.success(), beganExecution );
        }
        catch ( IllegalStateException | DatabaseLockFailed | SQLException se )
        {
            String errorMessage = "Failed to clean the database. Unable to aquire lock or communicate with database";
            InternalWresException e = new InternalWresException( errorMessage, se );
            LOGGER.info( errorMessage, se );
            logJobFinishInformation( jobMessage, ExecutionResult.failure( e ), beganExecution );
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "Unable to clean the database with the error: "
                                    + e.getMessage() )
                           .build();
        }
        finally
        {
            EVALUATION_STAGE.set( CLOSED );
            lockManager.shutdown();
        }

        return Response.ok( "Database Cleaned" )
                       .build();
    }

    /**
     * A simplistic evaluate API, if using this call then the user will not have access to stdout, stderror or status
     * The output paths written are returned to the user and not stored anywhere
     *
     * @param projectConfig the evaluation project declaration string
     * @return the state of the evaluation
     */
    @POST
    @Path( "/evaluate" )
    @Consumes( MediaType.TEXT_XML )
    @Produces( "application/octet-stream" )
    public Response postEvaluate( String projectConfig )
    {
        long projectId;
        try
        {
            // Guarantee a positive number. Using Math.abs would open up failure
            // in edge cases. A while loop seems complex. Thanks to Ted Hopp
            // on StackOverflow question id 5827023.
            projectId = RANDOM.nextLong() & Long.MAX_VALUE;

            Canceller canceller = Canceller.of();
            ExecutionResult result = evaluator.evaluate( projectConfig, canceller );
            Set<java.nio.file.Path> outputPaths = result.getResources();
            OUTPUTS.put( projectId, outputPaths );
        }
        catch ( UserInputException e )
        {
            return Response.status( Response.Status.BAD_REQUEST )
                           .entity( "I received something I could not parse. The top-level exception was: "
                                    + e.getMessage() )
                           .build();
        }
        catch ( InternalWresException iwe )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "WRES experienced an internal issue. The top-level exception was: "
                                    + iwe.getMessage() )
                           .build();
        }
        catch ( Exception e )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR )
                           .entity( "WRES experienced an unexpected internal issue. The top-level exception was: "
                                    + e.getMessage() )
                           .build();
        }

        // TODO: Swap to not using cache once we can find root cause of files not being written

        return Response.ok( "I received project " + projectId
                            + ", and successfully ran it. Visit /project/"
                            + projectId
                            + " for more." )
                       .build();
    }

    /**
     * @param id the evaluation job identifier
     * @return the evaluation results
     * @deprecated
     */

    @GET
    @Path( "/{id}" )
    @Produces( MediaType.TEXT_PLAIN )
    @Deprecated( since = "6.17" )
    public Response getProjectResults( @PathParam( "id" ) Long id )
    {
        Set<java.nio.file.Path> paths = OUTPUTS.getIfPresent( id );

        if ( paths == null )
        {
            return Response.status( Response.Status.NOT_FOUND )
                           .entity( "Could not find job " + id )
                           .build();
        }

        Set<String> resources = getSetOfResources( id, paths );

        return Response.ok( "Here are result resources: " + resources )
                       .build();
    }

    /**
     * @param id the evaluation job identifier
     * @param resourceName the resource name
     * @return the resource
     * @deprecated
     */

    @GET
    @Path( "/{id}/{resourceName}" )
    @Deprecated( since = "6.17" )
    public Response getProjectResource( @PathParam( "id" ) Long id,
                                        @PathParam( "resourceName" ) String resourceName )
    {
        Set<java.nio.file.Path> paths = OUTPUTS.getIfPresent( id );
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
     * Creates a thread that will send messages from the stream to the provided ChunkedOutput.
     * @param redirectStream the redirect stream
     * @param output the chunked outputs stream
     */
    private void startChunkedOutputThread( ByteArrayOutputStream redirectStream, ChunkedOutput<String> output )
    {
        new Thread( () -> {
            try ( output )
            {
                int offset = 0;

                while ( !EVALUATION_STAGE.get().equals( CLOSED ) )
                {
                    offset = writeOutput( redirectStream, output, offset );
                }

                // After the evaluation is closed, send any more information missed in the last loop
                // This helps avoid logs being cut off if alot of information is sent at the end of an evaluation
                writeOutput( redirectStream, output, offset );
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
     * @return the new offset
     * @throws IOException IOexception when writting to the output
     */
    private int writeOutput( ByteArrayOutputStream redirectStream, ChunkedOutput<String> output, int offset )
            throws IOException
    {
        ByteArrayInputStream byteArrayInputStream =
                new ByteArrayInputStream( redirectStream.toByteArray() );
        // Skip the content in the message already sent
        long skip = byteArrayInputStream.skip( offset );
        String bytes = new String( byteArrayInputStream.readAllBytes(), StandardCharsets.UTF_8 );

        // If we skipped successfully and the resulting string isn't empty send SSE
        if ( offset == skip && !bytes.isEmpty() )
        {
            offset += bytes.length();
            output.write( bytes );
        }
        return offset;
    }

    /**
     * Closes the current evaluation by setting all atomic values to expected states and resetting standard streams
     */
    private static void close()
    {
        // Project closed gracefully, stop the timeout thread
        timeoutThread.interrupt();

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

    /**
     * Logs information about the end of a job
     * @param jobMessage The job message used by the evaluation
     * @param result The result of the execution
     * @param beganExecution When the evaluation started
     */
    private void logJobFinishInformation( Job.job jobMessage, ExecutionResult result, Instant beganExecution )
    {
        // Log timing of execution
        if ( LOGGER.isInfoEnabled() )
        {
            Instant endedExecution = Instant.now();
            Duration duration = Duration.between( beganExecution, endedExecution );
            LOGGER.info( "The function '{}' took {}", jobMessage.getVerb(), duration );
        }

        // Log the execution to the database if a database is used
        if ( this.systemSettings.isUseDatabase() )
        {
            Instant endedExecution = Instant.now();
            // Log both the operation and the args
            List<String> argList = new ArrayList<>();
            argList.add( jobMessage.getVerb().toString() );
            argList.addAll( jobMessage.getAdditionalArgumentsList().stream().toList() );

            DatabaseOperations.LogParameters logParameters =
                    new DatabaseOperations.LogParameters( argList,
                                                          result.getName(),
                                                          result.getDeclaration(),
                                                          result.getHash(),
                                                          beganExecution,
                                                          endedExecution,
                                                          result.failed(),
                                                          result.getException(),
                                                          Main.getVersion() );

            DatabaseOperations.logExecution( database,
                                             logParameters );
        }
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
     * If the job contains database information different from the current database then change what database we are using
     * @param job The job we are about to evaluate
     */
    private void swapDatabaseIfNeeded( Job.job job )
    {
        boolean databaseChangeDetected = false;
        String databaseName = job.getDatabaseName();
        String databaseHost = job.getDatabaseHost();
        String databasePort = job.getDatabasePort();

        DatabaseSettings databaseConfiguration = systemSettings.getDatabaseConfiguration();
        DatabaseSettings.DatabaseSettingsBuilder databaseBuilder = databaseConfiguration.toBuilder();

        // Attempt to apply any changes detected
        if ( !databaseName.isEmpty() && !databaseName.equals( databaseConfiguration.getDatabaseName() ) )
        {
            databaseBuilder.databaseName( databaseName );
            databaseChangeDetected = true;
        }
        if ( !databaseHost.isEmpty() && !databaseHost.equals( databaseConfiguration.getHost() ) )
        {
            databaseBuilder.host( databaseHost );
            databaseChangeDetected = true;
        }
        if ( !databasePort.isEmpty() && !databasePort.equals( String.valueOf( databaseConfiguration.getPort() ) ) )
        {
            databaseBuilder.port( Integer.parseInt( databasePort ) );
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

            if ( systemSettings.isUseDatabase() )
            {
                if ( Objects.nonNull( database ) )
                {
                    LOGGER.info( "Terminating current database connections" );
                    database.shutdown();
                }

                database = new Database( new ConnectionSupplier( systemSettings ) );
                // Migrate the database, as needed
                if ( database.getAttemptToMigrate() )
                {
                    try
                    {
                        DatabaseOperations.migrateDatabase( database );
                    }
                    catch ( SQLException e )
                    {
                        String errorMessage = "Failed to migrate the WRES database.";
                        LOGGER.error( errorMessage, e );
                        throw new IllegalStateException( errorMessage, e );
                    }
                }
            }
            evaluator = new Evaluator( systemSettings, database, broker );
        }
    }

    private Job.job createJobFromByteArray( byte[] message )
    {
        try
        {
            return Job.job.parseFrom( message );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            throw new IllegalArgumentException( "Bad message received: " + Arrays.toString( message ), ipbe );
        }
    }

    /**
     * Examines the content type of the resource at the path.
     * @param path the path
     * @return the content type
     */

    private static String getContentType( java.nio.file.Path path )
    {
        String type = MediaType.TEXT_PLAIN_TYPE.getType();

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
     * Get a set of resources connected to a project.
     * @param projectId the project identifier
     * @param pathSet the paths
     * @return the resources
     */

    private static Set<String> getSetOfResources( long projectId,
                                                  Set<java.nio.file.Path> pathSet )
    {
        Set<String> resources = new HashSet<>();

        for ( java.nio.file.Path path : pathSet )
        {
            resources.add( "project/"
                           + projectId
                           + "/"
                           + path.getFileName() );
        }

        return Collections.unmodifiableSet( resources );
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
        // Perform action during application's shutdown
        if ( systemSettings.isUseDatabase() && Objects.nonNull( database ) )
        {
            LOGGER.info( "Terminating database activities..." );
            database.shutdown();
        }
    }
}
