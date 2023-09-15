package wres.worker;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.generated.Job;


/**
 * The concrete class that does the work of taking a job message and creating
 * a WRES process to fulfil the job message's request.
 *
 * Uses environment variable JAVA_OPTS to set database details for a run,
 * appends environment variable INNER_JAVA_OPTS to JAVA_OPTS to set additional
 * -D parameters such as those related to logging.
 */

class JobReceiver extends DefaultConsumer
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobReceiver.class );
    private static final int MAX_COMMAND_ARG_LENGTH = 131072;

    private final BlockingQueue<WresEvaluationProcessor> processToLaunch;

    private final int port;

    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * @param channel the channel to which this consumer is attached
     * @param processToLaunch a Q to send data back to the main thread with
     * @param port The port that the local server lives on, passing this through to the WresProcess
     */

    JobReceiver( Channel channel,
                 BlockingQueue<WresEvaluationProcessor> processToLaunch,
                 int port )
    {
        super( channel );
        this.processToLaunch = processToLaunch;
        this.port = port;
    }

    private BlockingQueue<WresEvaluationProcessor> getProcessToLaunch()
    {
        return this.processToLaunch;
    }

    private int getPort()
    {
        return this.port;
    }


    /**
     * This is the entry point that will accept a message and create a
     * WresProcess that the main thread or another thread can run, sharing it
     * with the creator of this JobReceiver via a blocking q.
     * @param consumerTag boilerplate
     * @param envelope boilerplate
     * @param properties boilerplate
     * @param body the message body of the job request from the queue
     */

    @Override
    public void handleDelivery( String consumerTag,
                                Envelope envelope,
                                AMQP.BasicProperties properties,
                                byte[] body )
    {
        String jobId = properties.getCorrelationId();
        // Create an area for data related to this job, using java.io.tmpdir.
        // Will end up with a nested directory structure.
        Path outputPath;

        // Permissions for temp directory require group read so that the tasker
        // may give the output to the client on GET. Write so that the tasker
        // may remove the output on client DELETE. Execute for dir reads.
        Set<PosixFilePermission> permissions = new HashSet<>( 6 );
        permissions.add( PosixFilePermission.OWNER_READ );
        permissions.add( PosixFilePermission.OWNER_WRITE );
        permissions.add( PosixFilePermission.OWNER_EXECUTE );
        permissions.add( PosixFilePermission.GROUP_READ );
        permissions.add( PosixFilePermission.GROUP_WRITE );
        permissions.add( PosixFilePermission.GROUP_EXECUTE );
        FileAttribute<Set<PosixFilePermission>> fileAttribute =
                PosixFilePermissions.asFileAttribute( permissions );

        String jobIdString = "wres_job_" + jobId;
        String tempDir = System.getProperty( "java.io.tmpdir" );
        outputPath = Paths.get( tempDir, jobIdString );

        Job.job jobMessage;

        try
        {
            jobMessage = Job.job.parseFrom( body );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            throw new IllegalArgumentException( "Bad message received", ipbe );
        }
        String projectConfig = jobMessage.getProjectConfig();


        try
        {
            Files.createDirectory( outputPath, fileAttribute );
            LOGGER.debug( "Created job directory {}.", outputPath );
        }
        catch ( FileAlreadyExistsException faee )
        {
            LOGGER.warn( "Job directory {} already existed indicating another process started working here.",
                         outputPath, faee );
        }
        catch ( IOException ioe )
        {
            throw new IllegalStateException( "Failed to create directory for job "
                                             + jobId, ioe );
        }
        // Set up the information needed to launch process and send info back
        WresEvaluationProcessor wresEvaluationProcessor = new WresEvaluationProcessor( properties.getReplyTo(),
                                                                                       properties.getCorrelationId(),
                                                                                       this.getChannel()
                                                                                           .getConnection(),
                                                                                       envelope,
                                                                                       projectConfig,
                                                                                       this.getPort() );
        // Share the process information with the caller
        this.getProcessToLaunch().offer( wresEvaluationProcessor );
    }

    //TODO: This method can be removed, but keeping it for reference until I figure out how to handle remaining wres-core functions

    //    /**
    //     * Translate a message from the queue into a ProcessBuilder to run
    //     * @param message a message from the queue
    //     * @param outputDirectory a directory exclusively for this job's outputs
    //     * @param jobIdString the fully qualified job identifier to propagate to the inner wres process
    //     * @return a ProcessBuilder to attempt to run
    //     * @throws IllegalArgumentException if the message is not well formed
    //     * @throws IllegalStateException when output data directory cannot be created
    //     */

    //    private ProcessBuilder createBuilderFromMessage( byte[] message,
    //                                                     Path outputDirectory,
    //                                                     String jobIdString )
    //    {
    //        Job.job jobMessage;
    //
    //        try
    //        {
    //            jobMessage = Job.job.parseFrom( message );
    //        }
    //        catch ( InvalidProtocolBufferException ipbe )
    //        {
    //            throw new IllegalArgumentException( "Bad message received", ipbe );
    //        }
    //
    //        // #84942
    //        String javaOpts = " -Dwres.jobId=" + jobIdString;
    //
    //        List<String> result = new ArrayList<>();
    //
    //        String executable = this.getWresExecutable()
    //                                .getPath();
    //        Verb command = jobMessage.getVerb();
    //        String projectConfig = jobMessage.getProjectConfig();
    //        List<String> additionalArguments = jobMessage.getAdditionalArgumentsList();
    //        String databaseName = jobMessage.getDatabaseName();
    //        String databaseHost = jobMessage.getDatabaseHost();
    //        String databasePort = jobMessage.getDatabasePort();
    //
    //        result.add( executable );
    //        result.add( command.name()
    //                           .toLowerCase() );
    //
    //        // Make sure we have a project config for ingest or execute
    //        if ( projectConfig == null || projectConfig.isBlank() )
    //        {
    //            if ( command.equals( Verb.INGEST ) || command.equals( Verb.EXECUTE ) )
    //            {
    //                LOGGER.warn( "No project config specified in message with {} verb.",
    //                             command );
    //                return null;
    //            }
    //
    //            LOGGER.debug( "Not adding null or blank projectConfig." );
    //        }
    //        else if ( projectConfig.length() * 4 >= MAX_COMMAND_ARG_LENGTH )
    //        {
    //            // A single character can take anywhere from 1 to 4 bytes depending
    //            // on use of a common encoding (ASCII 1, UTF-16 2, UTF-8 1-4).
    //            LOGGER.warn( "Found long project declaration, using a temp file." );
    //            Set<PosixFilePermission> permissions = new HashSet<>( 6 );
    //            permissions.add( PosixFilePermission.OWNER_READ );
    //            permissions.add( PosixFilePermission.OWNER_WRITE );
    //            permissions.add( PosixFilePermission.OWNER_EXECUTE );
    //            permissions.add( PosixFilePermission.GROUP_READ );
    //            permissions.add( PosixFilePermission.GROUP_WRITE );
    //            permissions.add( PosixFilePermission.GROUP_EXECUTE );
    //            FileAttribute<Set<PosixFilePermission>> fileAttribute =
    //                    PosixFilePermissions.asFileAttribute( permissions );
    //
    //            try
    //            {
    //                Path tempProject =
    //                        Files.createTempFile( outputDirectory,
    //                                              "wres_project_declaration_",
    //                                              ".xml",
    //                                              fileAttribute );
    //                Files.writeString( tempProject, projectConfig );
    //                result.add( tempProject.toString() );
    //            }
    //            catch ( IOException ioe )
    //            {
    //                throw new IllegalStateException( "Unable to write temp file",
    //                                                 ioe );
    //            }
    //        }
    //        else
    //        {
    //            LOGGER.debug( "Adding projectConfig to args of child process." );
    //            result.add( projectConfig );
    //        }
    //
    //        // When additional args exist, e.g. for rotatepartitions, add them.
    //        if ( additionalArguments != null && !additionalArguments.isEmpty() )
    //        {
    //            LOGGER.debug( "Adding additionalArguments to child process: {}",
    //                          additionalArguments );
    //            result.addAll( additionalArguments );
    //        }
    //
    //        ProcessBuilder processBuilder = new ProcessBuilder( result );
    //
    //        // Pass through additional java options set in the environment for this
    //        // inner worker process, as distinct from this shim process.
    //        String innerJavaOpts = System.getenv( "INNER_JAVA_OPTS" );
    //
    //        if ( innerJavaOpts != null && innerJavaOpts.length() > 0 )
    //        {
    //            javaOpts = javaOpts + " " + innerJavaOpts;
    //        }
    //
    //        //Pass through the database options if not null or blank.
    //        if ( databaseHost != null && !databaseHost.isBlank() )
    //        {
    //            javaOpts = JobReceiver.setJavaOptOption( javaOpts,
    //                                                     "wres.databaseHost",
    //                                                     databaseHost );
    //        }
    //        if (databaseName != null && !databaseName.isBlank() )
    //        {
    //            javaOpts = JobReceiver.setJavaOptOption( javaOpts,
    //                                                     "wres.databaseName",
    //                                                     databaseName );
    //        }
    //        if (databasePort != null && !databasePort.isBlank() )
    //        {
    //            javaOpts = JobReceiver.setJavaOptOption( javaOpts,
    //                                                     "wres.databasePort",
    //                                                     databasePort );
    //        }
    //
    //        // Assume that a request for "connecttodb" means "migrate the db", which
    //        // in turn means we must replace the option wres.attemptToMigrate=false
    //        // to wres.attemptToMigrate=true or add the option and set to true if
    //        // it is not present.
    //        if ( command.equals( Verb.CONNECTTODB ) )
    //        {
    //            LOGGER.info( "Special case: migrate the database, existing JAVA_OPTS: {}",
    //                         javaOpts );
    //            javaOpts = JobReceiver.setJavaOptOption( javaOpts,
    //                                                    "wres.attemptToMigrate",
    //                                                    "true" );
    //            LOGGER.info( "Updated JAVA_OPTS: {}", javaOpts );
    //        }
    //
    //        // Cause process builder to get java options
    //        processBuilder.environment().put( "JAVA_OPTS", javaOpts );
    //
    //        return processBuilder;
    //    }

    /**
     * Force option to have a value in a given JAVA_OPTS which may or may not
     * already have this value set.
     * @param javaOpts existing JAVA_OPTS, may or may not have the setting.
     * @param option The Java option to set.
     * @param optionValue The value of the option.
     * @return new JAVA_OPTS with the option set to the value.
     */
    static String setJavaOptOption( String javaOpts, String option, String optionValue )
    {
        String optionCheckText = "-D" + option + "=";
        String optionValueText = optionCheckText + optionValue;

        if ( javaOpts.contains( optionCheckText ) )
        {
            LOGGER.debug( "Found {} in java options, replacing with new valuei {}.",
                          option, optionValue );
            String optionReplacementString = optionCheckText.replaceAll( ".", "\\\\." );
            return javaOpts.replaceAll( optionCheckText + "[^\\s]+",
                                        optionValueText );
        }
        else
        {
            LOGGER.debug( "Did not find {} in java opts, appending {}.",
                          option, optionValueText );
            return javaOpts + " " + optionValueText;
        }
    }
}
