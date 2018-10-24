package wres.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    private final File wresExecutable;
    private final BlockingQueue<WresProcess> processToLaunch;


    /**
     * Constructs a new instance and records its association to the passed-in channel.
     * @param channel the channel to which this consumer is attached
     * @param wresExecutable the wresExecutable to use for launching WRES
     * @param processToLaunch a Q to send data back to the main thread with
     */

    JobReceiver( Channel channel, File wresExecutable, BlockingQueue<WresProcess> processToLaunch )
    {
        super( channel );
        this.wresExecutable = wresExecutable;
        this.processToLaunch = processToLaunch;
    }

    private File getWresExecutable()
    {
        return this.wresExecutable;
    }

    private BlockingQueue<WresProcess> getProcessToLaunch()
    {
        return this.processToLaunch;
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

        try
        {
            outputPath = Files.createTempDirectory( "wres_job_"
                                                    + jobId + "_",
                                                    fileAttribute );
        }
        catch ( IOException ioe )
        {
            throw new IllegalStateException( "Failed to create directory for job "
                                             + jobId, ioe );
        }

        // Translate the message into a command
        ProcessBuilder processBuilder = createBuilderFromMessage( body,
                                                                  outputPath );
        // Set up the information needed to launch process and send info back
        WresProcess wresProcess = new WresProcess( processBuilder,
                                                   properties.getReplyTo(),
                                                   properties.getCorrelationId(),
                                                   this.getChannel().getConnection(),
                                                   envelope,
                                                   outputPath );
        // Share the process information with the caller
        this.getProcessToLaunch().offer( wresProcess );
    }


    /**
     * Translate a message from the queue into a ProcessBuilder to run
     * @param message a message from the queue
     * @param outputDirectory a directory exclusively for this job's outputs
     * @return a ProcessBuilder to attempt to run
     * @throws IllegalArgumentException if the message is not well formed
     * @throws IllegalStateException when output data directory cannot be created
     */

    private ProcessBuilder createBuilderFromMessage( byte[] message,
                                                     Path outputDirectory )
    {
        Job.job jobMessage;

        try
        {
            jobMessage = Job.job.parseFrom( message );
        }
        catch ( InvalidProtocolBufferException ipbe )
        {
            throw new IllegalArgumentException( "Bad message received", ipbe );
        }

        String javaOpts = "-Dwres.url=" + jobMessage.getDatabaseHostname()
                          + " -Dwres.databaseName=" + jobMessage.getDatabaseName()
                          + " -Dwres.username=" + jobMessage.getDatabaseUsername()
                          + " -Djava.io.tmpdir=" + outputDirectory.toString();

        List<String> result = new ArrayList<>();

        String executable = this.getWresExecutable().getPath();
        String command = "execute";
        String projectConfig = jobMessage.getProjectConfig();

        // Make sure we have a project config...
        if ( projectConfig == null )
        {
            LOGGER.warn( "No project config specified in message." );
            return null;
        }

        result.add( executable );
        result.add( command );
        result.add( projectConfig );

        ProcessBuilder processBuilder = new ProcessBuilder( result );

        // Pass through additional java options set in the environment for this
        // inner worker process, as distinct from this shim process.
        String innerJavaOpts = System.getenv( "INNER_JAVA_OPTS" );

        if ( innerJavaOpts != null && innerJavaOpts.length() > 0 )
        {
            javaOpts = javaOpts + " " + innerJavaOpts;
        }

        // Cause process builder to get java options
        processBuilder.environment().put( "JAVA_OPTS", javaOpts );

        return processBuilder;
    }
}
