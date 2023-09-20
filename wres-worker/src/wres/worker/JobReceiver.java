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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                                                                                       body,
                                                                                       this.getPort() );
        // Share the process information with the caller
        boolean wasOffered = this.getProcessToLaunch().offer( wresEvaluationProcessor );

        if (!wasOffered) {
            throw new InternalError( "Unable to queue job" );
        }
    }
}
