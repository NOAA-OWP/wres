package wres.worker;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import wres.messages.generated.JobOutput;

/**
 * Sends one message for each output found
 * At least one race condition which may or may not cause issues
 * 1. Should be created and started before WRES process is launched so that this
 *    can see the "create" event of the output directory.
 * 2. Connectivity to the filesystem and broker must be fast enough that this
 *    can see the "create" events inside the output directory.
 */
class JobOutputMessenger implements Runnable, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JobOutputMessenger.class );
    private final Connection connection;
    private final String exchangeName;
    private final String jobId;
    private final Path outputDirectory;
    private boolean foundInnerOutputDirectory = false;
    private boolean closed = false;

    JobOutputMessenger( Connection connection,
                        String exchangeName,
                        String jobId,
                        Path outputDirectory )
    {
        Objects.requireNonNull( connection );
        Objects.requireNonNull( exchangeName );
        Objects.requireNonNull( jobId );
        this.connection = connection;
        this.exchangeName = exchangeName;
        this.jobId = jobId;
        if ( ! Files.isDirectory( outputDirectory )
            && ! Files.isSymbolicLink( outputDirectory ) )
        {
            throw new IllegalArgumentException( "Path passed as outputDirectory was not a directory nor a symlink: "
                                                + outputDirectory );
        }
        this.outputDirectory = outputDirectory;
    }


    private Connection getConnection()
    {
        return this.connection;
    }

    private String getExchangeName()
    {
        return this.exchangeName;
    }

    private String getJobId()
    {
        return this.jobId;
    }

    private String getRoutingKey()
    {
        return "job." + this.getJobId() + ".output";
    }

    private Path getOutputDirectory()
    {
        return this.outputDirectory;
    }

    /**
     * Watch the filesystem for changes within output directory.
     * Must be run prior to running a WRES process so that this can watch for
     * inner output directory *create* event and find the outputs.
     */

    @Override
    public void run()
    {
        // To watch files, we have to jump through this hoop: get the filesystem
        FileSystem theFileSystem = FileSystems.getDefault();

        Path innerOutputDirectory = this.getOutputDirectory();

        try ( WatchService outputDirectoryWatchService = theFileSystem.newWatchService() )
        {
            // We assume that the
            this.getOutputDirectory()
                .register( outputDirectoryWatchService,
                           ENTRY_CREATE );

            while ( !this.foundInnerOutputDirectory )
            {
                // Look for the actual inner output directory to be created
                // by the WRES process (inside the one we're watching).
                WatchKey somethingFound =
                        outputDirectoryWatchService.poll( 1, TimeUnit.SECONDS );

                // Then look for files in that inner output directory
                LOGGER.debug( "Found something related to output? {}",
                              somethingFound );

                if ( somethingFound != null )
                {
                    for ( WatchEvent<?> event : somethingFound.pollEvents() )
                    {
                        LOGGER.debug( "Found an event related to output {}",
                                      event );
                        WatchEvent.Kind<?> fileEventKind = event.kind();

                        if ( fileEventKind.equals( OVERFLOW ) )
                        {
                            continue;
                        }

                        WatchEvent<Path> pathEvent = ( WatchEvent<Path> ) event;
                        Path output = pathEvent.context();
                        Path fullOutput = Paths.get( this.getOutputDirectory().toString(),
                                                     output.toString() );
                        LOGGER.debug( "Found a file related to output {}",
                                     fullOutput );

                        if ( !Files.isRegularFile( fullOutput ) )
                        {
                            // If it's (potentially) a directory, register it
                            // too, to watch inside the directory for files.
                            // Somehow this distinction is more reliable than
                            // asking if this is a symlink or directory.
                            LOGGER.debug( "Found inner output directory {}",
                                          fullOutput );
                            innerOutputDirectory = fullOutput;
                            this.foundInnerOutputDirectory = true;
                            // Kind of redundant, but prevents the reset stuff.
                            break;
                        }
                        else
                        {
                            LOGGER.warn( "Found unexpected output {}", fullOutput );
                        }
                    }

                    // Following guide at
                    // https://docs.oracle.com/javase/tutorial/essential/io/notification.html
                    // To allow <></>he key to see more events, reset the key.
                    boolean valid = somethingFound.reset();

                    // But if the key is no longer valid, we have to stop? See guide.
                    if ( !valid )
                    {
                        LOGGER.warn(
                                "Stopped watching for innermost output directory early because {} reset was invalid.",
                                somethingFound );
                        return;
                    }
                }
                else
                {
                    // This is expected, normal. The reason we wait one second
                    // is to give the opportunity for graceful close of this
                    // object instead of getting InterruptedException every time
                    LOGGER.debug( "Found expected null while watching for innermost output directory" );
                }
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while looking for innermost output directory.", ie );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "While looking for innermost output directory", ioe );
        }

        try ( WatchService outputFileWatchService = theFileSystem.newWatchService();
              Channel channel = this.getConnection().createChannel() )
        {
            // There is an assumption that above steps will be fast enough to
            // allow us to
            // capture the *create* event of every output file. Whether this
            // will bear out in the wild is unknown, but seems simplest for now.
            // If we also watch for ENTRY_MODIFY, the choice has to be made
            // whether to collect a set of output files and ensure we send only
            // one message, or put the burden on the consumer of messages to
            // collect the set of paths and to expect multiple messages per
            // output file.
            // Both .newWatchService() and .createChannel() need to finish, it
            // is probably the latter that would cause a bigger delay. Not sure.
            innerOutputDirectory
                    .register( outputFileWatchService,
                               ENTRY_CREATE );

            String exchangeName = this.getExchangeName();
            String exchangeType = "topic";

            channel.exchangeDeclare( exchangeName, exchangeType );

            while ( !this.closed )
            {
                // Look for the files inside the innermost output directory
                WatchKey somethingFound =
                        outputFileWatchService.poll( 1, TimeUnit.SECONDS );

                LOGGER.debug( "Found something related to output? {}",
                              somethingFound );

                if ( somethingFound != null )
                {
                    for ( WatchEvent<?> event : somethingFound.pollEvents() )
                    {
                        LOGGER.debug( "Found an event related to output {}",
                                      event );
                        WatchEvent.Kind<?> fileEventKind = event.kind();

                        if ( fileEventKind.equals( OVERFLOW ) )
                        {
                            continue;
                        }

                        WatchEvent<Path> pathEvent = ( WatchEvent<Path> ) event;
                        Path output = pathEvent.context();
                        Path fullOutput = Paths.get( innerOutputDirectory.toString(),
                                                     output.toString() );
                        LOGGER.info( "Found a file related to output {}",
                                     fullOutput );

                        if ( Files.isRegularFile( fullOutput ) )
                        {
                            this.sendPath( channel, fullOutput );
                        }
                        else
                        {
                            LOGGER.warn( "Found unexpected non regular-file {}", fullOutput );
                        }
                    }

                    boolean valid = somethingFound.reset();

                    if ( !valid )
                    {
                        LOGGER.warn(
                                "Stopped watching for output early because {} reset was invalid.",
                                somethingFound );
                        break;
                    }
                }
                else
                {
                    // This is expected, normal. The reason we wait one second
                    // is to give the opportunity for graceful close of this
                    // object instead of getting InterruptedException every time
                    LOGGER.debug( "Found expected null while watching for output files." );
                }
            }
        }
        catch ( TimeoutException te )
        {
            LOGGER.warn( "Failed to create channel on broker connection {}.",
                         this.getConnection(), te );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while looking for output files.", ie );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "While looking for output files", ioe );
        }

        LOGGER.info( "Finished sending output messages for job {}", jobId );
    }


    /**
     * Attempts to send a message with a single line of output
     * @param channel the channel to use
     * @param path the path to send
     */
    private void sendPath( Channel channel, Path path )
    {
        LOGGER.debug( "Sending output path {} to broker.", path );
        AMQP.BasicProperties properties =
                new AMQP.BasicProperties
                        .Builder()
                        .correlationId( this.getJobId() )
                        .build();

        URI theOutputResource = path.toUri();

        LOGGER.debug( "Sending output uri {} to broker.", theOutputResource );

        JobOutput.job_output jobOutputMessage = JobOutput.job_output
                .newBuilder()
                .setResource( theOutputResource.toString() )
                .build();

        try
        {
            channel.basicPublish( this.getExchangeName(),
                                  this.getRoutingKey(),
                                  properties,
                                  jobOutputMessage.toByteArray() );
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "Sending this output failed: {}", jobOutputMessage, ioe );
        }
    }

    @Override
    public void close()
    {
        // Tempted to put a Thread.sleep in here, but I don't think it is
        // needed because the this.closed flag will not immediately terminate
        // this run, it will allow the processing of found files to continue.
        // In a sense the 1 second timeout above when polling the watcher *is*
        // a sleep-before-completely-shutting-down.
        // Is there still the possibility that more files could be created
        // after the completion of the "run" above? Not sure.
        LOGGER.debug( "Closing {}", this );
        this.closed = true;
    }
}
