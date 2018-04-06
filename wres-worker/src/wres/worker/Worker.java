package wres.worker;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Worker.class );
    private static final String RECV_QUEUE_NAME= "wres.job";


    /**
     * Expects exactly one arg with a path to WRES executable
     * @param args arguments, but only one is expected, a WRES executable
     * @throws IOException when communication with queue fails or process start fails.
     * @throws IllegalArgumentException when the first argument is not a WRES executable
     * @throws java.net.ConnectException when connection to queue fails.
     * @throws TimeoutException when connection to the queue times out.
     * @throws InterruptedException when interrupted while waiting for work.
     */

    public static void main( String[] args )
            throws IOException, TimeoutException, InterruptedException
    {
        if ( args.length != 1 )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres path." );
        }

        // Getting as a file allows us to verify it exists
        File wresExecutable = Paths.get( args[0] ).toFile();
        if ( !wresExecutable.exists() )
        {
            throw new IllegalArgumentException( "First arg must be an executable wres *path*." );
        }
        else if ( !wresExecutable.canExecute() )
        {
            throw new IllegalArgumentException( "First arg must be an *executable* wres path." );
        }

        // Get work from the queue
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( "localhost" );

        try ( Connection connection = factory.newConnection();
              Channel channel = connection.createChannel() )
        {
            channel.queueDeclare( RECV_QUEUE_NAME, false, false, false, null );
            JobReceiver receiver = new JobReceiver( channel, wresExecutable );

            while ( true )
            {
                LOGGER.info( "Waiting for work..." );
                channel.basicConsume( RECV_QUEUE_NAME, true, receiver );
                Thread.sleep( 2000 );
            }
        }
    }


    private static final class JobReceiver extends DefaultConsumer
    {
        private final File wresExecutable;

        /**
         * Constructs a new instance and records its association to the passed-in channel.
         * @param channel the channel to which this consumer is attached
         * @param wresExecutable the wresExecutable to use for launching WRES
         */
        JobReceiver( Channel channel, File wresExecutable )
        {
            super( channel );
            this.wresExecutable = wresExecutable;
        }

        File getWresExecutable()
        {
            return this.wresExecutable;
        }

        @Override
        public void handleDelivery( String consumerTag,
                                    Envelope envelope,
                                    AMQP.BasicProperties properties,
                                    byte[] body )
        {
            String message = new String( body, Charset.forName( "UTF-8" ) );
            LOGGER.info( "Received job message {}", message );

            // Translate the message into a command
            ProcessBuilder processBuilder = createBuilderFromMessage( message );

            // Check to see if there is any command at all.
            if ( processBuilder == null )
            {
                LOGGER.warn( "Could not execute due to invalid message." );
                return;
            }

            // Do the execution requested from the queue


            Process process;

            try
            {
                process = processBuilder.start();
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to launch process from {}.", processBuilder, ioe );
                return;
            }

            LOGGER.info( "Started subprocess {}", process );

            try
            {
                process.waitFor();
                int exitValue = process.exitValue();
                LOGGER.info( "Subprocess {} exited {}", process, exitValue );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while waiting for {}.", process );
                Thread.currentThread().interrupt();
            }
        }


        /**
         * Translate a message from the queue into a ProcessBuilder to run
         * @param message
         * @return
         */
        ProcessBuilder createBuilderFromMessage( String message )
        {
            List<String> result = new ArrayList<>();

            String javaOpts = null;
            String executable = this.getWresExecutable().getPath();
            String command = "execute";
            String projectConfig = null;

            String[] messageParts = message.split( "," );

            for ( String messagePart : messageParts )
            {
                int indexOfEquals = messagePart.indexOf( '=' );

                if ( indexOfEquals < 1 || indexOfEquals == messagePart.length() )
                {
                    LOGGER.warn( "Bad message, no equals here, or nothing after equals: '{}'", messagePart );
                    return null;
                }

                String first = messagePart.substring( 0, indexOfEquals);
                String second = messagePart.substring( indexOfEquals + 1);

                if ( first.toUpperCase().equals( "JAVA_OPTS" ) )
                {
                    javaOpts = second;
                }
                else if ( first.toLowerCase().equals( "projectconfig" ) )
                {
                    projectConfig = second;
                }
            }

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

            // Cause process builder to echo the subprocess's output when started.
            processBuilder.inheritIO();

            // Cause process builder to get java options if needed
            if ( javaOpts != null )
            {
                processBuilder.environment().put( "JAVA_OPTS", javaOpts );
            }

            return processBuilder;
        }
    }
}
