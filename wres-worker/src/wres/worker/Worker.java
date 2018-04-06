package wres.worker;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
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
     */

    public static void main( String[] args )
            throws IOException, TimeoutException
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
            channel.basicConsume( RECV_QUEUE_NAME, true, receiver );
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
                throws IOException
        {
            String message = new String( body, Charset.forName( "UTF-8" ) );
            LOGGER.info( "Received job message {}", message );

            // Do the execution requested from the queue

            String command = "execute";
            String projectFile = "project_config.xml";

            ProcessBuilder processBuilder = new ProcessBuilder( this.getWresExecutable().getPath(),
                                                                command,
                                                                projectFile );

            // Cause process builder to echo the subprocess's output when started.
            processBuilder.inheritIO();

            Process process = processBuilder.start();

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
    }
}
