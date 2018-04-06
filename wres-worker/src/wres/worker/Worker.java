package wres.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

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
}
