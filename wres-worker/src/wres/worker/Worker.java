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

/**
 * A long-running, light-weight process that takes a job from a queue, and runs
 * a single WRES instance for the job taken from the queue, and repeats.
 */

public class Worker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Worker.class );
    private static final String RECV_QUEUE_NAME = "wres.job";
    private static final String SEND_QUEUE_NAME = "wres.jobResults";

    private static final String BROKER_HOST_PROPERTY_NAME = "wres.broker";
    private static final String DEFAULT_BROKER_HOST = "localhost";

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

        // Determine the actual broker name, whether from -D or default
        String brokerHost = Worker.getBrokerHost();

        // Get work from the queue
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( brokerHost );

        try ( Connection connection = factory.newConnection();
              Channel receiveChannel = connection.createChannel();
              Channel sendChannel = connection.createChannel() )
        {
            receiveChannel.queueDeclare( RECV_QUEUE_NAME, false, false, false, null );
            sendChannel.queueDeclare( SEND_QUEUE_NAME, false, false, false, null );
            JobReceiver receiver = new JobReceiver( receiveChannel,
                                                    wresExecutable,
                                                    sendChannel,
                                                    SEND_QUEUE_NAME );

            while ( true )
            {
                LOGGER.info( "Waiting for work..." );
                receiveChannel.basicConsume( RECV_QUEUE_NAME, true, receiver );
                Thread.sleep( 2000 );
            }
        }
    }


    /**
     * Helper to get the broker host name. Returns what was set in -D args
     * or a default value if -D is not set.
     * @return the broker host name to try connecting to.
     */

    private static final String getBrokerHost()
    {
        String brokerFromDashD= System.getProperty( BROKER_HOST_PROPERTY_NAME );

        if ( brokerFromDashD != null )
        {
            return brokerFromDashD;
        }
        else
        {
            return DEFAULT_BROKER_HOST;
        }
    }

}
