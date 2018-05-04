package wres.worker;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import com.rabbitmq.client.DefaultSaslConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.messages.BrokerHelper;

/**
 * A long-running, light-weight process that takes a job from a queue, and runs
 * a single WRES instance for the job taken from the queue, and repeats.
 */

public class Worker
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Worker.class );
    private static final String RECV_QUEUE_NAME = "wres.job";


    /**
     * Expects exactly one arg with a path to WRES executable
     * @param args arguments, but only one is expected, a WRES executable
     * @throws IOException when communication with queue fails or process start fails.
     * @throws IllegalArgumentException when the first argument is not a WRES executable
     * @throws java.net.ConnectException when connection to queue fails.
     * @throws TimeoutException when connection to the queue times out.
     * @throws InterruptedException when interrupted while waiting for work.
     * @throws IllegalStateException when setting up our custom trust list fails
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
        String brokerHost = BrokerHelper.getBrokerHost();
        String brokerVhost = BrokerHelper.getBrokerVhost();
        int brokerPort = BrokerHelper.getBrokerPort();
        LOGGER.info( "Using broker at host '{}', vhost '{}', port '{}'",
                     brokerHost, brokerVhost, brokerPort );

        // Set up connection parameters for connection to broker
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( brokerHost );
        factory.setVirtualHost( brokerVhost );
        factory.setPort( brokerPort );
        factory.setSaslConfig( DefaultSaslConfig.EXTERNAL );

        SSLContext sslContext =
                BrokerHelper.getSSLContextWithClientCertificate( BrokerHelper.Role.WORKER );
        factory.useSslProtocol( sslContext );

        // Get work from the queue
        try ( Connection connection = factory.newConnection();
              Channel receiveChannel = connection.createChannel() )
        {
            // Take precisely one job at a time:
            receiveChannel.basicQos( 1 );

            receiveChannel.queueDeclare( RECV_QUEUE_NAME, false, false, false, null );

            BlockingQueue<WresProcess> processToLaunch = new ArrayBlockingQueue<>( 1 );

            JobReceiver receiver = new JobReceiver( receiveChannel,
                                                    wresExecutable,
                                                    processToLaunch );

            receiveChannel.basicConsume( RECV_QUEUE_NAME, false, receiver );

            while ( true )
            {
                LOGGER.info( "Waiting for work..." );
                WresProcess wresProcess = processToLaunch.poll( 2, TimeUnit.SECONDS );

                if ( wresProcess != null )
                {
                    // Launch WRES if the consumer found a message saying so.
                    wresProcess.call();
                    // Tell broker it is OK to get more messages by acknowledging
                    receiveChannel.basicAck( wresProcess.getDeliveryTag(), false );
                }
            }
        }
    }
}
