package wres.vis.client;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.events.EvaluationEventUtilities;
import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.client.MessagingClient;
import wres.events.subscribe.ConsumerFactory;
import wres.eventsbroker.embedded.EmbeddedBroker;

/**
 * A long-running graphics client that encapsulates one graphics subscriber, which consumes statistics and writes them 
 * to graphics.
 *
 * @author James Brown
 */

class GraphicsClient
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsClient.class );

    /**
     * Start the graphics server.
     * @param args the command line arguments
     */

    public static void main( String[] args )
    {
        LOGGER.info( "Starting WRES messaging client for graphics format writing..." );

        // Create the server
        int exitCode = 0;

        // Create the broker connections for statistics messaging
        Properties brokerConnectionProperties =
                BrokerUtilities.getBrokerConnectionProperties( BrokerConnectionFactory.DEFAULT_PROPERTIES );

        // Create an embedded broker for statistics messages, if needed
        EmbeddedBroker broker = null;
        if ( BrokerUtilities.isEmbeddedBrokerRequired( brokerConnectionProperties ) )
        {
            // No dynamic binding, nominated port only
            broker = EmbeddedBroker.of( brokerConnectionProperties, false );
        }
        final EmbeddedBroker brokerToClose = broker;

        BrokerConnectionFactory brokerConnections = BrokerConnectionFactory.of( brokerConnectionProperties );

        // Client identifier = identifier of the one subscriber it composes
        String subscriberId = EvaluationEventUtilities.getId();

        // A factory that creates consumers on demand
        ConsumerFactory consumerFactory = new GraphicsConsumerFactory( subscriberId );

        MessagingClient graphics = MessagingClient.of( brokerConnections, consumerFactory );

        // Add a shutdown hook to respond gracefully to SIGINT signals
        Runtime.getRuntime()
               .addShutdownHook( new Thread( () -> {

                   // Close the resources
                   LOGGER.info( "Stopping WRES messaging client for graphics format writing..." );
                   graphics.stop();

                   if ( Objects.nonNull( brokerToClose ) )
                   {
                       try
                       {
                           LOGGER.info( "Closing embedded broker {}.", brokerToClose );
                           brokerToClose.close();
                       }
                       catch ( IOException e )
                       {
                           LOGGER.error( "Failed to close the embedded broker associated with graphics client {}.",
                                         graphics );

                       }
                   }
               } ) );

        try
        {
            // Start the subscriber
            graphics.start();

            // Await completion
            graphics.await();
        }
        catch ( InterruptedException e )
        {
            LOGGER.error( "Interrupted while waiting for a WRES Graphics Client." );

            exitCode = 1;

            Thread.currentThread()
                  .interrupt();
        }
        catch ( MessagingClient.MessagingClientException f )
        {
            LOGGER.error( "Encountered an internal error in a WRES Graphics Client, which will now shut down.", f );

            exitCode = 2;
        }

        // Failed subscriber?
        if ( graphics.getSubscriberStatus()
                     .isFailed() )
        {
            exitCode = 3;
        }

        System.exit( exitCode );
    }

    /**
     * Do not construct.
     */

    private GraphicsClient()
    {
    }
}
