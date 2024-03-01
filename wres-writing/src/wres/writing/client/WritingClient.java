package wres.writing.client;

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
 * A long-running numerics client that encapsulates one numerics subscriber, which consumes statistics and writes them
 * to numeric formats (csv, protobuff, etc).
 *
 * @author Evan Pagryzinski
 */

class WritingClient
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WritingClient.class );

    /**
     * Start the writing server.
     * @param args the command line arguments
     */

    public static void main( String[] args )
    {
        LOGGER.info( "Starting WRES messaging client for numerics format writing..." );

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
        ConsumerFactory consumerFactory = new WritingConsumerFactory( subscriberId );

        MessagingClient numerics = MessagingClient.of( brokerConnections, consumerFactory );

        // Add a shutdown hook to respond gracefully to SIGINT signals
        Runtime.getRuntime()
               .addShutdownHook( new Thread( () -> {

                   // Close the resources
                   LOGGER.info( "Stopping WRES messaging client for numerics format writing..." );
                   numerics.stop();

                   if ( Objects.nonNull( brokerToClose ) )
                   {
                       try
                       {
                           LOGGER.info( "Closing embedded broker {}.", brokerToClose );
                           brokerToClose.close();
                       }
                       catch ( IOException e )
                       {
                           LOGGER.error( "Failed to close the embedded broker associated with numerics client {}.",
                                         numerics );

                       }
                   }
               } ) );

        try
        {
            // Start the subscriber
            numerics.start();

            // Await completion
            numerics.await();
        }
        catch ( InterruptedException e )
        {
            LOGGER.error( "Interrupted while waiting for a WRES numerics Client." );

            exitCode = 1;

            Thread.currentThread()
                  .interrupt();
        }

        // Failed subscriber?
        if ( numerics.getSubscriberStatus()
                     .isFailed() )
        {
            exitCode = 3;
        }

        System.exit( exitCode );
    }

    /**
     * Do not construct.
     */

    private WritingClient()
    {
    }
}
