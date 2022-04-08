package wres.eventsbroker.embedded;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link EmbeddedBroker}.
 * 
 * @author James Brown
 */

public class EmbeddedBrokerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger( EmbeddedBrokerTest.class );

    @Test
    public void testConnectionSucceeds() throws Exception
    {
        // Create and start the broker, clean up on completion
        try ( EmbeddedBroker embeddedBroker = EmbeddedBroker.of(); )
        {
            embeddedBroker.start();

            // Dynamic port assigned
            int amqpPort = embeddedBroker.getMessagingPort();
            assertTrue( amqpPort > 0 );

            LOGGER.info( "The testConnectionSucceeds test created an embedded broker, which bound AMQP transport to "
                         + "ephemeral port {}.",
                         amqpPort );
        }
    }

}
