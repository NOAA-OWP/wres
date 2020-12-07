package wres.eventsbroker.embedded;

import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.Test;

/**
 * Tests the {@link EmbeddedBroker}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EmbeddedBrokerTest
{

    @Test
    public void testConnectionSucceeds() throws Exception
    {
        // Create and start the broker, clean up on completion
        try ( EmbeddedBroker embeddedBroker = EmbeddedBroker.of(); )
        {
            embeddedBroker.start();

            // Dynamic port assigned
            Map<String,Integer> ports = embeddedBroker.getBoundPorts();

            Integer port = ports.get( "wres-statistics-AMQP" );
            
            assertNotNull( port );
        }
    }

}
