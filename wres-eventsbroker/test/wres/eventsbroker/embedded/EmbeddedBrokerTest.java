package wres.eventsbroker.embedded;

import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

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
        String jndiProperties = "eventbroker.properties";
        URL config = EmbeddedBrokerTest.class.getClassLoader().getResource( jndiProperties );

        Properties properties = new Properties();
        try ( InputStream stream = config.openStream() )
        {
            properties.load( stream );

            try ( EmbeddedBroker embeddedBroker = EmbeddedBroker.of( properties, true ); )
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

}
