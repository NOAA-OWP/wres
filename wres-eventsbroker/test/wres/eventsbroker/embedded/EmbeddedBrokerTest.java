package wres.eventsbroker.embedded;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the {@link EmbeddedBroker}.
 * 
 * @author James Brown
 */

class EmbeddedBrokerTest
{

    private static final Logger LOGGER = LoggerFactory.getLogger( EmbeddedBrokerTest.class );

    @Test
    void testConnectionSucceeds() throws Exception
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

                LOGGER.info( "The testConnectionSucceeds test created an embedded broker, which bound AMQP transport "
                             + "to ephemeral port {}.",
                             amqpPort );
            }
        }
    }

    @Test
    void testEphemeralPortUpdatedInProperties() throws Exception
    {
        // Create and start the broker, clean up on completion
        String jndiProperties = "eventbroker.properties";
        URL config = EmbeddedBrokerTest.class.getClassLoader().getResource( jndiProperties );

        Properties properties = new Properties();
        int amqpPort = -1;
        try ( InputStream stream = config.openStream() )
        {
            properties.load( stream );

            try ( EmbeddedBroker embeddedBroker = EmbeddedBroker.of( properties, true ); )
            {
                amqpPort = embeddedBroker.getMessagingPort();
            }
        }

        // Acquire the port from the properties
        String bindingUrl = properties.getProperty( "connectionFactory.statisticsFactory" );
        Pattern p = Pattern.compile( ":(?<port>[0-9]+)" );
        Matcher m = p.matcher( bindingUrl );

        assertTrue( m.find(), "Could not find port in binding URL " + bindingUrl );

        String portString = m.group().replace( ":", "" );
        int freePort = Integer.valueOf( portString );

        // Port acquired from the properties is the same as the one acquired from the broker
        assertEquals( freePort, amqpPort );
    }

    @Test
    void testConstructionFailsWhenAttemptingToBindAlreadyBoundPortAndDynamicBindingNotAllowed() throws IOException
    {
        try ( ServerSocket socket = new ServerSocket( 0 ) )
        {
            int boundPort = socket.getLocalPort();

            Properties properties = new Properties();
            properties.put( "connectionFactory.statisticsFactory", "amqp://localhost:" + boundPort );

            assertThrows( CouldNotStartEmbeddedBrokerException.class,
                          () -> EmbeddedBroker.of( properties, false ) ); // No dynamic binding
        }
    }

    @Test
    void testConstructionSucceedsWhenAttemptingToBindAlreadyBoundPortAndDynamicBindingAllowed() throws IOException
    {
        try ( ServerSocket socket = new ServerSocket( 0 ) )
        {
            int boundPort = socket.getLocalPort();

            Properties properties = new Properties();
            properties.put( "connectionFactory.statisticsFactory", "amqp://localhost:" + boundPort );

            // Dynamic binding allowed, so initial failure will be recovered
            try ( EmbeddedBroker embeddedBroker = EmbeddedBroker.of( properties, true ); )
            {
                int amqpPort = embeddedBroker.getMessagingPort();
                assertTrue( amqpPort > 0 );
            }
        }
    }

}
