package wres.events.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import wres.eventsbroker.embedded.EmbeddedBroker;

/**
 * Tests the {@link BrokerConnectionFactory}.
 * 
 * @author James Brown
 */

class BrokerUtilitiesTest
{

    @Test
    void testGetConnectionPropertyName()
    {
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        
        assertEquals( "connectionFactory.statisticsFactory", BrokerUtilities.getConnectionPropertyName( properties ) );
    }
    
    @Test
    void testIsEmbeddedBrokerRequired() throws IOException
    {
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        assertTrue( BrokerUtilities.isEmbeddedBrokerRequired( properties ) );
    }
    
    @Test
    void testIsEmbeddedBrokerRequiredWhenOverrideIsTrue() throws IOException
    {
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        
        // Test system property overrides
        System.setProperty( "wres.startBroker", "true" );
        assertTrue( BrokerUtilities.isEmbeddedBrokerRequired( properties ) );
    }
    
    @Test
    void testIsEmbeddedBrokerRequiredWhenOverrideIsFalse() throws IOException
    {
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );

        System.setProperty( "wres.startBroker", "false" );
        assertFalse( BrokerUtilities.isEmbeddedBrokerRequired( properties ) );
    }    
    
    @Test
    void testIsEmbeddedBrokerRequiredWhenPortBoundByNonBroker() throws IOException
    {
        // Bind a port and create a URL with the bound port
        try ( ServerSocket socket = new ServerSocket( 0 ) )
        {
            int boundPort = socket.getLocalPort();

            Properties innerProperties = new Properties();
            innerProperties.put( "connectionFactory.statisticsFactory", "amqp://localhost:" + boundPort );

            assertTrue( BrokerUtilities.isEmbeddedBrokerRequired( innerProperties ) );
        }
    }
    
    @Test
    void testIsEmbeddedBrokerRequiredWhenPortBoundByBroker() throws IOException
    {
        // Bind a port and create a URL with the bound port
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        try ( EmbeddedBroker broker = EmbeddedBroker.of( properties, true ) )
        {
            // Properties are updated automatically with the bound port
            assertFalse( BrokerUtilities.isEmbeddedBrokerRequired( properties ) );
        }
    }
    
}
