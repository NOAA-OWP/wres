package wres.eventsbroker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

/**
 * Tests the {@link BrokerConnectionFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BrokerConnectionFactoryTest
{

    @Test
    public void testCreateTwoFactoryInstancesCreatesOneEmbeddedBroker() throws IOException
    {
        // Create and start the broker, clean up on completion
        try ( BrokerConnectionFactory factoryOne = BrokerConnectionFactory.of(); )
        {
            // First factory creates an embedded broker when no other broker is available
            assertTrue( factoryOne.hasEmbeddedBroker() );

            try( BrokerConnectionFactory factoryTwo = BrokerConnectionFactory.of() )
            {
                // Second factory does not attempt to create a new broker (doing so would produce a connection binding 
                // error on attempting to connect)
                assertFalse( factoryTwo.hasEmbeddedBroker() );
            }          
        }
    }
}
