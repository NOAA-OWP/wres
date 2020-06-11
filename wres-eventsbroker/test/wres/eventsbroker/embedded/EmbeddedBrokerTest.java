package wres.eventsbroker.embedded;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

import org.apache.qpid.client.AMQConnection;

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
        boolean connectionMade = false;

        // Create and start the broker, clean up on completion
        try ( EmbeddedBroker embeddedBroker = EmbeddedBroker.of(); )
        {
            embeddedBroker.start();

            try ( AMQConnection connection =
                    new AMQConnection( "amqp://guest:guest@test/?brokerlist='tcp://localhost:5673'" ); )
            {
                connectionMade = connection.isConnected();
            }
        }

        assertTrue( connectionMade );
    }

}
