package wres.events;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import wres.eventsbroker.BrokerConnectionFactory;

/**
 * Tests the {@link MessageSubscriber}
 * 
 * @author james.brown@hydrosolved.com
 */

public class MessageSubscriberTest
{

    /**
     * Connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    @BeforeClass
    public static void runBeforeAllTests()
    {
        MessageSubscriberTest.connections = BrokerConnectionFactory.of();
    }

    @Test
    public void consumeOneMessageFilteredByEvaluationAndGroup()
            throws IOException, NamingException, JMSException, InterruptedException
    {
        // Create and start a broker and open an evaluation, closing on completion
        Destination destination = MessageSubscriberTest.connections.getDestination( "status" );
        Function<ByteBuffer, Integer> mapper = buffer -> buffer.getInt();
        AtomicInteger actual = new AtomicInteger();
        Consumer<Integer> consumer = anInt -> actual.set( anInt );

        // Bytes sent, representing integer 1695609641
        ByteBuffer sent = ByteBuffer.wrap( new byte[] { (byte) 0x65, (byte) 0x10, (byte) 0xf3, (byte) 0x29 } );

        try ( MessagePublisher publisher = MessagePublisher.of( MessageSubscriberTest.connections.get(),
                                                                destination );
              MessageSubscriber subscriber =
                      new MessageSubscriber.Builder<Integer>().setConnectionFactory( MessageSubscriberTest.connections.get() )
                                                              .setDestination( destination )
                                                              .setMapper( mapper )
                                                              .setEvaluationId( "someEvaluationId" )
                                                              .setGroupId( "someGroupId" )
                                                              .addSubscribers( List.of( consumer ) )
                                                              .build() )
        {
            // Signal one message
            subscriber.advanceCountToAwaitOnClose();

            // Must set the evaluation and group identifiers because messages are filtered by these
            publisher.publish( sent,
                               "ID:someId",
                               "someEvaluationId",
                               "someGroupId" );

            // Wait
            Phaser phase = subscriber.getStatus();
            phase.awaitAdvance( 0 );

            assertEquals( 1695609641, actual.get() );
        }
    }

    @AfterClass
    public static void runAfterAllTests() throws IOException
    {
        MessageSubscriberTest.connections.close();
    }

}
