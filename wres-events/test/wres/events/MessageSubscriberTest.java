package wres.events;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

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
    public void consumeOneMessageFilteredByEvaluationId()
            throws IOException, NamingException, JMSException, InterruptedException
    {
        // Create and start a broker and open an evaluation, closing on completion
        Destination destination = MessageSubscriberTest.connections.getDestination( "status" );
        Function<ByteBuffer, Integer> mapper = buffer -> buffer.getInt();
        AtomicInteger actualOne = new AtomicInteger();
        Consumer<Integer> consumerOne = anInt -> actualOne.set( anInt );

        // Bytes sent, representing integer 1695609641
        ByteBuffer sentOne = ByteBuffer.wrap( new byte[] { (byte) 0x65, (byte) 0x10, (byte) 0xf3, (byte) 0x29 } );


        // Mock completion: decrement latch when a new consumption is registered
        CountDownLatch latch = new CountDownLatch( 1 );
        CompletionTracker completionNotifier = Mockito.mock( CompletionTracker.class );

        Mockito.doAnswer( new Answer<>()
        {
            public Object answer( InvocationOnMock invocation )
            {
                latch.countDown();
                return null;
            }
        } ).when( completionNotifier ).register();

        try ( MessagePublisher publisher = MessagePublisher.of( MessageSubscriberTest.connections.get(),
                                                                destination );
              MessageSubscriber<Integer> subscriberOne =
                      new MessageSubscriber.Builder<Integer>().setConnectionFactory( MessageSubscriberTest.connections.get() )
                                                              .setDestination( destination )
                                                              .setMapper( mapper )
                                                              .setCompletionTracker( completionNotifier )
                                                              .setEvaluationId( "someEvaluationId" )
                                                              .addSubscribers( List.of( consumerOne ) )
                                                              .build(); )
        {
            publisher.publish( sentOne,
                               "ID:someId",
                               "someEvaluationId" );

            // Must set the evaluation and group identifiers because messages are filtered by these           
            latch.await();

            assertEquals( 1695609641, actualOne.get() );
        }
    }

    @Test
    public void consumeTwoMessagesFilteredByEvaluationIdAndGroupIdForTwoGroups()
            throws IOException, NamingException, JMSException, InterruptedException
    {
        // Create and start a broker and open an evaluation, closing on completion
        Destination statisticsDestination = MessageSubscriberTest.connections.getDestination( "statistics" );
        Destination statusDestination = MessageSubscriberTest.connections.getDestination( "status" );
        Function<ByteBuffer, Integer> mapper = buffer -> buffer.getInt();
        List<Integer> actual = new ArrayList<>();

        // Each consumer group adds two integers together from two separate integer messages
        Function<List<Integer>, Integer> groupAggregator = list -> list.stream().mapToInt( Integer::intValue ).sum();
        Consumer<Integer> consumer = anInt -> actual.add( anInt );

        // Bytes sent, representing integers 1695609641, 243, 1746072600 and 7, respectively
        ByteBuffer sentOne = ByteBuffer.wrap( new byte[] { (byte) 0x65, (byte) 0x10, (byte) 0xf3, (byte) 0x29 } );
        ByteBuffer sentTwo = ByteBuffer.wrap( new byte[] { (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0xf3 } );
        ByteBuffer sentThree = ByteBuffer.wrap( new byte[] { (byte) 0x68, (byte) 0x12, (byte) 0xf4, (byte) 0x18 } );
        ByteBuffer sentFour = ByteBuffer.wrap( new byte[] { (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x7 } );

        // Group complete message
        EvaluationStatus status =
                EvaluationStatus.newBuilder()
                                .setCompletionStatus( CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
                                .setMessageCount( 2 )
                                .build();

        ByteBuffer groupComplete = ByteBuffer.wrap( status.toByteArray() );

        // Mock completion: decrement latch when a new consumption is registered
        CountDownLatch latch = new CountDownLatch( 4 );
        CompletionTracker completionTracker = Mockito.mock( CompletionTracker.class );

        Mockito.doAnswer( new Answer<>()
        {
            public Object answer( InvocationOnMock invocation )
            {
                latch.countDown();

                return null;
            }
        } ).when( completionTracker ).register();

        Mockito.when( completionTracker.getExpectedMessagesPerGroup( "someGroupId" ) ).thenReturn( 2 );
        Mockito.when( completionTracker.getExpectedMessagesPerGroup( "anotherGroupId" ) ).thenReturn( 2 );

        try ( MessagePublisher publisher = MessagePublisher.of( MessageSubscriberTest.connections.get(),
                                                                statisticsDestination );
              MessagePublisher statusPublisher =
                      MessagePublisher.of( MessageSubscriberTest.connections.get(), statusDestination );
              MessageSubscriber<Integer> subscriberOne =
                      new MessageSubscriber.Builder<Integer>().setConnectionFactory( MessageSubscriberTest.connections.get() )
                                                              .setDestination( statisticsDestination )
                                                              .setEvaluationStatusDestination( statusDestination )
                                                              .setMapper( mapper )
                                                              .setEvaluationId( "someEvaluationId" )
                                                              .setCompletionTracker( completionTracker )
                                                              .addGroupSubscribers( List.of( consumer ) )
                                                              .setGroupAggregator( groupAggregator )
                                                              .build(); )
        {
            // Must set the evaluation and group identifiers because messages are filtered by these
            publisher.publish( sentOne,
                               "ID:123",
                               "someEvaluationId",
                               "someGroupId" );

            publisher.publish( sentTwo,
                               "ID:456",
                               "someEvaluationId",
                               "someGroupId" );

            // Group complete
            statusPublisher.publish( groupComplete, "ID:131415", "someEvaluationId", "someGroupId" );

            publisher.publish( sentThree,
                               "ID:789",
                               "someEvaluationId",
                               "anotherGroupId" );

            publisher.publish( sentFour,
                               "ID:101112",
                               "someEvaluationId",
                               "anotherGroupId" );

            // Another group complete
            statusPublisher.publish( groupComplete, "ID:161718", "someEvaluationId", "anotherGroupId" );

            latch.await();
        }

        List<Integer> expected = List.of( 1695609641 + 243, 1746072600 + 7 );
        
        assertEquals( expected, actual );
    }

    @AfterClass
    public static void runAfterAllTests() throws IOException
    {
        MessageSubscriberTest.connections.close();
    }

}
