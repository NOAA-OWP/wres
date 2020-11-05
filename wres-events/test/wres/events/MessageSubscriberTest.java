package wres.events;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Topic;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import wres.events.Evaluation.EvaluationInfo;
import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
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
        Topic destination = (Topic) MessageSubscriberTest.connections.getDestination( "statistics" );
        Topic statusDestination = (Topic) MessageSubscriberTest.connections.getDestination( "status" );

        Function<ByteBuffer, Integer> mapper = buffer -> buffer.getInt();
        AtomicInteger actualOne = new AtomicInteger();
        Function<Integer, Set<Path>> consumerOne = anInt -> {
            actualOne.set( anInt );
            return Collections.emptySet();
        };

        // Bytes sent, representing integer 1695609641
        ByteBuffer sentOne = ByteBuffer.wrap( new byte[] { (byte) 0x65, (byte) 0x10, (byte) 0xf3, (byte) 0x29 } );

        EvaluationInfo evaluationInfo = EvaluationInfo.of( "someEvaluationId" );

        // Publication complete
        EvaluationStatus published =
                EvaluationStatus.newBuilder()
                                .setCompletionStatus( CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS )
                                .setMessageCount( 1 )
                                .build();
        ByteBuffer publicationComplete = ByteBuffer.wrap( published.toByteArray() );

        try ( Connection publisherConnection = MessageSubscriberTest.connections.get().createConnection();
              MessagePublisher publisher =
                      MessagePublisher.of( publisherConnection,
                                           destination );
              MessagePublisher statusPublisher =
                      MessagePublisher.of( publisherConnection,
                                           statusDestination );
              Connection consumerConnection = MessageSubscriberTest.connections.get()
                                                                               .createConnection();
              Connection producerConnection = MessageSubscriberTest.connections.get()
                                                                               .createConnection();
              MessageSubscriber<Integer> subscriberOne =
                      new MessageSubscriber.Builder<Integer>().setConsumerConnection( consumerConnection )
                                                              .setProducerConnection( producerConnection )
                                                              .setTopic( destination )
                                                              .setMapper( mapper )
                                                              .setEvaluationInfo( evaluationInfo )
                                                              .setExpectedMessageCountSupplier( EvaluationStatus::getMessageCount )
                                                              .addSubscribers( List.of( consumerOne ) )
                                                              .setEvaluationStatusTopic( statusDestination )
                                                              .build(); )
        {
            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:someId" );
            properties.put( MessageProperty.JMS_CORRELATION_ID, "someEvaluationId" );

            publisher.publish( sentOne, Collections.unmodifiableMap( properties ) );

            // Publication complete
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:anotherId" );
            statusPublisher.publish( publicationComplete, Collections.unmodifiableMap( properties ) );

            // Wait until one message was received
            while ( !subscriberOne.isComplete() )
            {
            }

            assertEquals( 1695609641, actualOne.get() );
        }
    }

    @Test
    public void consumeTwoMessagesFilteredByEvaluationIdAndGroupIdForTwoGroups()
            throws IOException, NamingException, JMSException, InterruptedException
    {
        // Create and start a broker and open an evaluation, closing on completion
        Topic statisticsDestination = (Topic) MessageSubscriberTest.connections.getDestination( "statistics" );
        Topic statusDestination = (Topic) MessageSubscriberTest.connections.getDestination( "status" );
        Function<ByteBuffer, Integer> mapper = buffer -> buffer.getInt();
        List<Integer> actual = new ArrayList<>();

        // Each consumer group adds two integers together from two separate integer messages
        Function<Collection<Integer>, Integer> groupAggregator = list -> list.stream()
                                                                             .mapToInt( Integer::intValue )
                                                                             .sum();
        Function<Collection<Integer>, Set<Path>> consumer = aList -> {
            actual.add( groupAggregator.apply( aList ) );
            return Collections.emptySet();
        };

        // Bytes sent, representing integers 1695609641, 243, 1746072600 and 7, respectively
        ByteBuffer sentOne = ByteBuffer.wrap( new byte[] { (byte) 0x65, (byte) 0x10, (byte) 0xf3, (byte) 0x29 } );
        ByteBuffer sentTwo = ByteBuffer.wrap( new byte[] { (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0xf3 } );
        ByteBuffer sentThree = ByteBuffer.wrap( new byte[] { (byte) 0x68, (byte) 0x12, (byte) 0xf4, (byte) 0x18 } );
        ByteBuffer sentFour = ByteBuffer.wrap( new byte[] { (byte) 0x0, (byte) 0x0, (byte) 0x0, (byte) 0x7 } );

        // Group complete message
        EvaluationStatus status =
                EvaluationStatus.newBuilder()
                                .setCompletionStatus( CompletionStatus.GROUP_PUBLICATION_COMPLETE )
                                .setMessageCount( 2 )
                                .build();

        // Publication complete
        EvaluationStatus published =
                EvaluationStatus.newBuilder()
                                .setCompletionStatus( CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS )
                                .setMessageCount( 4 )
                                .build();

        ByteBuffer publicationComplete = ByteBuffer.wrap( published.toByteArray() );


        // Mock completion: decrement latch when a new consumption is registered
        GroupCompletionTracker completionTracker = Mockito.mock( GroupCompletionTracker.class );

        Mockito.when( completionTracker.getExpectedMessagesPerGroup( "someGroupId" ) ).thenReturn( 2 );
        Mockito.when( completionTracker.getExpectedMessagesPerGroup( "anotherGroupId" ) ).thenReturn( 2 );

        EvaluationInfo evaluationInfo = EvaluationInfo.of( "someEvaluationId", completionTracker );

        try ( Connection publisherConnection = MessageSubscriberTest.connections.get().createConnection();
              MessagePublisher publisher =
                      MessagePublisher.of( publisherConnection,
                                           statisticsDestination );
              MessagePublisher statusPublisher =
                      MessagePublisher.of( publisherConnection,
                                           statusDestination );
              Connection consumerConnection = MessageSubscriberTest.connections.get().createConnection();
              Connection producerConnection = MessageSubscriberTest.connections.get().createConnection();
              MessageSubscriber<Integer> subscriberOne =
                      new MessageSubscriber.Builder<Integer>().setConsumerConnection( consumerConnection )
                                                              .setProducerConnection( producerConnection )
                                                              .setTopic( statisticsDestination )
                                                              .setEvaluationStatusTopic( statusDestination )
                                                              .setExpectedMessageCountSupplier( EvaluationStatus::getMessageCount )
                                                              .setMapper( mapper )
                                                              .setEvaluationInfo( evaluationInfo )
                                                              .addGroupSubscribers( List.of( consumer ) )
                                                              .build(); )
        {

            // Must set the evaluation and group identifiers because messages are filtered by these
            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:123" );
            properties.put( MessageProperty.JMS_CORRELATION_ID, "someEvaluationId" );
            properties.put( MessageProperty.JMSX_GROUP_ID, "someGroupId" );

            publisher.publish( sentOne, Collections.unmodifiableMap( properties ) );

            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:456" );

            publisher.publish( sentTwo, Collections.unmodifiableMap( properties ) );

            // Group complete
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:131415" );
            EvaluationStatus groupOne = status.toBuilder()
                                              .setGroupId( "someGroupId" )
                                              .build();
            statusPublisher.publish( ByteBuffer.wrap( groupOne.toByteArray() ),
                                     Collections.unmodifiableMap( properties ) );

            // Start another group
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:789" );
            properties.put( MessageProperty.JMSX_GROUP_ID, "anotherGroupId" );

            publisher.publish( sentThree, Collections.unmodifiableMap( properties ) );

            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:101112" );
            publisher.publish( sentFour, Collections.unmodifiableMap( properties ) );

            // Another group complete
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:161718" );
            EvaluationStatus groupTwo = status.toBuilder()
                                              .setGroupId( "anotherGroupId" )
                                              .build();
            statusPublisher.publish( ByteBuffer.wrap( groupTwo.toByteArray() ),
                                     Collections.unmodifiableMap( properties ) );

            // Publication complete
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:192021" );
            properties.remove( MessageProperty.JMSX_GROUP_ID );
            statusPublisher.publish( publicationComplete, Collections.unmodifiableMap( properties ) );

            // Wait until four messages were received
            while ( !subscriberOne.isComplete() )
            {
            }
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
