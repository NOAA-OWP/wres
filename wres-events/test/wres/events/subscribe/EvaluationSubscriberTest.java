package wres.events.subscribe;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Function;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import wres.events.publish.MessagePublisher;
import wres.events.publish.MessagePublisher.MessageProperty;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

class EvaluationSubscriberTest
{

    /**
     * Connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    @BeforeAll
    static void runBeforeAllTests()
    {
        EvaluationSubscriberTest.connections = BrokerConnectionFactory.of();
    }

    @Test
    void testFailedEvaluationIsNotReopened() throws IOException, JMSException, NamingException
    {
        ConsumerFactory consumer = new ConsumerFactory()
        {
            @Override
            public Function<Statistics, Set<Path>>
                    getConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> {
                    throw new ConsumerException( "Evaluation failed!" );
                };
            }

            @Override
            public Function<Collection<Statistics>, Set<Path>>
                    getGroupedConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> Set.of();
            }

            @Override
            public Consumer getConsumerDescription()
            {
                return Consumer.newBuilder()
                               .setConsumerId( "aConsumer" )
                               .addFormats( Format.NETCDF )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        SubscriberStatus status = null;

        try ( EvaluationSubscriber subscriber = EvaluationSubscriber.of( consumer,
                                                                         Executors.newSingleThreadExecutor(),
                                                                         EvaluationSubscriberTest.connections );
              MessagePublisher evalPublisher =
                      MessagePublisher.of( EvaluationSubscriberTest.connections,
                                           EvaluationSubscriberTest.connections.getDestination( "evaluation" ) );
              MessagePublisher statsPublisher =
                      MessagePublisher.of( EvaluationSubscriberTest.connections,
                                           EvaluationSubscriberTest.connections.getDestination( "statistics" ) ); )
        {

            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.NETCDF, subscriber.getClientId() );
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:someId" );
            properties.put( MessageProperty.JMS_CORRELATION_ID, "someCorrelationId" );
            properties.put( MessageProperty.JMSX_GROUP_ID, "aGroupId" );

            ByteBuffer evalBytes =
                    ByteBuffer.wrap( wres.statistics.generated.Evaluation.getDefaultInstance().toByteArray() );
            ByteBuffer statsBytes = ByteBuffer.wrap( Statistics.getDefaultInstance().toByteArray() );

            // Publish an evaluation message
            evalPublisher.publish( evalBytes, Collections.unmodifiableMap( properties ) );

            // Publish a statistics message that propagates a failure
            statsPublisher.publish( statsBytes, Collections.unmodifiableMap( properties ) );

            // Wait until the failure occurs
            while ( subscriber.getSubscriberStatus().getEvaluationFailedCount() < 1 )
            {
                // Wait
            }

            // Attempt to sweep away the failed evaluation, which should remain
            subscriber.sweep();

            // Publish another statistics message, which should not cause an evaluation to be recreated
            statsPublisher.publish( statsBytes, Collections.unmodifiableMap( properties ) );

            status = subscriber.getSubscriberStatus();
        }

        // One evaluation was started, one failed
        assertEquals( 1, status.getEvaluationCount() );
        assertEquals( 1, status.getEvaluationFailedCount() );
    }

    @Test
    void testSuccessfulEvaluationIsNotReopened() throws IOException, JMSException, NamingException
    {
        ConsumerFactory consumer = new ConsumerFactory()
        {
            @Override
            public Function<Statistics, Set<Path>>
                    getConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> Set.of();
            }

            @Override
            public Function<Collection<Statistics>, Set<Path>>
                    getGroupedConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> Set.of();
            }

            @Override
            public Consumer getConsumerDescription()
            {
                return Consumer.newBuilder()
                               .setConsumerId( "aConsumer" )
                               .addFormats( Format.NETCDF )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        SubscriberStatus status = null;

        try ( EvaluationSubscriber subscriber = EvaluationSubscriber.of( consumer,
                                                                         Executors.newSingleThreadExecutor(),
                                                                         EvaluationSubscriberTest.connections );
              MessagePublisher evalPublisher =
                      MessagePublisher.of( EvaluationSubscriberTest.connections,
                                           EvaluationSubscriberTest.connections.getDestination( "evaluation" ) );
              MessagePublisher statsPublisher =
                      MessagePublisher.of( EvaluationSubscriberTest.connections,
                                           EvaluationSubscriberTest.connections.getDestination( "statistics" ) );
              MessagePublisher statusPublisher =
                      MessagePublisher.of( EvaluationSubscriberTest.connections,
                                           EvaluationSubscriberTest.connections.getDestination( "status" ) ); )
        {

            Map<MessageProperty, String> properties = new EnumMap<>( MessageProperty.class );
            properties.put( MessageProperty.NETCDF, subscriber.getClientId() );
            properties.put( MessageProperty.JMS_MESSAGE_ID, "ID:someId" );
            properties.put( MessageProperty.JMS_CORRELATION_ID, "someEvaluation" );

            ByteBuffer evalBytes =
                    ByteBuffer.wrap( wres.statistics.generated.Evaluation.getDefaultInstance().toByteArray() );
            ByteBuffer statsBytes = ByteBuffer.wrap( Statistics.getDefaultInstance().toByteArray() );

            // Publish an evaluation message
            evalPublisher.publish( evalBytes, Collections.unmodifiableMap( properties ) );
            
            // Publish a statistics message
            statsPublisher.publish( statsBytes, Collections.unmodifiableMap( properties ) );

            // Mark publication complete with one statistics message expected
            EvaluationStatus complete = EvaluationStatus.newBuilder()
                                                        .setCompletionStatus( CompletionStatus.PUBLICATION_COMPLETE_REPORTED_SUCCESS )
                                                        .setMessageCount( 2 )
                                                        .build();

            ByteBuffer statusBytes = ByteBuffer.wrap( complete.toByteArray() );
            statusPublisher.publish( statusBytes, Collections.unmodifiableMap( properties ) );

            // Await success
            while ( subscriber.getSubscriberStatus().getEvaluationCompletedCount() < 1 )
            {
                // Wait
            }

            // Attempt to sweep away the successful evaluation
            subscriber.sweep();

            // Publish a status message, which should not cause the same evaluation to be recreated
            statusPublisher.publish( statsBytes, Collections.unmodifiableMap( properties ) );

            status = subscriber.getSubscriberStatus();
        }

        // One evaluation was started, one completed
        assertEquals( 1, status.getEvaluationCount() );
        assertEquals( 1, status.getEvaluationCompletedCount() );
    }


    @AfterAll
    static void runAfterAllTests() throws IOException
    {
        EvaluationSubscriberTest.connections.close();
    }

}
