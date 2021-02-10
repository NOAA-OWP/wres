package wres.events;

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
import wres.events.subscribe.ConsumerException;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.EvaluationSubscriber.SubscriberStatus;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Consumer.Format;

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
            while( subscriber.getSubscriberStatus().getEvaluationFailedCount() < 1 )
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

    @AfterAll
    static void runAfterAllTests() throws IOException
    {
        EvaluationSubscriberTest.connections.close();
    }

}
