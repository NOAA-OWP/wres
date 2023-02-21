package wres.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.events.subscribe.SubscriberApprover;
import wres.eventsbroker.embedded.EmbeddedBroker;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Outputs.NetcdfFormat;

/**
 * Tests the {@link EvaluationStatusTracker}.
 * @author James Brown
 */

class EvaluationStatusTrackerTest
{
    /**
     * Embedded broker.
     */

    private static EmbeddedBroker broker = null;

    /**
     * Broker connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    @BeforeAll
    static void runBeforeAllTests()
    {
        // Create and start and embedded broker
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        EvaluationStatusTrackerTest.broker = EmbeddedBroker.of( properties, true );
        EvaluationStatusTrackerTest.broker.start();

        // Create a connection factory to supply broker connections
        EvaluationStatusTrackerTest.connections = BrokerConnectionFactory.of( properties, 2 );
    }

    @Test
    void testNegotiationWithTwoCompetingSubscribersAndOneBestSubscriber() throws IOException, InterruptedException
    {
        // Use an evaluation instance as a publisher and a separate status tracker as a receiver in order to test the
        // status tracker. Mocking could be used, but this is cleaner. It creates two status trackers, but only 
        // one is tested/asserted against.
        // The evaluation will fail, expectedly, and this behavior is not part of the test.
        // A fake consumer for a fake evaluation subscriber.
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

        // Approve any offer
        SubscriberApprover subscriberApprover = new SubscriberApprover.Builder().build();

        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored = EvaluationSubscriber.of( consumer,
                                                                         Executors.newSingleThreadExecutor(),
                                                                         EvaluationStatusTrackerTest.connections );
              Evaluation evaluation =
                      Evaluation.of( wres.statistics.generated.Evaluation.newBuilder()
                                                                         .setOutputs( Outputs.newBuilder()
                                                                                             .setNetcdf( NetcdfFormat.getDefaultInstance() ) )
                                                                         .build(),
                                     EvaluationStatusTrackerTest.connections,
                                     "aClient" );
              EvaluationStatusTracker tracker = new EvaluationStatusTracker( evaluation,
                                                                             EvaluationStatusTrackerTest.connections,
                                                                             Set.of( Format.PNG, Format.CSV ),
                                                                             0,
                                                                             subscriberApprover,
                                                                             new ProducerFlowController( evaluation ) ) )
        {
            // A less-good ignored: delivers only one of the required formats
            Consumer consumerOne = Consumer.newBuilder()
                                           .setConsumerId( "aConsumer" )
                                           .addFormats( Format.PNG )
                                           .build();

            // The best ignored: two formats, both required
            Consumer consumerTwo = Consumer.newBuilder()
                                           .setConsumerId( "anotherConsumer" )
                                           .addFormats( Format.PNG )
                                           .addFormats( Format.CSV )
                                           .build();

            EvaluationStatus statusOne = EvaluationStatus.newBuilder()
                                                         .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                         .setConsumer( consumerOne )
                                                         .build();

            EvaluationStatus statusTwo = EvaluationStatus.newBuilder()
                                                         .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                         .setConsumer( consumerTwo )
                                                         .build();

            evaluation.publish( statusOne );
            evaluation.publish( statusTwo );

            tracker.awaitNegotiatedSubscribers();

            Map<Format, String> actual = tracker.getNegotiatedSubscribers();
            Map<Format, String> expected = new EnumMap<>( Format.class );

            expected.put( Format.CSV, "anotherConsumer" );
            expected.put( Format.PNG, "anotherConsumer" );

            assertEquals( expected, actual );

            // Forcibly stop the evaluation
            evaluation.stop( null );
        }
    }

    @Test
    void testNegotiationWithThreeCompetingSubscribers() throws IOException, InterruptedException
    {
        // Use an evaluation instance as a publisher and a separate status tracker as a receiver in order to test the
        // status tracker. Mocking could be used, but this is cleaner. It creates two status trackers, but only 
        // one is tested/asserted against.
        // The evaluation will fail, expectedly, and this behavior is not part of the test.
        // A fake consumer for a fake evaluation subscriber.
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

        // Approve any offer
        SubscriberApprover subscriberApprover = new SubscriberApprover.Builder().build();

        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored = EvaluationSubscriber.of( consumer,
                                                                         Executors.newSingleThreadExecutor(),
                                                                         EvaluationStatusTrackerTest.connections );
              Evaluation evaluation =
                      Evaluation.of( wres.statistics.generated.Evaluation.newBuilder()
                                                                         .setOutputs( Outputs.newBuilder()
                                                                                             .setNetcdf( NetcdfFormat.getDefaultInstance() ) )
                                                                         .build(),
                                     EvaluationStatusTrackerTest.connections,
                                     "aClient" );
              EvaluationStatusTracker tracker = new EvaluationStatusTracker( evaluation,
                                                                             EvaluationStatusTrackerTest.connections,
                                                                             Set.of( Format.PNG ),
                                                                             0,
                                                                             subscriberApprover,
                                                                             new ProducerFlowController( evaluation ) ) )
        {
            Consumer consumerOne = Consumer.newBuilder()
                                           .setConsumerId( "aConsumer" )
                                           .addFormats( Format.PNG )
                                           .build();

            Consumer consumerTwo = Consumer.newBuilder()
                                           .setConsumerId( "anotherConsumer" )
                                           .addFormats( Format.PNG )
                                           .build();

            Consumer consumerThree = Consumer.newBuilder()
                                             .setConsumerId( "yetAnotherConsumer" )
                                             .addFormats( Format.PNG )
                                             .build();

            EvaluationStatus statusOne = EvaluationStatus.newBuilder()
                                                         .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                         .setConsumer( consumerOne )
                                                         .build();

            EvaluationStatus statusTwo = EvaluationStatus.newBuilder()
                                                         .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                         .setConsumer( consumerTwo )
                                                         .build();
            EvaluationStatus statusThree = EvaluationStatus.newBuilder()
                                                           .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                           .setConsumer( consumerThree )
                                                           .build();

            evaluation.publish( statusOne );
            evaluation.publish( statusTwo );
            evaluation.publish( statusThree );

            tracker.awaitNegotiatedSubscribers();

            Map<Format, String> actual = tracker.getNegotiatedSubscribers();

            assertTrue( actual.containsKey( Format.PNG ) );

            String actualSubscriber = actual.get( Format.PNG );

            // Should be one of the three options, which are all equally good
            assertTrue( actualSubscriber.equals( "aConsumer" ) || actualSubscriber.equals( "anotherConsumer" )
                        || actualSubscriber.equals( "yetAnotherConsumer" ) );

            // Forcibly stop the evaluation
            evaluation.stop( null );
        }
    }

    @Test
    void testNegotiationWithThreeCompletingSubscribersAndTwoApprovedSubscribers()
            throws IOException, InterruptedException
    {
        // Use an evaluation instance as a publisher and a separate status tracker as a receiver in order to test the
        // status tracker. Mocking could be used, but this is cleaner. It creates two status trackers, but only 
        // one is tested/asserted against.
        // The evaluation will fail, expectedly, and this behavior is not part of the test.
        // A fake consumer for a fake evaluation subscriber.
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

        // Approve only two of the three offers
        SubscriberApprover subscriberApprover = new SubscriberApprover.Builder()
                                                                                .addApprovedSubscriber( Format.PNG,
                                                                                                        "aConsumer" )
                                                                                .addApprovedSubscriber( Format.PNG,
                                                                                                        "anotherConsumer" )
                                                                                .build();

        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored = EvaluationSubscriber.of( consumer,
                                                                         Executors.newSingleThreadExecutor(),
                                                                         EvaluationStatusTrackerTest.connections );
              Evaluation evaluation =
                      Evaluation.of( wres.statistics.generated.Evaluation.newBuilder()
                                                                         .setOutputs( Outputs.newBuilder()
                                                                                             .setNetcdf( NetcdfFormat.getDefaultInstance() ) )
                                                                         .build(),
                                     EvaluationStatusTrackerTest.connections,
                                     "aClient" );
              EvaluationStatusTracker tracker = new EvaluationStatusTracker( evaluation,
                                                                             EvaluationStatusTrackerTest.connections,
                                                                             Set.of( Format.PNG ),
                                                                             0,
                                                                             subscriberApprover,
                                                                             new ProducerFlowController( evaluation ) ) )
        {
            Consumer consumerOne = Consumer.newBuilder()
                                           .setConsumerId( "aConsumer" )
                                           .addFormats( Format.PNG )
                                           .build();

            Consumer consumerTwo = Consumer.newBuilder()
                                           .setConsumerId( "anotherConsumer" )
                                           .addFormats( Format.PNG )
                                           .build();

            Consumer consumerThree = Consumer.newBuilder()
                                             .setConsumerId( "yetAnotherConsumer" )
                                             .addFormats( Format.PNG )
                                             .build();

            EvaluationStatus statusOne = EvaluationStatus.newBuilder()
                                                         .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                         .setConsumer( consumerOne )
                                                         .build();

            EvaluationStatus statusTwo = EvaluationStatus.newBuilder()
                                                         .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                         .setConsumer( consumerTwo )
                                                         .build();
            EvaluationStatus statusThree = EvaluationStatus.newBuilder()
                                                           .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                           .setConsumer( consumerThree )
                                                           .build();

            evaluation.publish( statusOne );
            evaluation.publish( statusTwo );

            // This one is not pre-approved
            evaluation.publish( statusThree );

            tracker.awaitNegotiatedSubscribers();

            Map<Format, String> actual = tracker.getNegotiatedSubscribers();

            assertTrue( actual.containsKey( Format.PNG ) );

            String actualSubscriber = actual.get( Format.PNG );

            // Should be one of the two options, which are both equally good
            assertTrue( actualSubscriber.equals( "aConsumer" ) || actualSubscriber.equals( "anotherConsumer" ) );

            // Forcibly stop the evaluation
            evaluation.stop( null );
        }
    }

    @AfterAll
    static void runAfterAllTests() throws IOException
    {
        if ( Objects.nonNull( EvaluationStatusTrackerTest.connections ) )
        {
            EvaluationStatusTrackerTest.connections.close();
        }

        if ( Objects.nonNull( EvaluationStatusTrackerTest.broker ) )
        {
            EvaluationStatusTrackerTest.broker.close();
        }
    }

}
