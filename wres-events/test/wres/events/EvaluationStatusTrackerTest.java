package wres.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;

public class EvaluationStatusTrackerTest
{
    /**
     * Connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    @BeforeClass
    public static void runBeforeAllTests()
    {
        EvaluationStatusTrackerTest.connections = BrokerConnectionFactory.of();
    }

    @Test
    public void testNegotiationWithCompetingSubscribersAndOneBestSubscriber() throws IOException, InterruptedException
    {

        // Use an evaluation instance as a publisher and a separate status tracker as a receiver in order to test the
        // status tracker. Power-mocking could be used, but this is cleaner. It creates two status trackers, but only 
        // one is tested/asserted against.
        // The evaluation will fail, expectedly, and this behavior is not part of the test.
        try ( Evaluation evaluation = Evaluation.of( wres.statistics.generated.Evaluation.getDefaultInstance(),
                                                     EvaluationStatusTrackerTest.connections );
              EvaluationStatusTracker tracker = new EvaluationStatusTracker( evaluation,
                                                                             EvaluationStatusTrackerTest.connections,
                                                                             Set.of( Format.PNG, Format.CSV ),
                                                                             "anIdentifier",
                                                                             0 ) )
        {
            // The best subscriber: two formats, both required 
            Consumer consumerOne = Consumer.newBuilder()
                                           .setConsumerId( "aConsumer" )
                                           .addFormats( Format.PNG )
                                           .addFormats( Format.CSV )
                                           .build();

            // A less-good subscriber: delivers only one of the required formats
            Consumer consumerTwo = Consumer.newBuilder()
                                           .setConsumerId( "anotherConsumer" )
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

            evaluation.publish( statusOne );
            evaluation.publish( statusTwo );

            tracker.awaitNegotiatedSubscribers();
            
            Map<Format,String> actual = tracker.getNegotiatedSubscribers();
            Map<Format,String> expected = new EnumMap<>( Format.class );
            
            expected.put( Format.CSV, "aConsumer");
            expected.put( Format.PNG, "aConsumer");
            
            assertEquals( expected, actual );
        }
    }

    @Test
    public void testNegotiationWithCompetingSubscribersAndThreeEqualSubscribers()
            throws IOException, InterruptedException
    {
        try ( Evaluation evaluation = Evaluation.of( wres.statistics.generated.Evaluation.getDefaultInstance(),
                                                     EvaluationStatusTrackerTest.connections );
              EvaluationStatusTracker tracker = new EvaluationStatusTracker( evaluation,
                                                                             EvaluationStatusTrackerTest.connections,
                                                                             Set.of( Format.PNG ),
                                                                             "anIdentifier",
                                                                             0 ) )
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
        }


    }
}
