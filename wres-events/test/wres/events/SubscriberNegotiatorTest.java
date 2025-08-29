package wres.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.statistics.generated.Consumer;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.events.subscribe.SubscriberApprover;
import wres.statistics.generated.Consumer.Format;

/**
 * Tests the {@link SubscriberNegotiator}.
 * 
 * @author James Brown
 */

class SubscriberNegotiatorTest
{
    // Fake evaluation to help with testing
    private EvaluationMessager fakeEvaluation;

    @BeforeEach
    void runBeforeEachTest()
    {
        // Create a fake evaluation for testing
        this.fakeEvaluation = Mockito.mock( EvaluationMessager.class );
        Mockito.when( this.fakeEvaluation.getEvaluationId() )
               .thenReturn( "aFakeEvaluation" );
    }

    @Test
    void testNegotiateSubscribersWithOneBestOffer() throws InterruptedException
    {
        // Negotiator of PNG and CSV without any restrictions on subscribers 
        SubscriberNegotiator negotiator = new SubscriberNegotiator( this.fakeEvaluation,
                                                                    Set.of( Format.PNG, Format.CSV2 ),
                                                                    new SubscriberApprover.Builder().build() );

        // Register the offers first because the negotiator is blocking and we are testing in a single thread.

        // Register an offer
        negotiator.registerAnOfferToDeliverFormats( EvaluationStatus.newBuilder()
                                                                    .setClientId( "aClient" )
                                                                    .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                                    .setConsumer( Consumer.newBuilder()
                                                                                          .setConsumerId( "aConsumer" )
                                                                                          .addFormats( Format.PNG ) )
                                                                    .build() );

        // Register yet another offer
        negotiator.registerAnOfferToDeliverFormats( EvaluationStatus.newBuilder()
                                                                    .setClientId( "yetAnotherClient" )
                                                                    .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                                    .setConsumer( Consumer.newBuilder()
                                                                                          .setConsumerId( "yetAnotherConsumer" )
                                                                                          .addFormats( Format.PNG )
                                                                                          .addFormats( Format.CSV2 ) )
                                                                    .build() );

        // Register one more offer
        negotiator.registerAnOfferToDeliverFormats( EvaluationStatus.newBuilder()
                                                                    .setClientId( "oneMoreClient" )
                                                                    .setCompletionStatus( CompletionStatus.READY_TO_CONSUME )
                                                                    .setConsumer( Consumer.newBuilder()
                                                                                          .setConsumerId( "oneMoreConsumer" )
                                                                                          .addFormats( Format.NETCDF ) )
                                                                    .build() );

        Map<Format, String> actual = negotiator.negotiateSubscribers();

        Map<Format, String> expected = new EnumMap<>( Format.class );
        expected.put( Format.PNG, "yetAnotherConsumer" );
        expected.put( Format.CSV2, "yetAnotherConsumer" );

        assertEquals( expected, actual );
    }

}
