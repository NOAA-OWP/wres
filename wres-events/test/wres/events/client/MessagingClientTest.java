package wres.events.client;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.events.EvaluationMessager;
import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.StatisticsConsumer;
import wres.eventsbroker.embedded.EmbeddedBroker;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Statistics;

/**
 * Tests the {@link MessagingClient}.
 *
 * @author James Brown
 */

class MessagingClientTest
{
    /** Embedded broker. */
    private static EmbeddedBroker broker = null;

    /** Broker connection factory. */
    private static BrokerConnectionFactory connections = null;

    @BeforeAll
    static void runBeforeAllTests()
    {
        // Create and start and embedded broker
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        MessagingClientTest.broker = EmbeddedBroker.of( properties, true );
        MessagingClientTest.broker.start();

        // Create a connection factory to supply broker connections
        MessagingClientTest.connections = BrokerConnectionFactory.of( properties, 2 );
    }

    @Test
    void publishAndConsumeOneEvaluationWithAnExternalSubscriber() throws IOException
    {
        // Create some state written by the consumers that can be asserted against
        Set<Integer> actualIntegersWritten = new HashSet<>();

        // A factory that creates consumers on demand
        ConsumerFactory consumerFactory = new ConsumerFactory()
        {
            @Override
            public StatisticsConsumer getConsumer( Evaluation evaluation, Path path )
            {
                actualIntegersWritten.add( 1 );
                return StatisticsConsumer.getResourceFreeConsumer( a -> Set.of() );
            }

            @Override
            public StatisticsConsumer getGroupedConsumer( Evaluation evaluation, Path path )
            {
                actualIntegersWritten.add( 2 );
                return StatisticsConsumer.getResourceFreeConsumer( a -> Set.of() );
            }

            @Override
            public Consumer getConsumerDescription()
            {
                return Consumer.newBuilder()
                               .addFormats( Consumer.Format.PNG )
                               .setConsumerId( "bar" )
                               .build();
            }
        };

        MessagingClient client = MessagingClient.of( MessagingClientTest.connections, consumerFactory );

        // Start the client
        client.start();

        Evaluation evaluationMessage = Evaluation.newBuilder()
                                                 .setOutputs( Outputs.newBuilder()
                                                                     .setPng( Outputs.PngFormat.getDefaultInstance() ) )
                                                 .build();

        try ( EvaluationMessager evaluation = EvaluationMessager.of( evaluationMessage,
                                                                     MessagingClientTest.connections,
                                                                     "aClient" ) )
        {
            // Start the evaluation
            evaluation.start();

            Statistics statistics = Statistics.getDefaultInstance();

            // Publish the statistics without a group id
            evaluation.publish( statistics );

            // Publish the statistics with a group id
            evaluation.publish( statistics, "foo" );

            // Mark publication complete, which implicitly marks all groups complete
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();
        }
        finally
        {
            client.stop();
        }

        assertEquals( Set.of( 1, 2 ), actualIntegersWritten );
    }

    @AfterAll
    static void runAfterAllTests() throws IOException
    {
        if ( Objects.nonNull( MessagingClientTest.broker ) )
        {
            MessagingClientTest.broker.close();
        }
    }

}
