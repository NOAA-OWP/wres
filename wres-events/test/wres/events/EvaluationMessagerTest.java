package wres.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.subscribe.ConsumerException;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.eventsbroker.embedded.EmbeddedBroker;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.NetcdfFormat;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Pairs.Pair;
import wres.statistics.generated.Pairs.TimeSeriesOfPairs;
import wres.statistics.generated.Pairs;

/**
 * Tests the {@link EvaluationMessager}.
 *
 * @author James Brown
 */

class EvaluationMessagerTest
{
    /**
     * Embedded broker.
     */

    private static EmbeddedBroker broker = null;

    /**
     * Broker connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    /**
     * Re-used string.
     */

    private static final String A_CLIENT = "aClient";

    /**
     * One evaluation for testing.
     */

    private wres.statistics.generated.Evaluation oneEvaluation;

    /**
     * Another evaluation for testing.
     */

    private wres.statistics.generated.Evaluation anotherEvaluation;

    /**
     * Collection of statistics for testing.
     */

    private List<Statistics> oneStatistics;

    /**
     * Another collection of statistics for testing.
     */

    private List<Statistics> anotherStatistics;

    /**
     * Some pairs to reuse.
     */

    private Pairs somePairs;

    @BeforeAll
    static void runBeforeAllTests()
    {
        // Create and start and embedded broker
        Properties properties = BrokerUtilities.getBrokerConnectionProperties( "eventbroker.properties" );
        EvaluationMessagerTest.broker = EmbeddedBroker.of( properties, true );
        EvaluationMessagerTest.broker.start();

        // Create a connection factory to supply broker connections
        EvaluationMessagerTest.connections = BrokerConnectionFactory.of( properties, 2 );
    }

    @BeforeEach
    void runBeforeEachTest()
    {
        // First evaluation
        this.oneEvaluation =
                wres.statistics.generated.Evaluation.newBuilder()
                                                    .setOutputs( Outputs.newBuilder()
                                                                        .setPng( PngFormat.getDefaultInstance() ) )
                                                    .build();

        this.oneStatistics = new ArrayList<>();

        // Add one score with an incrementing value across each of ten pools
        for ( int i = 0; i < 10; i++ )
        {
            Statistics.Builder statistics = Statistics.newBuilder();
            DoubleScoreStatisticComponent.Builder componentBuilder =
                    DoubleScoreStatisticComponent.newBuilder().setValue( i );
            DoubleScoreStatistic.Builder scoreBuilder =
                    DoubleScoreStatistic.newBuilder().addStatistics( componentBuilder );
            statistics.addScores( scoreBuilder );
            this.oneStatistics.add( statistics.build() );
        }

        // Second evaluation
        this.anotherStatistics = new ArrayList<>();
        this.anotherEvaluation =
                wres.statistics.generated.Evaluation.newBuilder()
                                                    .setOutputs( Outputs.newBuilder()
                                                                        .setPng( PngFormat.getDefaultInstance() ) )
                                                    .build();

        // Add one score with an incrementing value across each of five pools
        for ( int i = 10; i < 15; i++ )
        {
            Statistics.Builder statistics = Statistics.newBuilder();
            DoubleScoreStatisticComponent.Builder componentBuilder =
                    DoubleScoreStatisticComponent.newBuilder().setValue( i );
            DoubleScoreStatistic.Builder scoreBuilder =
                    DoubleScoreStatistic.newBuilder().addStatistics( componentBuilder );
            statistics.addScores( scoreBuilder );
            this.anotherStatistics.add( statistics.build() );
        }

        this.somePairs =
                Pairs.newBuilder()
                     .addTimeSeries( TimeSeriesOfPairs.newBuilder()
                                                      .addPairs( Pair.newBuilder()
                                                                     .addLeft( 7.0 )
                                                                     .addRight( 13.0 ) ) )
                     .build();
    }

    @Test
    void publishAndConsumeTwoEvaluationsSimultaneously() throws IOException
    {
        // Containers to hold the statistics
        List<Statistics> actualStatistics = new ArrayList<>();
        List<Statistics> otherActualStatistics = new ArrayList<>();

        // Consumer factory implementation that simply adds the statistics to the above containers
        ConsumerFactory consumer = new ConsumerFactory()
        {
            @Override
            public Function<Statistics, Set<Path>>
            getConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> {
                    if ( path.toString().contains( "evaluationOne" ) )
                    {
                        actualStatistics.add( statistics );
                    }
                    else
                    {
                        otherActualStatistics.add( statistics );
                    }

                    return Set.of();
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
                               .addFormats( Format.PNG )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        // Consumer factory implementation that simply adds the statistics to the above containers
        ConsumerFactory consumerTwo = new ConsumerFactory()
        {
            @Override
            public Function<Statistics, Set<Path>>
            getConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> {
                    if ( path.toString().contains( "evaluationOne" ) )
                    {
                        actualStatistics.add( statistics );
                    }
                    else
                    {
                        otherActualStatistics.add( statistics );
                    }

                    return Set.of();
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
                               .setConsumerId( "anotherConsumer" )
                               .addFormats( Format.PNG )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        // Create and start a broker and open an evaluation, closing on completion
        // Subscribers are pub-sub, so technically not referenced inband, aka "ignored", but are used out-of-band
        try ( EvaluationSubscriber ignoredOne =
                      EvaluationSubscriber.of( consumer,
                                               Executors.newSingleThreadExecutor(),
                                               EvaluationMessagerTest.connections );
              EvaluationSubscriber ignoredTwo =
                      EvaluationSubscriber.of( consumerTwo,
                                               Executors.newSingleThreadExecutor(),
                                               EvaluationMessagerTest.connections );
              EvaluationMessager evaluationOne =
                      EvaluationMessager.of( this.oneEvaluation,
                                             EvaluationMessagerTest.connections,
                                             A_CLIENT,
                                             "evaluationOne" );
              EvaluationMessager evaluationTwo =
                      EvaluationMessager.of( this.anotherEvaluation,
                                             EvaluationMessagerTest.connections,
                                             A_CLIENT,
                                             "evaluationTwo" ) )
        {
            // Start the subscribers
            ignoredOne.start();
            ignoredTwo.start();

            // Start the evaluations
            evaluationOne.start();
            evaluationTwo.start();

            // First evaluation
            for ( Statistics next : this.oneStatistics )
            {
                evaluationOne.publish( next );
            }

            // Success
            evaluationOne.markPublicationCompleteReportedSuccess();

            // Second evaluation
            for ( Statistics next : this.anotherStatistics )
            {
                evaluationTwo.publish( next );
            }

            // Success
            evaluationTwo.markPublicationCompleteReportedSuccess();

            // Wait for the evaluations to complete
            evaluationOne.await();
            evaluationTwo.await();
        }

        assertEquals( this.oneStatistics, actualStatistics );
        assertEquals( this.anotherStatistics, otherActualStatistics );
    }


    @Test
    void testPublishThrowsExceptionAfterStop() throws IOException
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

        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored = EvaluationSubscriber.of( consumer,
                                                                      Executors.newSingleThreadExecutor(),
                                                                      EvaluationMessagerTest.connections );
              EvaluationMessager evaluation =
                      EvaluationMessager.of( wres.statistics.generated.Evaluation.newBuilder()
                                                                                 .setOutputs( Outputs.newBuilder()
                                                                                             .setNetcdf( NetcdfFormat.getDefaultInstance() ) )
                                                                                 .build(),
                                             EvaluationMessagerTest.connections,
                                             A_CLIENT ) )
        {
            // Start the subscriber
            ignored.start();

            // Start the evaluation
            evaluation.start();

            // Stop the evaluation
            evaluation.stop( new Exception( "an exception" ) );

            EvaluationStatus message = EvaluationStatus.getDefaultInstance();
            assertThrows( IllegalStateException.class, () -> evaluation.publish( message ) );
        }
    }

    @Test
    void publishAndConsumeOneEvaluationWithTwoGroupsAndOneConsumerForEachGroupAndOneOverallConsumer()
            throws IOException
    {
        // Statistics incremented as the pipeline progresses
        List<Statistics> actualStatistics = new ArrayList<>();

        // End-of-pipeline statistics
        List<Statistics> actualAggregatedStatistics = new ArrayList<>();

        // Consumer factory implementation that simply adds the statistics to the above containers
        ConsumerFactory consumer = new ConsumerFactory()
        {
            @Override
            public Function<Statistics, Set<Path>>
            getConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> {
                    actualStatistics.add( statistics );
                    return Set.of();
                };
            }

            @Override
            public Function<Collection<Statistics>, Set<Path>>
            getGroupedConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statisticsMessages -> {
                    actualAggregatedStatistics.add( EvaluationMessagerTest.getStatisticsAggregator()
                                                                          .apply( statisticsMessages ) );
                    return Collections.emptySet();
                };
            }

            @Override
            public Consumer getConsumerDescription()
            {
                return Consumer.newBuilder()
                               .setConsumerId( "aConsumer" )
                               .addFormats( Format.PNG )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        // Create and start a broker and open an evaluation, closing on completion
        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored =
                      EvaluationSubscriber.of( consumer,
                                               Executors.newSingleThreadExecutor(),
                                               EvaluationMessagerTest.connections );
              EvaluationMessager evaluation = EvaluationMessager.of( this.oneEvaluation,
                                                                     EvaluationMessagerTest.connections,
                                                                     A_CLIENT ) )
        {
            // Start the subscriber
            ignored.start();

            // Start the evaluation
            evaluation.start();

            // First group
            for ( Statistics next : this.oneStatistics )
            {
                evaluation.publish( next, "groupOne" );
            }

            // Publish the pairs
            evaluation.publish( this.somePairs );

            // Mark the group complete
            evaluation.markGroupPublicationCompleteReportedSuccess( "groupOne" );

            // Second group
            for ( Statistics next : this.anotherStatistics )
            {
                evaluation.publish( next, "groupTwo" );
            }

            // Publish the pairs
            evaluation.publish( this.somePairs );

            // Mark the group complete
            evaluation.markGroupPublicationCompleteReportedSuccess( "groupTwo" );

            // Success
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();
        }

        // Make some assertions

        // Assertions about the disaggregated statistics
        List<Statistics> expectedWithoutGroups = new ArrayList<>( this.oneStatistics );
        expectedWithoutGroups.addAll( this.anotherStatistics );

        assertEquals( expectedWithoutGroups, actualStatistics );

        // Assertions about the aggregated statistics
        assertEquals( 2, actualAggregatedStatistics.size() );

        Statistics.Builder expectedOneBuilder = Statistics.newBuilder();
        this.oneStatistics.forEach( expectedOneBuilder::mergeFrom );

        Statistics.Builder expectedTwoBuilder = Statistics.newBuilder();
        this.anotherStatistics.forEach( expectedTwoBuilder::mergeFrom );

        List<Statistics> expectedAggregatedStatistics = List.of( expectedOneBuilder.build(),
                                                                 expectedTwoBuilder.build() );

        assertEquals( expectedAggregatedStatistics, actualAggregatedStatistics );
    }

    @Test
    void testEmptyEvaluation() throws IOException
    {
        // Create and start a broker and open an evaluation, closing on completion
        EvaluationMessager evaluation = null;
        Integer exitCode = null;

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

        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored = EvaluationSubscriber.of( consumer,
                                                                      Executors.newSingleThreadExecutor(),
                                                                      EvaluationMessagerTest.connections ) )
        {
            evaluation =
                    EvaluationMessager.of( wres.statistics.generated.Evaluation.newBuilder()
                                                                               .setOutputs( Outputs.newBuilder()
                                                                                           .setNetcdf( NetcdfFormat.getDefaultInstance() ) )
                                                                               .build(),
                                           EvaluationMessagerTest.connections,
                                           A_CLIENT );

            // Start the subscriber
            ignored.start();

            // Start the evaluation
            evaluation.start();

            // Notify publication done, even though nothing published, as this 
            // has the expected message count
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();
        }
        finally
        {
            // Close the evaluation
            if ( Objects.nonNull( evaluation ) )
            {
                evaluation.close();
                exitCode = evaluation.getExitCode();
            }
        }

        assertEquals( ( Integer ) 0, exitCode );
    }

    @Test
    void testEvaluationWithUnrecoverableConsumerException() throws IOException
    {
        // Create a statistics consumer that fails always, together with some no-op consumers for other message types
        // Consumer factory implementation that simply adds the statistics to the above containers
        ConsumerFactory consumer = new ConsumerFactory()
        {
            @Override
            public Function<Statistics, Set<Path>>
            getConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> {
                    throw new ConsumerException( "This is an expected consumption failure that tests error recovery!" );
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
                               .addFormats( Format.PNG )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        // Open an evaluation, closing on completion
        EvaluationMessager evaluation = null;
        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored =
                      EvaluationSubscriber.of( consumer,
                                               Executors.newSingleThreadExecutor(),
                                               EvaluationMessagerTest.connections ) )
        {
            // Start the subscriber
            ignored.start();

            evaluation = EvaluationMessager.of( this.oneEvaluation,
                                                EvaluationMessagerTest.connections,
                                                A_CLIENT );

            // Start the evaluation
            evaluation.start();

            // Publish a statistics message, which fails to be consumed after retries
            evaluation.publish( Statistics.getDefaultInstance() );

            // Notify publication done
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            EvaluationMessager finalEvaluation = evaluation;
            assertThrows( EvaluationFailedToCompleteException.class, finalEvaluation::await );
        }
        finally
        {
            // Close the evaluation
            if ( Objects.nonNull( evaluation ) )
            {
                evaluation.close();
            }
        }
    }

    @Test
    void testEvaluationWithRecoverableConsumerException() throws IOException
    {
        // Create a statistics consumer that fails always, together with some no-op consumers for other message types
        // Consumer factory implementation that simply adds the statistics to the above containers
        AtomicInteger failureCount = new AtomicInteger();
        ConsumerFactory consumer = new ConsumerFactory()
        {
            @Override
            public Function<Statistics, Set<Path>>
            getConsumer( wres.statistics.generated.Evaluation evaluation, Path path )
            {
                return statistics -> {
                    if ( failureCount.getAndIncrement() < 1 )
                    {
                        throw new ConsumerException( "This is an expected consumption failure that tests error "
                                                     + "recovery!" );
                    }

                    return Collections.emptySet();
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
                               .addFormats( Format.PNG )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        // Open an evaluation, closing on completion
        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored =
                      EvaluationSubscriber.of( consumer,
                                               Executors.newSingleThreadExecutor(),
                                               EvaluationMessagerTest.connections );
              EvaluationMessager evaluation = EvaluationMessager.of( this.oneEvaluation,
                                                                     EvaluationMessagerTest.connections,
                                                                     A_CLIENT ) )
        {
            // Start the subscriber
            ignored.start();

            // Start the evaluation
            evaluation.start();

            // Publish a statistics message, triggering one failed consumption followed by recovery
            evaluation.publish( Statistics.getDefaultInstance() );

            // Notify publication complete
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            int exitCode = evaluation.await();

            // Assert that the evaluation succeeded
            assertEquals( 0, exitCode );
        }
    }

    @Test
    void testEvaluationWithUnrecoverablePublisherException() throws IOException
    {
        // Open an evaluation, closing on completion
        EvaluationMessager evaluation = null;

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
                               .addFormats( Format.PNG )
                               .build();
            }

            @Override
            public void close()
            {
            }
        };

        // Subscriber is pub-sub, so technically not referenced inband, aka "ignored", but used out-of-band
        try ( EvaluationSubscriber ignored =
                      EvaluationSubscriber.of( consumer,
                                               Executors.newSingleThreadExecutor(),
                                               EvaluationMessagerTest.connections ) )
        {
            evaluation = EvaluationMessager.of( this.oneEvaluation,
                                                EvaluationMessagerTest.connections,
                                                A_CLIENT );

            // Start the subscriber
            ignored.start();

            // Start the evaluation
            evaluation.start();

            Statistics mockedStatistics = Mockito.mock( Statistics.class );
            Mockito.when( mockedStatistics.toByteArray() )
                   .thenThrow( new IllegalArgumentException( "An exception." ) );

            // Publish one good message
            evaluation.publish( Statistics.getDefaultInstance() );

            // Publish one bad message
            EvaluationMessager finalEvaluation = evaluation;
            assertThrows( IllegalArgumentException.class, () -> finalEvaluation.publish( mockedStatistics ) );

            // Notify publication done
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();
        }
        finally
        {
            // Close the evaluation
            if ( Objects.nonNull( evaluation ) )
            {
                evaluation.close();
            }
        }
    }

    /**
     * Helper that returns an aggregator for statistics messages, which collects and sorts according to the first 
     * score.
     *
     * @return a statistics aggregator
     */

    private static Function<Collection<Statistics>, Statistics> getStatisticsAggregator()
    {
        return statistics -> {

            // Build the aggregate statistics
            Statistics.Builder aggregate = Statistics.newBuilder();

            // Sort the statistics
            List<Statistics> sortedStatistics = new ArrayList<>( statistics );
            sortedStatistics.sort( Comparator.comparingDouble( a -> a.getScoresList()
                                                                     .get( 0 )
                                                                     .getStatisticsList()
                                                                     .get( 0 )
                                                                     .getValue() ) );

            // Merge the cached statistics
            for ( Statistics next : sortedStatistics )
            {
                aggregate.mergeFrom( next );
            }

            return aggregate.build();
        };
    }

    @AfterAll
    static void runAfterAllTests() throws IOException
    {
        if ( Objects.nonNull( EvaluationMessagerTest.broker ) )
        {
            EvaluationMessagerTest.broker.close();
        }
    }

}
