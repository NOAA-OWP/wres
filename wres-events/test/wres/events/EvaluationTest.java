package wres.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import wres.events.subscribe.ConsumerException;
import wres.events.subscribe.ConsumerFactory;
import wres.events.subscribe.EvaluationSubscriber;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Pairs.Pair;
import wres.statistics.generated.Pairs.TimeSeriesOfPairs;
import wres.statistics.generated.Pairs;

/**
 * Tests the {@link Evaluation}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EvaluationTest
{

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

    /**
     * Connection factory.
     */

    private static BrokerConnectionFactory connections = null;

    @BeforeClass
    public static void runBeforeAllTests()
    {
        EvaluationTest.connections = BrokerConnectionFactory.of();
    }

    @Before
    public void runBeforeEachTest()
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
    public void publishAndConsumeTwoEvaluationsSimultaneously()
            throws IOException, NamingException, JMSException, InterruptedException
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
        };

        // Create and start a broker and open an evaluation, closing on completion
        try ( EvaluationSubscriber subscriberOne =
                EvaluationSubscriber.of( consumer,
                                         Executors.newSingleThreadExecutor(),
                                         EvaluationTest.connections );
              Evaluation evaluationOne =
                      Evaluation.of( this.oneEvaluation,
                                     EvaluationTest.connections,
                                     "evaluationOne" );
              Evaluation evaluationTwo =
                      Evaluation.of( this.anotherEvaluation,
                                     EvaluationTest.connections,
                                     "evaluationTwo" ) )
        {
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

            // Wait for the evaluation to complete
            evaluationOne.await();
            evaluationTwo.await();
        }

        assertEquals( this.oneStatistics, actualStatistics );
        assertEquals( this.anotherStatistics, otherActualStatistics );
    }


    @Test
    public void testPublishThrowsExceptionAfterStop() throws IOException
    {
        // Create and start a broker and open an evaluation, closing on completion
        Evaluation evaluationOne = Evaluation.of( wres.statistics.generated.Evaluation.getDefaultInstance(),
                                                  EvaluationTest.connections );

        // Stop the evaluation
        evaluationOne.stop( new Exception( "an exception" ) );

        EvaluationStatus message = EvaluationStatus.getDefaultInstance();
        assertThrows( IllegalStateException.class, () -> evaluationOne.publish( message ) );
    }

    @Test
    public void publishAndConsumeOneEvaluationWithTwoGroupsAndOneConsumerForEachGroupAndOneOverallConsumer()
            throws IOException, NamingException, JMSException, InterruptedException
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
                    actualAggregatedStatistics.add( EvaluationTest.getStatisticsAggregator()
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
        };

        // Create and start a broker and open an evaluation, closing on completion
        try ( EvaluationSubscriber subscriber =
                EvaluationSubscriber.of( consumer,
                                         Executors.newSingleThreadExecutor(),
                                         EvaluationTest.connections );
              Evaluation evaluation = Evaluation.of( this.oneEvaluation,
                                                     EvaluationTest.connections ); )
        {
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
        this.oneStatistics.forEach( next -> expectedOneBuilder.mergeFrom( next ) );

        Statistics.Builder expectedTwoBuilder = Statistics.newBuilder();
        this.anotherStatistics.forEach( next -> expectedTwoBuilder.mergeFrom( next ) );

        List<Statistics> expectedAggregatedStatistics = List.of( expectedOneBuilder.build(),
                                                                 expectedTwoBuilder.build() );

        assertEquals( expectedAggregatedStatistics, actualAggregatedStatistics );
    }

    @Test
    public void testEmptyEvaluation() throws IOException
    {
        // Create and start a broker and open an evaluation, closing on completion
        Evaluation evaluation = null;
        Integer exitCode = null;
        try
        {
            evaluation = Evaluation.of( wres.statistics.generated.Evaluation.getDefaultInstance(),
                                        EvaluationTest.connections );

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

        assertEquals( (Integer) 0, exitCode );
    }

    @Test
    public void testEvaluationWithUnrecoverableConsumerException() throws IOException
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
                    throw new ConsumerException( "Consumption failed!" );
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
        };

        // Open an evaluation, closing on completion
        Evaluation evaluation = null;
        EvaluationFailedToCompleteException actualException = null;
        try ( EvaluationSubscriber subscriberOne =
                EvaluationSubscriber.of( consumer,
                                         Executors.newSingleThreadExecutor(),
                                         EvaluationTest.connections ); )
        {
            evaluation = Evaluation.of( this.oneEvaluation,
                                        EvaluationTest.connections );

            // Publish a statistics message, which fails to be consumed after retries
            evaluation.publish( Statistics.getDefaultInstance() );

            // Notify publication done
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();
        }
        // All recovery attempts exhausted and now an exception is caught
        catch ( EvaluationFailedToCompleteException e )
        {
            // No clean-up to do here, just flag the expected exception
            actualException = e;
        }
        finally
        {
            // Close the evaluation
            if ( Objects.nonNull( evaluation ) )
            {
                evaluation.close();
            }
        }

        // Assert that the evaluation failed and that an expected exception was caught
        assertNotEquals( 0, evaluation.getExitCode() );
        assertNotNull( actualException );

        assertTrue( actualException.getMessage()
                                   .contains( "Failed to complete evaluation" ) );
    }

    @Test
    public void testEvaluationWithRecoverableConsumerException() throws IOException
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
                        throw new ConsumerException( "Consumption failed!" );
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
        };

        // Open an evaluation, closing on completion
        try ( EvaluationSubscriber subscriberOne =
                EvaluationSubscriber.of( consumer,
                                         Executors.newSingleThreadExecutor(),
                                         EvaluationTest.connections );
              Evaluation evaluation = Evaluation.of( this.oneEvaluation,
                                                     EvaluationTest.connections ) )
        {
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
    public void testEvaluationWithUnrecoverablePublisherException() throws IOException
    {
        // Open an evaluation, closing on completion
        Evaluation evaluation = null;
        AtomicInteger exitCode = new AtomicInteger();

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
        };

        try ( EvaluationSubscriber subscriberOne =
                EvaluationSubscriber.of( consumer,
                                         Executors.newSingleThreadExecutor(),
                                         EvaluationTest.connections ); )
        {
            evaluation = Evaluation.of( this.oneEvaluation,
                                        EvaluationTest.connections );

            Statistics mockedStatistics = Mockito.mock( Statistics.class );
            Mockito.when( mockedStatistics.toByteArray() )
                   .thenThrow( new IllegalArgumentException( "An exception." ) );

            // Publish one good message
            evaluation.publish( Statistics.getDefaultInstance() );

            // Publish one bad message
            evaluation.publish( mockedStatistics );

            // Notify publication done
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();
        }
        // Catch the exception thrown
        catch ( IllegalArgumentException e )
        {
            evaluation.stop( e );
            exitCode.set( evaluation.getExitCode() );
        }
        finally
        {
            // Close the evaluation
            if ( Objects.nonNull( evaluation ) )
            {
                evaluation.close();
            }
        }

        assertNotEquals( 0, exitCode.get() );
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
            sortedStatistics.sort( ( a, b ) -> Double.compare( a.getScoresList()
                                                                .get( 0 )
                                                                .getStatisticsList()
                                                                .get( 0 )
                                                                .getValue(),
                                                               b.getScoresList()
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

    @AfterClass
    public static void runAfterAllTests() throws IOException
    {
        EvaluationTest.connections.close();
    }

}
