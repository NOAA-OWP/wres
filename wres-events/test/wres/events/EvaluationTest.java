package wres.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.EvaluationStatus;
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
     * Nominal formats negotiated by consumers.
     */

    private final Format[] formats = new Format[] { Format.CSV, Format.PNG };

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
        wres.statistics.generated.Evaluation.Builder oneEvaluationBuilder =
                wres.statistics.generated.Evaluation.newBuilder();

        this.oneEvaluation = oneEvaluationBuilder.build();
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
        wres.statistics.generated.Evaluation.Builder anotherEvaluationBuilder =
                wres.statistics.generated.Evaluation.newBuilder();

        this.anotherStatistics = new ArrayList<>();
        this.anotherEvaluation = anotherEvaluationBuilder.build();

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
        // Create the consumers upfront
        // Consumers simply dump to an actual output store for comparison with the expected output
        List<wres.statistics.generated.Evaluation> actualEvaluations = new ArrayList<>(); // Common store
        List<EvaluationStatus> actualStatuses = new ArrayList<>();
        List<Statistics> actualStatistics = new ArrayList<>();
        List<EvaluationStatus> otherActualStatuses = new ArrayList<>();
        List<Statistics> otherActualStatistics = new ArrayList<>();

        // Consumers, three for each evaluation
        Consumer<EvaluationStatus> status = actualStatuses::add;
        Consumer<wres.statistics.generated.Evaluation> description = actualEvaluations::add;
        Consumer<Statistics> statistics = actualStatistics::add;

        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( status )
                                       .addEvaluationConsumer( description )
                                       .addStatisticsConsumer( statistics, this.formats )
                                       .build();

        Consumer<wres.statistics.generated.Evaluation> otherDescription = actualEvaluations::add; // Common store
        Consumer<EvaluationStatus> otherStatus = otherActualStatuses::add;
        Consumer<Statistics> otherStatistics = otherActualStatistics::add;

        Consumers otherConsumerGroup =
                new Consumers.Builder().addStatusConsumer( otherStatus )
                                       .addEvaluationConsumer( otherDescription )
                                       .addStatisticsConsumer( otherStatistics, this.formats )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluationOne =
                Evaluation.of( this.oneEvaluation,
                               EvaluationTest.connections,
                               consumerGroup );
              Evaluation evaluationTwo =
                      Evaluation.of( this.anotherEvaluation,
                                     EvaluationTest.connections,
                                     otherConsumerGroup ) )
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

        List<wres.statistics.generated.Evaluation> expectedEvaluations =
                List.of( this.oneEvaluation, this.anotherEvaluation );

        assertEquals( expectedEvaluations, actualEvaluations );

        assertEquals( this.oneStatistics, actualStatistics );
        assertEquals( this.anotherStatistics, otherActualStatistics );

        // For status messages, assert number only
        assertEquals( 12, actualStatuses.size() );
        assertEquals( 7, otherActualStatuses.size() );
    }

    @Test
    public void publishAndConsumeOneEvaluationWithTwoStatusConsumers()
            throws IOException, NamingException, JMSException, InterruptedException
    {
        // Create the consumers upfront
        // Consumers simply dump to an actual output store for comparison with the expected output
        List<wres.statistics.generated.Evaluation> actualEvaluations = new ArrayList<>(); // Common store
        List<EvaluationStatus> actualStatuses = new ArrayList<>();
        List<Statistics> actualStatistics = new ArrayList<>();

        // Consumers, three for each evaluation
        Consumer<EvaluationStatus> status = actualStatuses::add;
        Consumer<wres.statistics.generated.Evaluation> description = actualEvaluations::add;
        Consumer<Statistics> statistics = actualStatistics::add;

        // Add the same status consumer twice, which doubles the expected number of messages to 24
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( status )
                                       .addStatusConsumer( status )
                                       .addEvaluationConsumer( description )
                                       .addStatisticsConsumer( statistics, this.formats )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluationOne =
                Evaluation.of( this.oneEvaluation,
                               EvaluationTest.connections,
                               consumerGroup ); )
        {
            // First evaluation
            for ( Statistics next : this.oneStatistics )
            {
                evaluationOne.publish( next );
            }

            // Success
            evaluationOne.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluationOne.await();
        }

        List<wres.statistics.generated.Evaluation> expectedEvaluations =
                List.of( this.oneEvaluation );

        assertEquals( expectedEvaluations, actualEvaluations );
        assertEquals( this.oneStatistics, actualStatistics );

        // For status messages, assert number only
        assertEquals( 24, actualStatuses.size() );
    }

    @Test
    public void testPublishThrowsExceptionAfterStop() throws IOException
    {
        // Add the same status consumer twice, which doubles the expected number of messages to 24
        Consumers consumerGroup =
                new Consumers.Builder()
                                       .addStatusConsumer( message -> {
                                       } )
                                       .addEvaluationConsumer( message -> {
                                       } )
                                       .addStatisticsConsumer( message -> {
                                       }, this.formats )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        Evaluation evaluationOne = Evaluation.of( this.oneEvaluation,
                                                  EvaluationTest.connections,
                                                  consumerGroup );

        // Stop the evaluation
        evaluationOne.stop( new Exception( "an exception" ) );

        EvaluationStatus message = EvaluationStatus.getDefaultInstance();
        assertThrows( IllegalStateException.class, () -> evaluationOne.publish( message ) );
    }

    @Test
    @Ignore( "Performance testing only. Not to be exposed. Remove @ignore locally, as needed." )
    public void testLargeEvaluation()
            throws IOException, NamingException, JMSException, InterruptedException
    {
        Instant then = Instant.now();

        // Create the consumers upfront
        // Consumers simply dump to an actual output store for comparison with the expected output
        List<wres.statistics.generated.Evaluation> actualEvaluations = new ArrayList<>(); // Common store
        List<EvaluationStatus> actualStatuses = new ArrayList<>();

        // Statistics incremented as the pipeline progresses
        List<Statistics> actualStatistics = new ArrayList<>();

        // End-of-pipeline statistics
        List<Statistics> actualAggregatedStatistics = new ArrayList<>();

        // Consumers for the incremental messages
        Consumer<EvaluationStatus> status = actualStatuses::add;
        Consumer<wres.statistics.generated.Evaluation> description = actualEvaluations::add;
        Consumer<Collection<Statistics>> aggregatedStatistics =
                aList -> actualAggregatedStatistics.add( EvaluationTest.getStatisticsAggregator()
                                                                       .apply( aList ) );
        Consumer<Statistics> statistics = actualStatistics::add;

        int featureCount = 10000;

        // Create a container for all the consumers
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( status )
                                       .addEvaluationConsumer( description )
                                       .addStatisticsConsumer( statistics, this.formats )
                                       .addGroupedStatisticsConsumer( aggregatedStatistics )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluation = Evaluation.of( this.oneEvaluation,
                                                     EvaluationTest.connections,
                                                     consumerGroup ); )
        {
            // Iterate the groups/features
            for ( int i = 0; i < featureCount; i++ )
            {
                // Publish the group/feature
                for ( Statistics next : this.oneStatistics )
                {
                    evaluation.publish( next, "group_" + i );
                }

                // Mark the group complete
                evaluation.markGroupPublicationCompleteReportedSuccess( "group_" + i );
            }

            // Success
            evaluation.markPublicationCompleteReportedSuccess();

            // Wait for the evaluation to complete
            evaluation.await();
        }

        assertEquals( featureCount, actualAggregatedStatistics.size() );
        assertEquals( featureCount * this.oneStatistics.size(), actualStatistics.size() );

        Instant now = Instant.now();

        System.out.println();
        System.out.println( "Time elapsed for messaging an evaluation composed of " + featureCount
                            + " features, each with "
                            + this.oneStatistics.size()
                            + " pools (and asserting the correct number of consumed statistics): "
                            + Duration.between( then, now ) );
    }

    @Test
    public void
            publishAndConsumeOneEvaluationWithTwoGroupsAndOneConsumerForEachGroupAndOneOverallConsumerAndOnePairsConsumer()
                    throws IOException, NamingException, JMSException, InterruptedException
    {
        // Create the consumers upfront
        // Consumers simply dump to an actual output store for comparison with the expected output
        List<wres.statistics.generated.Evaluation> actualEvaluations = new ArrayList<>(); // Common store
        List<EvaluationStatus> actualStatuses = new ArrayList<>();

        // Statistics incremented as the pipeline progresses
        List<Statistics> actualStatistics = new ArrayList<>();

        // End-of-pipeline statistics
        List<Statistics> actualAggregatedStatistics = new ArrayList<>();

        // Pairs
        List<Pairs> actualPairs = new ArrayList<>();

        // Consumers for the incremental messages
        Consumer<EvaluationStatus> status = actualStatuses::add;
        Consumer<wres.statistics.generated.Evaluation> description = actualEvaluations::add;
        Consumer<Statistics> statistics = actualStatistics::add;
        Consumer<Pairs> pairs = actualPairs::add;

        // Consumers for the end-of-pipeline/grouped statistics
        Consumer<Collection<Statistics>> aggregatedStatistics =
                aList -> actualAggregatedStatistics.add( EvaluationTest.getStatisticsAggregator()
                                                                       .apply( aList ) );

        // Create a container for all the consumers
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( status )
                                       .addEvaluationConsumer( description )
                                       .addStatisticsConsumer( statistics, this.formats )
                                       .addGroupedStatisticsConsumer( aggregatedStatistics, this.formats )
                                       .addPairsConsumer( pairs )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluation = Evaluation.of( this.oneEvaluation,
                                                     EvaluationTest.connections,
                                                     consumerGroup ); )
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
        List<wres.statistics.generated.Evaluation> expectedEvaluations =
                List.of( this.oneEvaluation );
        assertEquals( expectedEvaluations, actualEvaluations );

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

        // For status messages, assert number only: 15 statistics, 1 evaluation start and 1 evaluation end, 1 message 
        // for each 2 group completed and one for each of five consumers starting = 24
        assertEquals( 19, actualStatuses.size() );

        List<Pairs> expectedPairs = List.of( this.somePairs, this.somePairs );
        assertEquals( expectedPairs, actualPairs );
    }

    @Test
    public void testEmptyEvaluation() throws IOException
    {
        // Create the consumers
        // Create a container for all the consumers
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( Function.identity()::apply )
                                       .addEvaluationConsumer( Function.identity()::apply )
                                       .addStatisticsConsumer( Function.identity()::apply, this.formats )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        Evaluation evaluation = null;
        Integer exitCode = null;
        try
        {
            evaluation = Evaluation.of( this.oneEvaluation,
                                        EvaluationTest.connections,
                                        consumerGroup );

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
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( Function.identity()::apply )
                                       .addEvaluationConsumer( Function.identity()::apply )
                                       .addStatisticsConsumer( consume -> {
                                           throw new ConsumerException( "Consumption failed!" );
                                       }, this.formats )
                                       .build();

        // Open an evaluation, closing on completion
        Evaluation evaluation = null;
        EvaluationFailedToCompleteException actualException = null;
        try
        {
            evaluation = Evaluation.of( this.oneEvaluation,
                                        EvaluationTest.connections,
                                        consumerGroup );

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
        // Create a statistics consumer that fails one time only, together with some no-op consumers for other types
        AtomicInteger failureCount = new AtomicInteger();
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( Function.identity()::apply )
                                       .addEvaluationConsumer( Function.identity()::apply )
                                       .addStatisticsConsumer( statistics -> {
                                           if ( failureCount.getAndIncrement() < 1 )
                                           {
                                               throw new ConsumerException( "Consumption failed!" );
                                           }
                                       }, this.formats )
                                       .build();

        // Open an evaluation, closing on completion
        try ( Evaluation evaluation = Evaluation.of( this.oneEvaluation,
                                                     EvaluationTest.connections,
                                                     consumerGroup ) )
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
        // Create the consumers
        // Create a container for all the consumers
        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( Function.identity()::apply )
                                       .addEvaluationConsumer( Function.identity()::apply )
                                       .addStatisticsConsumer( Function.identity()::apply, this.formats )
                                       .build();

        // Open an evaluation, closing on completion
        Evaluation evaluation = null;
        AtomicInteger exitCode = new AtomicInteger();
        try
        {
            evaluation = Evaluation.of( this.oneEvaluation,
                                        EvaluationTest.connections,
                                        consumerGroup );

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
            sortedStatistics.sort( ( a,
                                     b ) -> Double.compare( a.getScoresList()
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
