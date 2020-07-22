package wres.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
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
        wres.statistics.generated.Evaluation.Builder oneEvaluationBuilder =
                wres.statistics.generated.Evaluation.newBuilder();
        oneEvaluationBuilder.setPoolMessageCount( 10 );
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
        anotherEvaluationBuilder.setPoolMessageCount( 5 );
        this.anotherStatistics = new ArrayList<>();
        this.anotherEvaluation = anotherEvaluationBuilder.build();

        // Add one score with an incrementing value across each of ten pools
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
    @Ignore( "An out-of-band test for use with LongRunningSubscriber and a persistent broker. See #80267" )
    public void publishAndConsumeOneEvaluationWithAnExternalSubscriber()
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

        Consumers consumerGroup =
                new Consumers.Builder().addStatusConsumer( status )
                                       .addEvaluationConsumer( description )
                                       .addStatisticsConsumer( statistics )
                                       // Register an external subscriber with a unique identifier
                                       .addExternalSubscriber( "4mOgkGkse3gWIGKuIhzVnl5ZPCM" )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluationOne = Evaluation.open( this.oneEvaluation,
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
        }

        // Make assertions about the internal subscriptions
        assertEquals( List.of( this.oneEvaluation ), actualEvaluations );
        assertEquals( this.oneStatistics, actualStatistics );
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
                                       .addStatisticsConsumer( statistics )
                                       .build();

        Consumer<wres.statistics.generated.Evaluation> otherDescription = actualEvaluations::add; // Common store
        Consumer<EvaluationStatus> otherStatus = otherActualStatuses::add;
        Consumer<Statistics> otherStatistics = otherActualStatistics::add;

        Consumers otherConsumerGroup =
                new Consumers.Builder().addStatusConsumer( otherStatus )
                                       .addEvaluationConsumer( otherDescription )
                                       .addStatisticsConsumer( otherStatistics )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluationOne =
                Evaluation.open( this.oneEvaluation, EvaluationTest.connections, consumerGroup );
              Evaluation evaluationTwo =
                      Evaluation.open( this.anotherEvaluation, EvaluationTest.connections, otherConsumerGroup ) )
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
                                       .addStatisticsConsumer( statistics )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluationOne =
                Evaluation.open( this.oneEvaluation, EvaluationTest.connections, consumerGroup ); )
        {
            // First evaluation
            for ( Statistics next : this.oneStatistics )
            {
                evaluationOne.publish( next );
            }

            // Success
            evaluationOne.markPublicationCompleteReportedSuccess();
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
                                       } )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        Evaluation evaluationOne = Evaluation.open( this.oneEvaluation,
                                                    EvaluationTest.connections,
                                                    consumerGroup );

        // Stop the evaluation
        evaluationOne.stopOnException( new Exception( "an exception" ) );

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
                                       .addStatisticsConsumer( statistics )
                                       .addGroupedStatisticsConsumer( aggregatedStatistics )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluation = Evaluation.open( this.oneEvaluation,
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

                // Flag completion of group one
                EvaluationStatus groupDone =
                        EvaluationStatus.newBuilder()
                                        .setCompletionStatus( CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
                                        .setMessageCount( 10 )
                                        .build();

                evaluation.publish( groupDone, "group_" + i );
            }

            // Success
            evaluation.markPublicationCompleteReportedSuccess();
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
                                       .addStatisticsConsumer( statistics )
                                       .addGroupedStatisticsConsumer( aggregatedStatistics )
                                       .addPairsConsumer( pairs )
                                       .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( Evaluation evaluation = Evaluation.open( this.oneEvaluation,
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

            // Flag completion of group one
            EvaluationStatus groupOneDone =
                    EvaluationStatus.newBuilder()
                                    .setCompletionStatus( CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
                                    .setMessageCount( 10 )
                                    .build();

            evaluation.publish( groupOneDone, "groupOne" );

            // Second group
            for ( Statistics next : this.anotherStatistics )
            {
                evaluation.publish( next, "groupTwo" );
            }

            // Publish the pairs
            evaluation.publish( this.somePairs );

            // Flag completion of group two
            EvaluationStatus groupTwoDone =
                    EvaluationStatus.newBuilder()
                                    .setCompletionStatus( CompletionStatus.GROUP_COMPLETE_REPORTED_SUCCESS )
                                    .setMessageCount( 5 )
                                    .build();

            evaluation.publish( groupTwoDone, "groupTwo" );

            // Success
            evaluation.markPublicationCompleteReportedSuccess();
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

    /**
     * Helper that returns an aggregator for statistics messages.
     * 
     * @return a statistics aggregator
     */

    private static Function<Collection<Statistics>, Statistics> getStatisticsAggregator()
    {
        return statistics -> {

            // Build the aggregate statistics
            Statistics.Builder aggregate = Statistics.newBuilder();

            // Merge the cached statistics
            for ( Statistics next : statistics )
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
