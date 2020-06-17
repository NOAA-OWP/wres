package wres.events;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.jms.JMSException;
import javax.naming.NamingException;

import org.junit.Before;
import org.junit.Test;

import wres.eventsbroker.BrokerConnectionFactory;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.ScoreStatistic;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.ScoreStatistic.ScoreStatisticComponent;

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
            ScoreStatisticComponent.Builder componentBuilder = ScoreStatisticComponent.newBuilder().setValue( i );
            ScoreStatistic.Builder scoreBuilder = ScoreStatistic.newBuilder().addStatistics( componentBuilder );
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
            ScoreStatisticComponent.Builder componentBuilder = ScoreStatisticComponent.newBuilder().setValue( i );
            ScoreStatistic.Builder scoreBuilder = ScoreStatistic.newBuilder().addStatistics( componentBuilder );
            statistics.addScores( scoreBuilder );
            this.anotherStatistics.add( statistics.build() );
        }
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

        ConsumerGroup consumerGroup =
                new ConsumerGroup.Builder().addStatusConsumer( status )
                                           .addEvaluationConsumer( description )
                                           .addStatisticsConsumer( statistics )
                                           .build();

        Consumer<wres.statistics.generated.Evaluation> otherDescription = actualEvaluations::add; // Common store
        Consumer<EvaluationStatus> otherStatus = otherActualStatuses::add;
        Consumer<Statistics> otherStatistics = otherActualStatistics::add;

        ConsumerGroup otherConsumerGroup =
                new ConsumerGroup.Builder().addStatusConsumer( otherStatus )
                                           .addEvaluationConsumer( otherDescription )
                                           .addStatisticsConsumer( otherStatistics )
                                           .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( BrokerConnectionFactory connections = BrokerConnectionFactory.of();
              Evaluation evaluationOne = Evaluation.open( this.oneEvaluation, connections, consumerGroup );
              Evaluation evaluationTwo = Evaluation.open( this.anotherEvaluation, connections, otherConsumerGroup ) )
        {
            // First evaluation
            for ( Statistics next : this.oneStatistics )
            {
                evaluationOne.publish( next );
            }

            // Second evaluation
            for ( Statistics next : this.anotherStatistics )
            {
                evaluationTwo.publish( next );
            }
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
    public void publishAndConsumeOneEvaluationsWithTwoStatusConsumers()
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
        ConsumerGroup consumerGroup =
                new ConsumerGroup.Builder().addStatusConsumer( status )
                                           .addStatusConsumer( status )
                                           .addEvaluationConsumer( description )
                                           .addStatisticsConsumer( statistics )
                                           .build();

        // Create and start a broker and open an evaluation, closing on completion
        try ( BrokerConnectionFactory connections = BrokerConnectionFactory.of();
              Evaluation evaluationOne = Evaluation.open( this.oneEvaluation, connections, consumerGroup ); )
        {
            // First evaluation
            for ( Statistics next : this.oneStatistics )
            {
                evaluationOne.publish( next );
            }
        }

        List<wres.statistics.generated.Evaluation> expectedEvaluations =
                List.of( this.oneEvaluation );

        assertEquals( expectedEvaluations, actualEvaluations );
        assertEquals( this.oneStatistics, actualStatistics );

        // For status messages, assert number only
        assertEquals( 24, actualStatuses.size() );
    }

}
