package wres.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import wres.statistics.MessageFactory;
import wres.statistics.generated.ScoreStatistic;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.ScoreStatistic.ScoreStatisticComponent;

/**
 * Tests the {@link OneGroupConsumer}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class OneGroupConsumerTest
{

    @Test
    public void testAdditionOfIntegers()
    {
        // Actual value of integers added within a group
        AtomicInteger sum = new AtomicInteger();

        // Each consumer group involves an addition of integers
        Function<List<Integer>, Integer> aggregator = list -> list.stream().mapToInt( Integer::intValue ).sum();
        Consumer<List<Integer>> consumer = aList -> sum.set( aggregator.apply( aList ) );
        OneGroupConsumer<Integer> group = OneGroupConsumer.of( consumer, "someGroupId" );

        group.accept( 23 );
        group.accept( 17 );
        group.accept( 5 );
        group.accept( -12 );

        // Compute sum
        group.acceptGroup();

        assertEquals( 33, sum.get() );
    }

    @Test
    public void testAggregationOfStatistics()
    {
        // Aggregated statistics
        AtomicReference<Statistics> aggregated = new AtomicReference<>();

        // Each consumer group involves an addition of integers
        Consumer<List<Statistics>> consumer = aList -> aggregated.set( MessageFactory.getStatisticsAggregator()
                                                                                             .apply( aList ) );
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( consumer, "someGroupId" );

        // Add two statistics and accept them
        Statistics.Builder one = Statistics.newBuilder();
        ScoreStatisticComponent.Builder componentBuilder = ScoreStatisticComponent.newBuilder().setValue( 3 );
        ScoreStatistic.Builder scoreBuilder = ScoreStatistic.newBuilder().addStatistics( componentBuilder );
        one.addScores( scoreBuilder );

        group.accept( one.build() );

        Statistics.Builder another = Statistics.newBuilder();
        ScoreStatisticComponent.Builder anotherComponentBuilder = ScoreStatisticComponent.newBuilder().setValue( 7 );
        ScoreStatistic.Builder anotherScoreBuilder =
                ScoreStatistic.newBuilder().addStatistics( anotherComponentBuilder );
        another.addScores( anotherScoreBuilder );

        group.accept( another.build() );

        // Compute aggregate
        group.acceptGroup();

        // Expected
        Statistics expected = Statistics.newBuilder()
                                        .mergeFrom( one.build() )
                                        .mergeFrom( another.build() )
                                        .build();

        assertEquals( expected, aggregated.get() );
    }

    @Test
    public void reuseOfAConsumerThrowsAnExceptionOnAcceptGroup()
    {
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( statistics -> {
        }, "someGroupId" );

        group.acceptGroup();

        IllegalStateException expected = assertThrows( IllegalStateException.class,
                                                       () -> group.acceptGroup() );

        String expectedMessage = "Attempted to reuse a one-use consumer, which is not allowed.";

        assertEquals( expectedMessage, expected.getMessage() );
    }

    @Test
    public void reuseOfAConsumerThrowsAnExceptionOnAccept()
    {
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( statistics -> {
        }, "someGroupId" );

        group.acceptGroup();

        Statistics defaultInstance = Statistics.getDefaultInstance();

        IllegalStateException expected = assertThrows( IllegalStateException.class,
                                                       () -> group.accept( defaultInstance ) );

        String expectedMessage = "Attempted to reuse a one-use consumer, which is not allowed.";

        assertEquals( expectedMessage, expected.getMessage() );
    }

    @Test
    public void testGetGroupId()
    {
        // No-op consumer
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( statistics -> {
        }, "someGroupId" );

        assertEquals( "someGroupId", group.getGroupId() );
    }

    @Test
    public void testSize()
    {
        // No-op consumer
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( statistics -> {
        }, "someGroupId" );

        assertEquals( 0, group.size() );
    }
}
