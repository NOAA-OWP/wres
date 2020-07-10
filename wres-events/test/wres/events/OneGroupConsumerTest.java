package wres.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Test;

import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.Statistics;

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
        Function<Collection<Integer>, Integer> aggregator = list -> list.stream().mapToInt( Integer::intValue ).sum();
        Consumer<Collection<Integer>> consumer = aList -> sum.set( aggregator.apply( aList ) );
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
        Consumer<Collection<Statistics>> consumer =
                aList -> aggregated.set( OneGroupConsumerTest.getStatisticsAggregator()
                                                             .apply( aList ) );
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( consumer, "someGroupId" );

        // Add two statistics and accept them
        Statistics.Builder one = Statistics.newBuilder();
        DoubleScoreStatisticComponent.Builder componentBuilder =
                DoubleScoreStatisticComponent.newBuilder().setValue( 3 );
        DoubleScoreStatistic.Builder scoreBuilder = DoubleScoreStatistic.newBuilder().addStatistics( componentBuilder );
        one.addScores( scoreBuilder );

        group.accept( one.build() );

        Statistics.Builder another = Statistics.newBuilder();
        DoubleScoreStatisticComponent.Builder anotherComponentBuilder =
                DoubleScoreStatisticComponent.newBuilder().setValue( 7 );
        DoubleScoreStatistic.Builder anotherScoreBuilder =
                DoubleScoreStatistic.newBuilder().addStatistics( anotherComponentBuilder );
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
}
