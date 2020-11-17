package wres.events.subscribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
        Function<Collection<Integer>, Set<Path>> consumer = aList -> {
            sum.set( aggregator.apply( aList ) );
            return Set.of();
        };
        OneGroupConsumer<Integer> group = OneGroupConsumer.of( consumer, "someGroupId" );

        group.accept( "a", 23 );
        group.accept( "b", 17 );
        group.accept( "c", 5 );
        group.accept( "d", -12 );

        // Set the expected group size, which triggers completion
        group.setExpectedMessageCount( 4 );

        assertEquals( 33, sum.get() );
    }

    @Test
    public void testAggregationOfStatistics()
    {
        // Aggregated statistics
        AtomicReference<Statistics> aggregated = new AtomicReference<>();

        // Consumer
        Function<Collection<Statistics>, Set<Path>> consumer = aList -> {
            aggregated.set( OneGroupConsumerTest.getStatisticsAggregator().apply( aList ) );
            return Set.of();
        };

        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( consumer, "someGroupId" );

        // Add two statistics and accept them
        Statistics.Builder one = Statistics.newBuilder();
        DoubleScoreStatisticComponent.Builder componentBuilder =
                DoubleScoreStatisticComponent.newBuilder().setValue( 3 );
        DoubleScoreStatistic.Builder scoreBuilder = DoubleScoreStatistic.newBuilder().addStatistics( componentBuilder );
        one.addScores( scoreBuilder );

        // Set the expected group size, which triggers completion when both messages have been received
        group.setExpectedMessageCount( 2 );
        
        group.accept( "a", one.build() );

        Statistics.Builder another = Statistics.newBuilder();
        DoubleScoreStatisticComponent.Builder anotherComponentBuilder =
                DoubleScoreStatisticComponent.newBuilder().setValue( 7 );
        DoubleScoreStatistic.Builder anotherScoreBuilder =
                DoubleScoreStatistic.newBuilder().addStatistics( anotherComponentBuilder );
        another.addScores( anotherScoreBuilder );

        group.accept( "b", another.build() );

        // Expected
        Statistics expected = Statistics.newBuilder()
                                        .mergeFrom( one.build() )
                                        .mergeFrom( another.build() )
                                        .build();

        assertEquals( expected, aggregated.get() );
    }

    @Test
    public void checkForExpectedExceptionWhenSettingTheExpectedMessageCountTwice()
    {
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( statistics -> {
            return Set.of();
        }, "someGroupId" );

        group.setExpectedMessageCount( 1 );
        group.accept( "aMessage", Statistics.getDefaultInstance() );

        IllegalStateException expected = assertThrows( IllegalStateException.class,
                                                       () -> group.setExpectedMessageCount( 1 ) );

        String expectedMessage = "The message count has already been set and cannot be reset.";

        assertEquals( expectedMessage, expected.getMessage() );
    }

    @Test
    public void checkForExpectedExceptionOnReusingAConsumer()
    {
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( statistics -> {
            return Set.of();
        }, "someGroupId" );

        group.setExpectedMessageCount( 1 );

        // Completes the group
        group.accept( "aMessage", Statistics.getDefaultInstance() );
        
        Statistics defaultInstance = Statistics.getDefaultInstance();

        IllegalStateException expected = assertThrows( IllegalStateException.class,
                                                       () -> group.accept( "a", defaultInstance ) );

        String expectedMessage = "Attempted to reuse a one-use consumer, which is not allowed.";

        assertEquals( expectedMessage, expected.getMessage() );
    }

    @Test
    public void testGetGroupId()
    {
        // No-op consumer
        OneGroupConsumer<Statistics> group = OneGroupConsumer.of( statistics -> {
            return Set.of();
        }, "someGroupId" );

        assertEquals( "someGroupId", group.getGroupId() );
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
