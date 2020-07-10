package wres.statistics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * Tests the generated statistics messages.
 * 
 * @author james.brown@hydrosolved.com
 */

public class StatisticsTest
{

    @Test
    public void testThatAStatisticCanBeCreated()
    {
        DoubleScoreStatistic aScore =
                DoubleScoreStatistic.newBuilder()
                              .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                     .setName( ComponentName.MAIN )
                                                                     .setValue( 1.0 ) )
                              .build();
        
        assertEquals( 1.0, aScore.getStatistics( 0 ).getValue(), 0.0001 );
    }

}
