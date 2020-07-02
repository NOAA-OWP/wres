package wres.statistics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import wres.statistics.generated.ScoreMetric.ScoreMetricComponent.ScoreComponentName;
import wres.statistics.generated.ScoreStatistic;
import wres.statistics.generated.ScoreStatistic.ScoreStatisticComponent;

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
        ScoreStatistic aScore =
                ScoreStatistic.newBuilder()
                              .addStatistics( ScoreStatisticComponent.newBuilder()
                                                                     .setName( ScoreComponentName.MAIN_SCORE )
                                                                     .setValue( 1.0 ) )
                              .build();
        
        assertEquals( 1.0, aScore.getStatistics( 0 ).getValue(), 0.0001 );
    }

}
