package wres.statistics;

import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.MetricName;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * Tests the generated statistics messages.
 *
 * @author James Brown
 */

class StatisticsTest
{

    @Test
    void testThatAStatisticCanBeCreated()
    {
        DoubleScoreMetricComponent metric = DoubleScoreMetricComponent.newBuilder()
                                                                      .setName( ComponentName.SHARPNESS )
                                                                      .build();
        DoubleScoreStatistic aScore =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BRIER_SCORE ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( metric )
                                                                                 .setValue( 1.0 ) )
                                    .build();

        assertEquals( 1.0, aScore.getStatistics( 0 ).getValue(), 0.0001 );
    }

}
