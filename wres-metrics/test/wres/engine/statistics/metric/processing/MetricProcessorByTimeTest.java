package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link MetricProcessorByTime}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorByTimeTest
{

    /**
     * Tests the {@link MetricProcessorByTime#getFilterForSingleValuedPairs(wres.datamodel.thresholds.Threshold)}.
     */

    @Test
    public void testGetFilterForSingleValuedPairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        Operator condition = Operator.GREATER;
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( Threshold.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( Threshold.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( Threshold.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT_AND_RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( Threshold.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( Threshold.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( Threshold.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.RIGHT_MEAN ) ) );
    }

    /**
     * Tests the {@link MetricProcessorByTime#getFilterForTimeSeriesOfSingleValuedPairs(wres.datamodel.thresholds.Threshold)}.
     */

    @Test
    public void testGetFilterForTimeSeriesOfSingleValuedPairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        Operator condition = Operator.GREATER;
        assertNotNull( MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( Threshold.of( doubles,
                                                                                                      condition,
                                                                                                      ThresholdDataType.LEFT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( Threshold.of( doubles,
                                                                                                      condition,
                                                                                                      ThresholdDataType.RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( Threshold.of( doubles,
                                                                                                      condition,
                                                                                                      ThresholdDataType.LEFT_AND_RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( Threshold.of( doubles,
                                                                                                      condition,
                                                                                                      ThresholdDataType.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( Threshold.of( doubles,
                                                                                                      condition,
                                                                                                      ThresholdDataType.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( Threshold.of( doubles,
                                                                                                      condition,
                                                                                                      ThresholdDataType.RIGHT_MEAN ) ) );
    }

}
