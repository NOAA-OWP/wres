package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the {@link MetricProcessorByTime}.
 * 
 * @author James Brown
 */
public final class MetricProcessorByTimeTest
{

    @Test
    public void testGetFilterForSingleValuedPairs()
    {
        OneOrTwoDoubles doubles = OneOrTwoDoubles.of( 1.0 );
        Operator condition = Operator.GREATER;
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( ThresholdOuter.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( ThresholdOuter.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( ThresholdOuter.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT_AND_RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( ThresholdOuter.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT_AND_ANY_RIGHT ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( ThresholdOuter.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.LEFT_AND_RIGHT_MEAN ) ) );
        assertNotNull( MetricProcessorByTime.getFilterForSingleValuedPairs( ThresholdOuter.of( doubles,
                                                                                          condition,
                                                                                          ThresholdDataType.RIGHT_MEAN ) ) );
    }

}
