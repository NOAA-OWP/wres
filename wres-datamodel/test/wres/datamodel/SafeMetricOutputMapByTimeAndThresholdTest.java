package wres.datamodel;

import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.Threshold.Operator;
import wres.datamodel.outputs.MapBiKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScalarOutput;
import wres.datamodel.time.ReferenceTime;
import wres.datamodel.time.TimeWindow;

/**
 * Tests the {@link SafeMetricOutputMapByTimeAndThreshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeMetricOutputMapByTimeAndThresholdTest
{

    /**
     * Constructs a {@link SafeMetricOutputMapByTimeAndThreshold} and slices the map, testing for equality against a
     * benchmark.
     */

    @Test
    public void test1ConstructAndSlice()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByTimeAndThreshold<ScalarOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final TimeWindow timeWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                              Instant.parse( "2010-12-31T11:59:59Z" ),
                                              ReferenceTime.VALID_TIME,
                                              42 );
        final Threshold q = outputFactory.getQuantileThreshold( 531.88, 0.005, Operator.GREATER );
        final MapBiKey<TimeWindow, Threshold> testKeyOne = outputFactory.getMapKey( timeWindow, q );
        final MetricOutputMapByTimeAndThreshold<ScalarOutput> subMap =
                results.sliceByTime( timeWindow ).sliceByThreshold( q );
        //Slice by threshold = 531.88
        final MetricOutputMapByTimeAndThreshold<ScalarOutput> subMap2 = results.sliceByThreshold( q );

        //Acquire a submap by threshold = all data and lead time = 714
        final TimeWindow timeWindowTwo = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     ReferenceTime.VALID_TIME,
                                                     714 );
        
        
        final Threshold q2 = outputFactory.getQuantileThreshold( Double.NEGATIVE_INFINITY,
                                                                 Double.NEGATIVE_INFINITY,
                                                                 Operator.GREATER );
        final MapBiKey<TimeWindow, Threshold> testKeyTwo = outputFactory.getMapKey( timeWindowTwo, q2 );

        //Slice by threshold = all data
        final MetricOutputMapByTimeAndThreshold<ScalarOutput> subMap3 = results.sliceByTime( timeWindowTwo )
                                                                               .sliceByThreshold( q2 );

        //Check the results
        final double actualOne = subMap.get( testKeyOne ).getData();
        final double expectedOne = 0.026543876961751534;
        final double actualTwo = subMap3.get( testKeyTwo ).getData();
        final double expectedTwo = 0.005999378857020621;
        assertTrue( "Unexpected output from data slice [" + actualOne
                    + ","
                    + expectedOne
                    + "].",
                    Double.compare( actualOne, expectedOne ) == 0 );
        assertTrue( "Unexpected output from data slice [" + actualTwo
                    + ","
                    + expectedTwo
                    + "].",
                    Double.compare( actualTwo, expectedTwo ) == 0 );
        assertTrue( "Unexpected output size from data slice [" + subMap2.size()
                    + ","
                    + 29
                    + "].",
                    subMap2.size() == 29 );
        assertTrue( "Expected quantile thresholds in store.", subMap.hasQuantileThresholds() );

    }

}
