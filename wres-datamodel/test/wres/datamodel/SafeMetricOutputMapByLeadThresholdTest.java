package wres.datamodel;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.Threshold.Operator;

/**
 * Tests the {@link SafeMetricOutputMapByLeadThreshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SafeMetricOutputMapByLeadThresholdTest
{

    /**
     * Constructs a {@link SafeMetricOutputMapByLeadThreshold} and slices the map, testing for equality against a
     * benchmark.
     */

    @Test
    public void test1ConstructAndSlice()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetricOutputMapByLeadThreshold<ScalarOutput> results =
                DataModelTestDataFactory.getScalarMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final int leadTimeOne = 42;
        final Threshold q = outputFactory.getQuantileThreshold( 531.88, 0.005, Operator.GREATER );
        final MapBiKey<Integer, Threshold> testKeyOne = outputFactory.getMapKey( leadTimeOne, q );
        final MetricOutputMapByLeadThreshold<ScalarOutput> subMap =
                results.sliceByLead( leadTimeOne ).sliceByThreshold( q );
        //Slice by threshold = 531.88
        final MetricOutputMapByLeadThreshold<ScalarOutput> subMap2 = results.sliceByThreshold( q );

        //Acquire a submap by threshold = all data and lead time = 714
        final int leadTimeTwo = 714;
        final Threshold q2 = outputFactory.getQuantileThreshold( Double.NEGATIVE_INFINITY,
                                                                 Double.NEGATIVE_INFINITY,
                                                                 Operator.GREATER );
        final MapBiKey<Integer, Threshold> testKeyTwo = outputFactory.getMapKey( leadTimeTwo, q2 );

        //Slice by threshold = all data
        final MetricOutputMapByLeadThreshold<ScalarOutput> subMap3 = results.sliceByLead( leadTimeTwo )
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
