package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.Threshold.Condition;

/**
 * Tests the {@link ScalarOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricOutputMapByLeadThresholdTest
{

    /**
     * Constructs a {@link SafeMetricOutputMapByLeadThreshold} and slices the map, testing for equality against a
     * benchmark.
     */

    @Test
    public void test1ConstructAndSlice()
    {
        final MetricOutputFactory outputFactory = DefaultMetricOutputFactory.getInstance();
        final MetricOutputMapByLeadThreshold<ScalarOutput> results =
                                                                   DataModelTestDataFactory.getMetricOutputMapByLeadThresholdOne();

        //Acquire a submap by threshold = 531.88 and lead time = 42
        final int leadTimeOne = 42;
        final Quantile q = outputFactory.getQuantile(531.88, 0.005, Condition.GREATER);
        final MapBiKey<Integer, Threshold> testKeyOne = outputFactory.getMapKeyByLeadThreshold(leadTimeOne, q);
        final MetricOutputMapByLeadThreshold<ScalarOutput> subMap =
                                                                  results.sliceByLead(leadTimeOne).sliceByThreshold(q);
        //Slice by threshold = 531.88
        final MetricOutputMapByLeadThreshold<ScalarOutput> subMap2 = results.sliceByThreshold(q);

        //Acquire a submap by threshold = all data and lead time = 714
        final int leadTimeTwo = 714;
        final Quantile q2 = outputFactory.getQuantile(Double.NEGATIVE_INFINITY,
                                                      Double.NEGATIVE_INFINITY,
                                                      Condition.GREATER);
        final MapBiKey<Integer, Threshold> testKeyTwo = outputFactory.getMapKeyByLeadThreshold(leadTimeTwo, q2);

        //Slice by threshold = all data
        final MetricOutputMapByLeadThreshold<ScalarOutput> subMap3 = results.sliceByLead(leadTimeTwo)
                                                                            .sliceByThreshold(q2);

        //Check the results
        final double actualOne = subMap.get(testKeyOne).getData();
        final double expectedOne = 0.026543876961751534;
        final double actualTwo = subMap3.get(testKeyTwo).getData();
        final double expectedTwo = 0.005999378857020621;
        assertTrue("Unexpected output from data slice [" + actualOne + "," + expectedOne + "].",
                   Double.compare(actualOne, expectedOne) == 0);
        assertTrue("Unexpected output from data slice [" + actualTwo + "," + expectedTwo + "].",
                   Double.compare(actualTwo, expectedTwo) == 0);
        assertTrue("Unexpected output size from data slice [" + subMap2.size() + "," + 29 + "].", subMap2.size() == 29);
    }

}
