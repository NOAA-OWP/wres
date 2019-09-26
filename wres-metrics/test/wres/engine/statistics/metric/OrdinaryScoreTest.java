package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.sampledata.pairs.SingleValuedPairs;

/**
 * Tests the {@link OrdinaryScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class OrdinaryScoreTest
{

    /**
     * Constructs a {@link OrdinaryScore} and compares the actual result to the expected result.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testToString()
    {
        SampleSize<SingleValuedPairs> ss = SampleSize.of();

        assertTrue( "SAMPLE SIZE".equals( ss.toString() ) );
    }

}
