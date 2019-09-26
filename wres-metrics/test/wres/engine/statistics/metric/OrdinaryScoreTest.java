package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.sampledata.SampleData;

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
        SampleSize<SampleData<Pair<Double, Double>>> ss = SampleSize.of();

        assertTrue( "SAMPLE SIZE".equals( ss.toString() ) );
    }

}
