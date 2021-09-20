package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.pools.Pool;

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
        SampleSize<Pool<Pair<Double, Double>>> ss = SampleSize.of();

        assertEquals( "SAMPLE SIZE", ss.toString() );
    }

}
