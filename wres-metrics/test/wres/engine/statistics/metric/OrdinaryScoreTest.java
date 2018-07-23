package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;

/**
 * Tests the {@link OrdinaryScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class OrdinaryScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Constructs a {@link OrdinaryScore} and compares the actual result to the expected result.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testToString() throws MetricParameterException
    {
        SampleSizeBuilder<SingleValuedPairs> b = new SampleSizeBuilder<>();
        SampleSize<SingleValuedPairs> ss = b.build();

        assertTrue( "SAMPLE SIZE".equals( ss.toString() ) );
    }

}
