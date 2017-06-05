package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.engine.statistics.metric.MeanAbsoluteError.MeanAbsoluteErrorBuilder;
import wres.engine.statistics.metric.MeanError.MeanErrorBuilder;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * <p>
 * Tests the {@link Metric}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricTest
{

    /**
     * Constructs a {@link Metric} and tests the {@link Metric#nameEquals(Object)}.
     */

    @Test
    public void test1NameEquals()
    {

        //Build a metric
        final MeanErrorBuilder<SingleValuedPairs, ScalarOutput> b = new MeanError.MeanErrorBuilder<>();
        final MeanError<SingleValuedPairs, ScalarOutput> me = b.build();
        //Build another metric
        final MeanError<SingleValuedPairs, ScalarOutput> me2 = b.build();
        //Build a different metric
        final MeanAbsoluteErrorBuilder<SingleValuedPairs, ScalarOutput> c =
                                                                          new MeanAbsoluteError.MeanAbsoluteErrorBuilder<>();
        final MeanAbsoluteError<SingleValuedPairs, ScalarOutput> mae = c.build();

        //Check for equality of names
        assertTrue("Unexpected difference in metric names.", me.nameEquals(me2));
        assertTrue("Unexpected equality in metric names.", !mae.nameEquals(me));
        assertTrue("Unexpected equality in metric names.", !mae.nameEquals(null));
        assertTrue("Unexpected equality in metric names.", !mae.nameEquals(new Integer(5)));
    }

    /**
     * Constructs a {@link Metric} and tests the {@link Metric#toString()}.
     */

    @Test
    public void test2ToString()
    {

        //Build a metric
        final MeanErrorBuilder<SingleValuedPairs, ScalarOutput> b = new MeanError.MeanErrorBuilder<>();
        final MeanError<SingleValuedPairs, ScalarOutput> me = b.build();

        //Check for equality of names
        assertTrue("Unexpected metric name.", "Mean Error".equals(me.toString()));
    }

}
