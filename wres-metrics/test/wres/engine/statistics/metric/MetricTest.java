package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.engine.statistics.metric.MeanAbsoluteError.MeanAbsoluteErrorBuilder;
import wres.engine.statistics.metric.MeanError.MeanErrorBuilder;

/**
 * Tests the {@link Metric}.
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
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        b.setOutputFactory(outF);
        final MeanError me = b.build();
        //Build another metric
        final MeanError me2 = b.build();
        //Build a different metric
        final MeanAbsoluteErrorBuilder c = new MeanAbsoluteError.MeanAbsoluteErrorBuilder();
        c.setOutputFactory(outF);
        final MeanAbsoluteError mae = c.build();

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
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory(outF);
        final MeanError me = b.build();

        //Check for equality of names
        assertTrue("Unexpected metric name.", metaFac.getMetricName(MetricConstants.MEAN_ERROR).equals(me.toString()));
    }

    /**
     * Constructs a {@link Metric} and tests for checked exceptions.
     */

    @Test
    public void test3Exceptions()
    {
        //Build a metric
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        try
        {
            b.build();
            fail("Expected a checked exception on building a metric without an output factory.");
        }
        catch(UnsupportedOperationException e)
        {
        }
    }

}
