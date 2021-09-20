package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.engine.statistics.metric.Metric;

/**
 * Tests the {@link Metric} using single-valued metrics.
 * 
 * @author James Brown
 */
public final class MetricSingleValuedTest
{

    /**
     * Constructs a {@link Metric} and tests the {@link Metric#toString()}.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testToString()
    {

        //Build a metric
        final MeanError me = MeanError.of();

        //Check for equality of names
        assertEquals( MetricConstants.MEAN_ERROR.toString(), me.toString() );
    }

}
