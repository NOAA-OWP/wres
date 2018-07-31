package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Tests the {@link Metric} using single-valued metrics.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricSingleValuedTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Constructs a {@link Metric} and tests the {@link Metric#nameEquals(Object)}.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testNameEquals() throws MetricParameterException
    {

        //Build a metric
        final MeanError me = MeanError.of();
        //Build another metric
        final MeanError me2 = MeanError.of();
        //Build a different metric
        final MeanAbsoluteError mae = MeanAbsoluteError.of();
        
        //Check for equality of names
        assertTrue( "Unexpected difference in metric names.", me.nameEquals( me2 ) );
        assertTrue( "Unexpected equality in metric names.", !mae.nameEquals( me ) );
        assertTrue( "Unexpected equality in metric names.", !mae.nameEquals( null ) );
        assertTrue( "Unexpected equality in metric names.", !mae.nameEquals( new Integer( 5 ) ) );
    }

    /**
     * Constructs a {@link Metric} and tests the {@link Metric#toString()}.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testToString() throws MetricParameterException
    {

        //Build a metric
        final MeanError me = MeanError.of();

        //Check for equality of names
        assertTrue( "Unexpected metric name.",
                    MetricConstants.MEAN_ERROR.toString().equals( me.toString() ) );
    }

}
