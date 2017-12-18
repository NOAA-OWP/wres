package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError.MeanAbsoluteErrorBuilder;
import wres.engine.statistics.metric.singlevalued.MeanError.MeanErrorBuilder;

/**
 * Tests the {@link Metric} using single-valued metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricSingleValuedTest
{

    /**
     * Constructs a {@link Metric} and tests the {@link Metric#nameEquals(Object)}.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1NameEquals() throws MetricParameterException
    {

        //Build a metric
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        final MeanError me = b.build();
        //Build another metric
        final MeanError me2 = b.build();
        //Build a different metric
        final MeanAbsoluteErrorBuilder c = new MeanAbsoluteError.MeanAbsoluteErrorBuilder();
        c.setOutputFactory( outF );
        final MeanAbsoluteError mae = c.build();

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
    public void test2ToString() throws MetricParameterException
    {

        //Build a metric
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        final MeanError me = b.build();

        //Check for equality of names
        assertTrue( "Unexpected metric name.",
                    MetricConstants.MEAN_ERROR.toString().equals( me.toString() ) );
    }

    /**
     * Constructs a {@link Metric} and tests for checked exceptions.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test3Exceptions() throws MetricParameterException
    {
        //No data factory
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        try
        {
            b.build();
            fail( "Expected a checked exception on building a metric without an output factory." );
        }
        catch ( MetricParameterException e )
        {
        }
        //No builder test
        class MetricNoBuilder extends Metric<SingleValuedPairs, ScalarOutput>
        {
            protected MetricNoBuilder( MetricBuilder<SingleValuedPairs, ScalarOutput> builder )
                    throws MetricParameterException
            {
                super( null );
            }

            @Override
            public ScalarOutput apply( SingleValuedPairs s )
            {
                return null;
            }

            @Override
            public MetricConstants getID()
            {
                return null;
            }

            @Override
            public boolean hasRealUnits()
            {
                return false;
            }
        }
        try
        {
            new MetricNoBuilder( null );
            fail( "Expected a checked exception on building a metric without a builder." );
        }
        catch ( MetricParameterException e )
        {
        }
    }

}
