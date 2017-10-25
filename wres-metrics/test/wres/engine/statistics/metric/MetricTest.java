package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
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
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory( outF );
        final MeanError me = b.build();

        //Check for equality of names
        assertTrue( "Unexpected metric name.",
                    metaFac.getMetricName( MetricConstants.MEAN_ERROR ).equals( me.toString() ) );
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
        //Obtaining metadata from a Collectable not allowed
        try
        {
            final DataFactory outF = DefaultDataFactory.getInstance();
            MetricFactory.getInstance( outF ).ofCorrelationPearsons().getMetadata(
                                                                                   MetricTestDataFactory.getSingleValuedPairsOne(),
                                                                                   1,
                                                                                   MetricConstants.MAIN,
                                                                                   null );
            fail( "Expected a checked exception on requesting metadata from a collectable metric." );
        }
        catch ( UnsupportedOperationException e )
        {
        }
    }

}
