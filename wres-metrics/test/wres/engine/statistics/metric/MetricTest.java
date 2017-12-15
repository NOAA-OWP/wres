package wres.engine.statistics.metric;

import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.ScalarOutput;

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
     * Constructs a {@link Metric} and tests for checked exceptions.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1Exceptions() throws MetricParameterException
    {
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
