package wres.engine.statistics.metric;

import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;

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
