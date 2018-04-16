package wres.engine.statistics.metric;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;

/**
 * Tests the {@link Metric}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Constructs a {@link Metric} and tests for checked exceptions.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptions() throws MetricParameterException
    {

        exception.expect( UnsupportedOperationException.class );
        exception.expectMessage( "Cannot safely obtain the metadata for the collectable "
                                 + "implementation of '"
                                 + "PEARSON CORRELATION COEFFICIENT"
                                 + "': build the metadata in the implementing class." );

        final DataFactory outF = DefaultDataFactory.getInstance();
        MetricFactory.getInstance( outF )
                     .ofCorrelationPearsons()
                     .getMetadata( MetricTestDataFactory.getSingleValuedPairsOne(),
                                   1,
                                   MetricConstants.MAIN,
                                   null );
    }

}
