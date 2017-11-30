package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.QuantileQuantileDiagram.QuantileQuantileDiagramBuilder;

/**
 * Tests the {@link QuantileQuantileDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class QuantileQuantileDiagramTest
{

    /**
     * Constructs a {@link QuantileQuantileDiagram} and compares the actual result to the expected result. Also, checks
     * the parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1QuantileQuantileDiagram() throws MetricParameterException
    {
        //Build the metric
        final QuantileQuantileDiagramBuilder b = new QuantileQuantileDiagramBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory( outF );

        final QuantileQuantileDiagram qq = b.build();

        //Generate some data
        final List<PairOfDoubles> values = new ArrayList<>();
        for ( int i = 1; i < 1001; i++ )
        {
            double left = i;
            double right = left;
            values.add( outF.pairOf( left, right ) );
        }

        final SingleValuedPairs input = outF.ofSingleValuedPairs( values, metaFac.getMetadata() );

        //Check the results       
        final MultiVectorOutput actual = qq.apply( input );
        double[] actualObs = actual.get( MetricDimension.OBSERVED_QUANTILES ).getDoubles();
        double[] actualPred = actual.get( MetricDimension.PREDICTED_QUANTILES ).getDoubles();

        //Check the first pair of quantiles, which should map to the first entry, since the lower bound is unknown
        assertTrue( "Difference between actual and expected quantiles of observations [" + 1.0
                    + ", "
                    + actualObs[0]
                    + "].",
                    Double.compare( actualObs[0], 1.0 ) == 0 );
        assertTrue( "Difference between actual and expected quantiles of predictions [" + 1.0
                    + ", "
                    + actualPred[0]
                    + "].",
                    Double.compare( actualPred[0], 1.0 ) == 0 );

        //Expected values
        for ( int i = 1; i < 1000; i++ )
        {
            double expectedObserved = i + 1;
            double expectedPredicted = i + 1;
            double actualObserved = Precision.round( actualObs[i], 5 );
            double actualPredicted = Precision.round( actualPred[i], 5 );
            assertTrue( "Difference between actual and expected quantiles of observations [" + expectedObserved
                        + ", "
                        + actualObserved
                        + "].",
                        Double.compare( actualObserved, expectedObserved ) == 0 );
            assertTrue( "Difference between actual and expected quantiles of predictions [" + expectedPredicted
                        + ", "
                        + actualPredicted
                        + "].",
                        Double.compare( actualPredicted, expectedPredicted ) == 0 );
        }

        //Check the parameters
        assertTrue( "Unexpected name for the Quantile-Quantile Diagram.",
                    qq.getName().equals( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.toString() ) );
    }

    /**
     * Constructs a {@link QuantileQuantileDiagram} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {

        //Build the metric
        final QuantileQuantileDiagramBuilder b = new QuantileQuantileDiagramBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );

        final QuantileQuantileDiagram qq = b.build();

        //Check exceptions
        try
        {
            qq.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }

}
