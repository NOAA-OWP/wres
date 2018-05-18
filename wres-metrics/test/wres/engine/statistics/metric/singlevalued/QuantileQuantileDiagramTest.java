package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram.QuantileQuantileDiagramBuilder;

/**
 * Tests the {@link QuantileQuantileDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class QuantileQuantileDiagramTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link QuantileQuantileDiagram}.
     */

    private QuantileQuantileDiagram qqd;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        QuantileQuantileDiagramBuilder b = new QuantileQuantileDiagram.QuantileQuantileDiagramBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.qqd = b.build();
    }

    /**
     * Compares the output from {@link QuantileQuantileDiagram#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Metadata for the output
        MetadataFactory metaFac = outF.getMetadataFactory();

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
        final MultiVectorOutput actual = qqd.apply( input );
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
    }

    /**
     * Validates the output from {@link QuantileQuantileDiagram#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                outF.ofSingleValuedPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );

        MultiVectorOutput actual = qqd.apply( input );

        double[] source = new double[1000];

        Arrays.fill( source, Double.NaN );

        assertTrue( Arrays.equals( actual.getData().get( MetricDimension.PREDICTED_QUANTILES ).getDoubles(), source ) );

        assertTrue( Arrays.equals( actual.getData().get( MetricDimension.OBSERVED_QUANTILES ).getDoubles(), source ) );
    }

    /**
     * Checks that the {@link QuantileQuantileDiagram#getName()} returns 
     * {@link MetricConstants#QUANTILE_QUANTILE_DIAGRAM.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( qqd.getName().equals( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.toString() ) );
    }

    /**
     * Tests for an expected exception on calling {@link QuantileQuantileDiagram#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'QUANTILE QUANTILE DIAGRAM'." );

        qqd.apply( null );
    }

}
