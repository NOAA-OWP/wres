package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.discreteprobability.ReliabilityDiagram.ReliabilityDiagramBuilder;

/**
 * Tests the {@link ReliabilityDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ReliabilityDiagramTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link ReliabilityDiagram}.
     */

    private ReliabilityDiagram rel;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        ReliabilityDiagramBuilder b = new ReliabilityDiagramBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.rel = b.build();
    }

    /**
     * Compares the output from {@link ReliabilityDiagram#apply(DiscreteProbabilityPairs)} against 
     * expected output.
     */

    @Test
    public void testApply() throws MetricParameterException
    {
        //Generate some data
        DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsThree();

        MetadataFactory metaFac = outF.getMetadataFactory();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getRawData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.RELIABILITY_DIAGRAM,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( metaFac.getLocation("Tampere"), "MAP", "FMI" ) );

        //Check the results       
        final MultiVectorOutput actual = rel.apply( input );
        double[] expectedFProb = new double[] { 0.05490196078431369, 0.19999999999999984, 0.3000000000000002,
                                                0.40000000000000013, 0.5, 0.5999999999999998, 0.6999999999999996,
                                                0.8000000000000003, 0.9000000000000002,
                                                1.0 };
        double[] expectedOProb = new double[] { 0.0196078431372549, 0.0847457627118644, 0.12195121951219512,
                                                0.21052631578947367, 0.36363636363636365, 0.2727272727272727,
                                                0.47058823529411764, 0.6666666666666666,
                                                0.7272727272727273, 0.8461538461538461 };
        double[] expectedSample = new double[] { 102.0, 59.0, 41.0, 19.0, 22.0, 22.0, 34.0, 24.0, 11.0, 13.0 };

        Map<MetricDimension, double[]> output = new HashMap<>();
        output.put( MetricDimension.FORECAST_PROBABILITY, expectedFProb );
        output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, expectedOProb );
        output.put( MetricDimension.SAMPLE_SIZE, expectedSample );

        final MultiVectorOutput expected = outF.ofMultiVectorOutput( output, m1 );

        assertTrue( "Difference between actual and expected Reliability Diagram.", actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link ReliabilityDiagram#apply(DiscreteProbabilityPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                outF.ofDiscreteProbabilityPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );

        MultiVectorOutput actual = rel.apply( input );

        double[] source = new double[10];
        double[] sourceSample = new double[10];

        Arrays.fill( source, Double.NaN );

        assertTrue( Arrays.equals( actual.getData()
                                         .get( MetricDimension.FORECAST_PROBABILITY )
                                         .getDoubles(),
                                   source ) );

        assertTrue( Arrays.equals( actual.getData()
                                         .get( MetricDimension.OBSERVED_RELATIVE_FREQUENCY )
                                         .getDoubles(),
                                   source ) );

        assertTrue( Arrays.equals( actual.getData()
                                         .get( MetricDimension.SAMPLE_SIZE )
                                         .getDoubles(),
                                   sourceSample ) );
    }

    /**
     * Checks that the {@link ReliabilityDiagram#getName()} returns 
     * {@link MetricConstants.RELIABILITY_DIAGRAM.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( rel.getName().equals( MetricConstants.RELIABILITY_DIAGRAM.toString() ) );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link ReliabilityDiagram#apply(DiscreteProbabilityPairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'RELIABILITY DIAGRAM'." );

        rel.apply( null );
    }

}
