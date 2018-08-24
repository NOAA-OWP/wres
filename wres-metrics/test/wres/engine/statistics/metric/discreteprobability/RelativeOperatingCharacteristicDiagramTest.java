package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs;
import wres.datamodel.statistics.MultiVectorOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
/**
 * Tests the {@link RelativeOperatingCharacteristicDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RelativeOperatingCharacteristicDiagramTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link RelativeOperatingCharacteristicDiagram}.
     */

    private RelativeOperatingCharacteristicDiagram roc;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.roc = RelativeOperatingCharacteristicDiagram.of();
    }

    /**
     * Compares the output from {@link RelativeOperatingCharacteristicDiagram#apply(DiscreteProbabilityPairs)} against 
     * expected output.
     */

    @Test
    public void testApply() throws MetricParameterException
    {
        //Generate some data
        DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsThree();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                MetricOutputMetadata.of( input.getRawData().size(),
                                                   MeasurementUnit.of(),
                                                   MeasurementUnit.of(),
                                                   MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                   MetricConstants.MAIN,
                                                   DatasetIdentifier.of( Location.of( "Tampere" ),
                                                                                         "MAP",
                                                                                         "FMI" ) );

        //Check the results       
        final MultiVectorOutput actual = roc.apply( input );
        double[] expectedPOD = new double[] { 0.0, 0.13580246913580246, 0.2345679012345679, 0.43209876543209874,
                                              0.6296296296296297, 0.7037037037037037, 0.8024691358024691,
                                              0.8518518518518519, 0.9135802469135802,
                                              0.9753086419753086, 1.0 };
        double[] expectedPOFD = new double[] { 0.0, 0.007518796992481203, 0.018796992481203006, 0.04887218045112782,
                                               0.11654135338345864, 0.17669172932330826, 0.22932330827067668,
                                               0.2857142857142857, 0.42105263157894735,
                                               0.6240601503759399, 1.0 };
        Map<MetricDimension, double[]> output = new HashMap<>();
        output.put( MetricDimension.PROBABILITY_OF_DETECTION, expectedPOD );
        output.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, expectedPOFD );
        final MultiVectorOutput expected = MultiVectorOutput.ofMultiVectorOutput( output, m1 );
        assertTrue( "Difference between actual and expected ROC.", actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link RelativeOperatingCharacteristicDiagram#apply(DiscreteProbabilityPairs)} when 
     * supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                DiscreteProbabilityPairs.of( Arrays.asList(), Metadata.of() );

        MultiVectorOutput actual = roc.apply( input );

        double[] source = new double[11];

        Arrays.fill( source, Double.NaN );

        assertTrue( Arrays.equals( actual.getData()
                                         .get( MetricDimension.PROBABILITY_OF_DETECTION )
                                         .getDoubles(),
                                   source ) );

        assertTrue( Arrays.equals( actual.getData()
                                         .get( MetricDimension.PROBABILITY_OF_FALSE_DETECTION )
                                         .getDoubles(),
                                   source ) );
    }

    /**
     * Checks that the {@link RelativeOperatingCharacteristicDiagram#getName()} returns 
     * {@link MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( roc.getName().equals( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM.toString() ) );
    }

    /**
     * Tests for an expected exception on calling 
     * {@link RelativeOperatingCharacteristicDiagram#apply(DiscreteProbabilityPairs)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'RELATIVE OPERATING CHARACTERISTIC DIAGRAM'." );

        roc.apply( null );
    }

}
