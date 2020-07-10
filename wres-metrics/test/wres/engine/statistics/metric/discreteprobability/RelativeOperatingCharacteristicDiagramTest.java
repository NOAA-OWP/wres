package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Probability;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
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
     * Compares the output from {@link RelativeOperatingCharacteristicDiagram#apply(SampleData)} against 
     * expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SampleData<Pair<Probability,Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsThree();

        //Metadata for the output
        final StatisticMetadata m1 =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "Tampere" ),
                                                                               "MAP",
                                                                               "FMI" ) ),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                      MetricConstants.MAIN );

        //Check the results       
        DiagramStatisticOuter actual = this.roc.apply( input );
        
        VectorOfDoubles expectedPoD = VectorOfDoubles.of( 0.0,
                                                          0.13580246913580246,
                                                          0.2345679012345679,
                                                          0.43209876543209874,
                                                          0.6296296296296297,
                                                          0.7037037037037037,
                                                          0.8024691358024691,
                                                          0.8518518518518519,
                                                          0.9135802469135802,
                                                          0.9753086419753086,
                                                          1.0 );
        VectorOfDoubles expectedPoFD = VectorOfDoubles.of( 0.0,
                                                           0.007518796992481203,
                                                           0.018796992481203006,
                                                           0.04887218045112782,
                                                           0.11654135338345864,
                                                           0.17669172932330826,
                                                           0.22932330827067668,
                                                           0.2857142857142857,
                                                           0.42105263157894735,
                                                           0.6240601503759399,
                                                           1.0 );
        Map<MetricDimension, VectorOfDoubles> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.PROBABILITY_OF_DETECTION, expectedPoD );
        output.put( MetricDimension.PROBABILITY_OF_FALSE_DETECTION, expectedPoFD );
        
        DiagramStatisticOuter expected = DiagramStatisticOuter.of( output, m1 );
        
        assertEquals( expected, actual );
    }

    /**
     * Validates the output from {@link RelativeOperatingCharacteristicDiagram#apply(SampleData)} when 
     * supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleData<Pair<Probability,Probability>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DiagramStatisticOuter actual = roc.apply( input );

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
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'RELATIVE OPERATING CHARACTERISTIC DIAGRAM'." );

        roc.apply( null );
    }

}
