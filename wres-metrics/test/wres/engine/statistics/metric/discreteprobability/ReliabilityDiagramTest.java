package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPair;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs;
import wres.datamodel.statistics.MultiVectorStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

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

    @Before
    public void setupBeforeEachTest()
    {
        this.rel = ReliabilityDiagram.of();
    }

    /**
     * Compares the output from {@link ReliabilityDiagram#apply(DiscreteProbabilityPairs)} against 
     * expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsThree();

        //Metadata for the output
        final StatisticMetadata m1 =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(),
                                                         DatasetIdentifier.of( Location.of( "Tampere" ),
                                                                               "MAP",
                                                                               "FMI" ) ),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.RELIABILITY_DIAGRAM,
                                      MetricConstants.MAIN );

        //Check the results       
        final MultiVectorStatistic actual = rel.apply( input );
        double[] expectedFProb = new double[] { 0.05490196078431369, 0.19999999999999984, 0.3000000000000002,
                                                0.40000000000000013, 0.5, 0.5999999999999998, 0.6999999999999996,
                                                0.8000000000000003, 0.9000000000000002,
                                                1.0 };
        double[] expectedOProb = new double[] { 0.0196078431372549, 0.0847457627118644, 0.12195121951219512,
                                                0.21052631578947367, 0.36363636363636365, 0.2727272727272727,
                                                0.47058823529411764, 0.6666666666666666,
                                                0.7272727272727273, 0.8461538461538461 };
        double[] expectedSample = new double[] { 102.0, 59.0, 41.0, 19.0, 22.0, 22.0, 34.0, 24.0, 11.0, 13.0 };

        Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.FORECAST_PROBABILITY, expectedFProb );
        output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, expectedOProb );
        output.put( MetricDimension.SAMPLE_SIZE, expectedSample );

        final MultiVectorStatistic expected = MultiVectorStatistic.ofMultiVectorOutput( output, m1 );

        assertTrue( "Difference between actual and expected Reliability Diagram.", actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link ReliabilityDiagram#apply(DiscreteProbabilityPairs)} against 
     * expected output for a scenario involving some bins with zero samples. See ticket #51362.
     */

    @Test
    public void testApplySomeBinsHaveZeroSamples()
    {
        //Generate some data
        List<DiscreteProbabilityPair> data = new ArrayList<>();
        data.add( DiscreteProbabilityPair.of( 1.0, 0.8775510204081632 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.6326530612244898 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.8163265306122449 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.9591836734693877 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.8979591836734694 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.9795918367346939 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.9183673469387755 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.8163265306122449 ) );
        data.add( DiscreteProbabilityPair.of( 1.0, 0.7755102040816326 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.3469387755102041 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.24489795918367346 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.20408163265306123 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.10204081632653061 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.08163265306122448 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.12244897959183673 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.0 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.0 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.0 ) );
        data.add( DiscreteProbabilityPair.of( 0.0, 0.0 ) );

        DatasetIdentifier identifier =
                DatasetIdentifier.of( Location.of( "FAKE" ), "MAP", "FK" );

        DiscreteProbabilityPairs input =
                DiscreteProbabilityPairs.of( data,
                                                        SampleMetadata.of( MeasurementUnit.of(),
                                                                                     identifier ) );

        //Metadata for the output
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of(), identifier ),
                                                           input.getRawData().size(),
                                                           MeasurementUnit.of(),
                                                           MetricConstants.RELIABILITY_DIAGRAM,
                                                           MetricConstants.MAIN );

        //Check the results       
        final MultiVectorStatistic actual = rel.apply( input );
        double[] expectedFProb = new double[] { 0.013605442176870748, 0.11224489795918367, 0.22448979591836735,
                                                0.3469387755102041, Double.NaN, Double.NaN, 0.6326530612244898,
                                                0.7755102040816326, 0.8520408163265306, 0.989010989010989 };
        double[] expectedOProb =
                new double[] { 0.16666666666666666, 0.0, 0.0, 0.0, Double.NaN, Double.NaN, 1.0, 1.0, 1.0, 1.0 };
        double[] expectedSample = new double[] { 6.0, 2.0, 2.0, 1.0, 0.0, 0.0, 1.0, 1.0, 4.0, 13.0 };

        Map<MetricDimension, double[]> output = new EnumMap<>( MetricDimension.class );
        output.put( MetricDimension.FORECAST_PROBABILITY, expectedFProb );
        output.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY, expectedOProb );
        output.put( MetricDimension.SAMPLE_SIZE, expectedSample );

        final MultiVectorStatistic expected = MultiVectorStatistic.ofMultiVectorOutput( output, m1 );

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
                DiscreteProbabilityPairs.of( Arrays.asList(), SampleMetadata.of() );

        MultiVectorStatistic actual = rel.apply( input );

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
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'RELIABILITY DIAGRAM'." );

        rel.apply( null );
    }

}
