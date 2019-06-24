package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link VolumetricEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class VolumetricEfficiencyTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link VolumetricEfficiency}.
     */

    private VolumetricEfficiency ve;

    @Before
    public void setupBeforeEachTest()
    {
        this.ve = VolumetricEfficiency.of();
    }

    /**
     * Compares the output from {@link VolumetricEfficiency#apply(SingleValuedPairs)} against expected output.
     * @throws IOException if the input data could not be read
     */

    @Test
    public void testApply() throws IOException
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 Duration.ofHours( 24 ) );

        final StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "MM/DAY" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "103.1" ),
                                                                                                       "QME",
                                                                                                       "NVE" ) )
                                                                 .setTimeWindow( window )
                                                                 .build(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.VOLUMETRIC_EFFICIENCY,
                                      MetricConstants.MAIN );
        //Check the results
        DoubleScoreStatistic actual = ve.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.657420176533252, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link VolumetricEfficiency#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = ve.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#getName()} returns 
     * {@link MetricConstants#VOLUMETRIC_EFFICIENCY.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( ve.getName().equals( MetricConstants.VOLUMETRIC_EFFICIENCY.toString() ) );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( ve.isDecomposable() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( ve.isSkillScore() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( ve.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link VolumetricEfficiency#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'VOLUMETRIC EFFICIENCY'." );

        ve.apply( null );
    }

}
