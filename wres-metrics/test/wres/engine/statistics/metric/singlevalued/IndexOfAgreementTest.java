package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link IndexOfAgreement}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class IndexOfAgreementTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link IndexOfAgreement}.
     */

    private IndexOfAgreement ioa;

    @Before
    public void setupBeforeEachTest()
    {
        this.ioa = IndexOfAgreement.of();
    }

    @Test
    public void testApply() throws IOException
    {
        SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "2010-12-31T11:59:59Z" ),
                                                     Duration.ofHours( 24 ) );

        SampleMetadata m1 = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "MM/DAY" ) )
                                                   .setIdentifier( DatasetIdentifier.of( MetricTestDataFactory.getLocation( "103.1" ),
                                                                                         "QME",
                                                                                         "NVE" ) )
                                                   .setTimeWindow( window )
                                                   .build();

        //Check the results
        DoubleScoreStatisticOuter actual = this.ioa.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( IndexOfAgreement.MAIN )
                                                                               .setValue( 0.8221179993380173 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( IndexOfAgreement.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = ioa.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( ioa.getName().equals( MetricConstants.INDEX_OF_AGREEMENT.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( ioa.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( ioa.isSkillScore() );
    }

    @Test
    public void testhasRealUnits()
    {
        assertFalse( ioa.hasRealUnits() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( ioa.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'INDEX OF AGREEMENT'." );

        ioa.apply( null );
    }

}
