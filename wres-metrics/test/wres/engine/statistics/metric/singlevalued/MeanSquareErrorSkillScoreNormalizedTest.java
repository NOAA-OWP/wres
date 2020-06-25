package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MeanSquareErrorSkillScoreNormalized}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanSquareErrorSkillScoreNormalizedTest
{

    /**
     * Default instance of a {@link MeanSquareErrorSkillScoreNormalized}.
     */

    private MeanSquareErrorSkillScoreNormalized msessn;

    @Before
    public void setupBeforeEachTest()
    {
        this.msessn = MeanSquareErrorSkillScoreNormalized.of();
    }

    @Test
    public void testApplyWithBaseline()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Metadata for the output
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( MetricTestDataFactory.getLocation( "DRRC2" ),
                                                                                                    "SQIN",
                                                                                                    "HEFS",
                                                                                                    "ESP" ) ),
                                                           input.getRawData().size(),
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED,
                                                           MetricConstants.MAIN );

        //Check the results
        final DoubleScoreStatistic actual = this.msessn.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.8338214896144127, m1 );
        assertEquals( expected.getData(), actual.getData() );
    }

    @Test
    public void testApplyWithoutBaseline() throws IOException
    {
        //Generate some data
        SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           Duration.ofHours( 24 ) );
        final TimeWindow timeWindow = window;

        final StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "MM/DAY" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( MetricTestDataFactory.getLocation( "103.1" ),
                                                                                                       "QME",
                                                                                                       "NVE" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.getRawData().size(),
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED,
                                      MetricConstants.MAIN );

        //Check the results
        DoubleScoreStatistic actual = this.msessn.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.82188122037703356, m1 );
        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithoutBaselineTwo()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED,
                                                     MetricConstants.MAIN );

        //Check the results
        DoubleScoreStatistic actual = this.msessn.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.9963778833284114, m1 );
        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = this.msessn.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.toString(), this.msessn.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.msessn.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( msessn.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( msessn.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE, this.msessn.getCollectionOf() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        SampleDataException expected = assertThrows( SampleDataException.class, () -> this.msessn.apply( null ) );

        assertEquals( "Specify non-null input to the 'MEAN SQUARE ERROR SKILL SCORE NORMALIZED'.",
                      expected.getMessage() );
    }

}
