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
 * Tests the {@link MeanSquareErrorSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanSquareErrorSkillScoreTest
{
    
    /**
     * Default instance of a {@link MeanSquareErrorSkillScore}.
     */

    private MeanSquareErrorSkillScore msess;

    @Before
    public void setupBeforeEachTest()
    {
        this.msess = MeanSquareErrorSkillScore.of();
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
                                                           MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                           MetricConstants.MAIN );

        //Check the results
        final DoubleScoreStatistic actual = msess.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.8007025335093799, m1 );
        assertEquals( expected, actual );
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
                                      MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                      MetricConstants.MAIN );

        //Check the results
        DoubleScoreStatistic actual = msess.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.7832791707526114, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
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
                                                     MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                     MetricConstants.MAIN );

        //Check the results
        DoubleScoreStatistic actual = msess.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.9963647159052861, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = msess.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertTrue( msess.getName().equals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( msess.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( msess.isSkillScore() );
    }
    
    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( msess.getScoreOutputGroup() == MetricGroup.NONE );
    }
    
    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE, this.msess.getCollectionOf() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        SampleDataException expected = assertThrows( SampleDataException.class, () -> this.msess.apply( null ) );

        assertEquals( "Specify non-null input to the 'MEAN SQUARE ERROR SKILL SCORE'.", expected.getMessage() );
    }

}
