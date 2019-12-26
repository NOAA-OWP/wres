package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link RootMeanSquareErrorNormalized}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RootMeanSquareErrorNormalizedTest
{

    /**
     * Default instance of a {@link RootMeanSquareErrorNormalized}.
     */

    private RootMeanSquareErrorNormalized rmsen;

    @Before
    public void setupBeforeEachTest()
    {
        this.rmsen = RootMeanSquareErrorNormalized.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.ROOT_MEAN_SQUARE_ERROR_NORMALIZED,
                                                     MetricConstants.MAIN );
        //Check the results
        DoubleScoreStatistic actual = this.rmsen.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.05719926297814069, m1 );
        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = this.rmsen.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.ROOT_MEAN_SQUARE_ERROR_NORMALIZED.toString(), this.rmsen.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.rmsen.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.rmsen.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.rmsen.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    @Test
    public void testHasRealUnits()
    {
        assertFalse( this.rmsen.hasRealUnits() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        SampleDataException expected = assertThrows( SampleDataException.class, () -> this.rmsen.apply( null ) );

        assertEquals( "Specify non-null input to the 'ROOT MEAN SQUARE ERROR NORMALIZED'.", expected.getMessage() );
    }

}
