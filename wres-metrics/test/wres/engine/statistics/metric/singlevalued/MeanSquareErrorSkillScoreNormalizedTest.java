package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

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

        //Check the results
        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                               .setValue( 0.8338214896144127 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScoreNormalized.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithoutBaseline() throws IOException
    {
        //Generate some data
        SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                               .setValue( 0.82188122037703356 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScoreNormalized.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithoutBaselineTwo()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                               .setValue( 0.9963778833284114 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareErrorSkillScoreNormalized.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
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
