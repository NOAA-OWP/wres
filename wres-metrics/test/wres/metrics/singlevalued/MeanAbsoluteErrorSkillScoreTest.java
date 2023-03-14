package wres.metrics.singlevalued;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.pools.Pool;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MeanAbsoluteErrorSkillScore}.
 * 
 * @author James Brown
 */
final class MeanAbsoluteErrorSkillScoreTest
{
    /** Score used for testing. */
    private MeanAbsoluteErrorSkillScore maess;

    @BeforeEach
    void setUpBeforeEachTest()
    {
        this.maess = MeanAbsoluteErrorSkillScore.of();
    }

    @Test
    void testApplyWithDefaultBaseline()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.maess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanAbsoluteErrorSkillScore.MAIN )
                                                                               .setValue( 0.9680049801455148 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanAbsoluteErrorSkillScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    void testApplyWithExplicitBaseline()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        DoubleScoreStatisticOuter actual = this.maess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanAbsoluteErrorSkillScore.MAIN )
                                                                               .setValue( 0.5543728423475258 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanAbsoluteErrorSkillScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input = Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.maess.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    void testMetricIsNamedCorrectly()
    {
        assertEquals( MetricConstants.MEAN_ABSOLUTE_ERROR_SKILL_SCORE.toString(), this.maess.getName() );
    }

    @Test
    void testMetricIsNotDecoposable()
    {
        assertFalse( this.maess.isDecomposable() );
    }

    @Test
    void testMetricIsASkillScore()
    {
        assertTrue( this.maess.isSkillScore() );
    }

    @Test
    void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.maess.getScoreOutputGroup() );
    }

    @Test
    void testGetCollectionOf()
    {
        assertSame( MetricConstants.MEAN_ABSOLUTE_ERROR, this.maess.getCollectionOf() );
    }

    @Test
    void testMetricBoundsAreCorrect()
    {
        Assertions.assertAll(
                              () -> assertEquals( Double.NEGATIVE_INFINITY,
                                                  MeanAbsoluteErrorSkillScore.MAIN.getMinimum(),
                                                  0.0 ),
                              () -> assertEquals( 1.0, MeanAbsoluteErrorSkillScore.MAIN.getMaximum(), 0.0 ),
                              () -> assertEquals( 1.0, MeanAbsoluteErrorSkillScore.MAIN.getOptimum(), 0.0 ) );
    }

    @Test
    void testExceptionOnNullInput()
    {
        MetricCalculationException actual = assertThrows( MetricCalculationException.class,
                                                          () -> this.maess.aggregate( (DoubleScoreStatisticOuter) null,
                                                                                    null ) );

        assertEquals( "Specify a non-null statistic for the '" + this.maess.getName() + "'.", actual.getMessage() );
    }
}
