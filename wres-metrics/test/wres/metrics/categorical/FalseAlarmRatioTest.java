package wres.metrics.categorical;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.datamodel.pools.Pool;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Boilerplate;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link FalseAlarmRatio}.
 *
 * @author James Brown
 */
final class FalseAlarmRatioTest
{
    /** Score used for testing. */
    private FalseAlarmRatio far;

    /** Metadata used for testing. */
    private PoolMetadata meta;

    @BeforeEach
    void setUpBeforeEachTest()
    {
        this.far = FalseAlarmRatio.of();
        this.meta = Boilerplate.getPoolMetadata( false );
    }

    @Test
    void testApply()
    {
        //Generate some data
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.far.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( FalseAlarmRatio.MAIN )
                                                                               .setValue( 0.31666666666666665 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( FalseAlarmRatio.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, this.meta );

        assertEquals( expected, actual );
    }

    @Test
    void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Boolean, Boolean>> input = Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.far.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    void testMetricIsNamedCorrectly()
    {
        assertEquals( MetricConstants.FALSE_ALARM_RATIO.toString(), this.far.getMetricNameString() );
    }

    @Test
    void testMetricIsNotDecoposable()
    {
        assertFalse( this.far.isDecomposable() );
    }

    @Test
    void testMetricIsASkillScore()
    {
        assertFalse( this.far.isSkillScore() );
    }

    @Test
    void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.far.getScoreOutputGroup() );
    }

    @Test
    void testGetCollectionOf()
    {
        assertSame( MetricConstants.CONTINGENCY_TABLE, this.far.getCollectionOf() );
    }

    @Test
    void testMetricBoundsAreCorrect()
    {
        Assertions.assertAll(
                () -> assertEquals( 0.0, FalseAlarmRatio.MAIN.getMinimum(), 0.0 ),
                () -> assertEquals( 1.0, FalseAlarmRatio.MAIN.getMaximum(), 0.0 ),
                () -> assertEquals( 0.0, FalseAlarmRatio.MAIN.getOptimum(), 0.0 ) );
    }

    @Test
    void testExceptionOnNullInput()
    {
        MetricCalculationException actual = assertThrows( MetricCalculationException.class,
                                                          () -> this.far.applyIntermediate( null, null ) );

        assertEquals( "Specify non-null input to the '" + this.far.getMetricNameString() + "'.", actual.getMessage() );
    }
}
