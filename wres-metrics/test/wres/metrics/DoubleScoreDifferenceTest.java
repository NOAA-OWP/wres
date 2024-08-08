package wres.metrics;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.singlevalued.BiasFraction;
import wres.metrics.singlevalued.CorrelationPearsons;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * Tests the {@link BiasFraction}.
 *
 * @author James Brown
 */
final class DoubleScoreDifferenceTest
{
    /** Default instance of a {@link DoubleScoreDifference}. */

    private DoubleScoreDifference<Pair<Double, Double>, Pool<Pair<Double, Double>>> score;

    @BeforeEach
    public void setupBeforeEachTest()
    {
        this.score = DoubleScoreDifference.of( CorrelationPearsons.of() );
    }

    @Test
    void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        DoubleScoreStatisticOuter actual = this.score.apply( input );

        DoubleScoreMetric.DoubleScoreMetricComponent metricComponent
                = CorrelationPearsons.MAIN.toBuilder()
                                          .setMinimum( MetricConstants.PEARSON_CORRELATION_COEFFICIENT_DIFFERENCE.getMinimum() )
                                          .setMaximum( MetricConstants.PEARSON_CORRELATION_COEFFICIENT_DIFFERENCE.getMaximum() )
                                          .setOptimum( MetricConstants.PEARSON_CORRELATION_COEFFICIENT_DIFFERENCE.getOptimum() )
                                          .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 1.2500146073790575E-7 )
                                                                               .build();

        DoubleScoreMetric metric = CorrelationPearsons.BASIC_METRIC.toBuilder()
                                                                   .setName( MetricName.PEARSON_CORRELATION_COEFFICIENT_DIFFERENCE )
                                                                   .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( metric )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    void testGetName()
    {
        assertEquals( "PEARSON CORRELATION COEFFICIENT DIFFERENCE", this.score.getMetricNameString() );
    }

    @Test
    void testIsDecomposable()
    {
        assertFalse( this.score.isDecomposable() );
    }

    @Test
    void testIsSkillScore()
    {
        assertTrue( this.score.isSkillScore() );
    }

    @Test
    void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.score.getScoreOutputGroup() );
    }

    @Test
    void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.score.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.score.getMetricNameString() + "'.",
                      actual.getMessage() );
    }
}
