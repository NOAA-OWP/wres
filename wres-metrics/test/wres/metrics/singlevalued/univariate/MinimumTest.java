package wres.metrics.singlevalued.univariate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link Minimum}.
 * 
 * @author James Brown
 */
public final class MinimumTest
{

    /**Default instance of a {@link Minimum}.*/

    private Minimum minimum;

    /**Metric description.*/
    private DoubleScoreMetric metricDescription;

    /**Template for the l/r/b components of the score.*/
    private DoubleScoreMetricComponent template;

    @Before
    public void setupBeforeEachTest()
    {
        this.minimum = Minimum.of();
        this.metricDescription = DoubleScoreMetric.newBuilder()
                                                  .setName( MetricName.MINIMUM )
                                                  .build();
        this.template = DoubleScoreMetricComponent.newBuilder()
                                                  .setMinimum( Double.NEGATIVE_INFINITY )
                                                  .setMaximum( Double.POSITIVE_INFINITY )
                                                  .setOptimum( Double.NaN )
                                                  .build();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.minimum.apply( input );

        DoubleScoreMetricComponent leftMetric = this.template.toBuilder()
                                                             .setUnits( input.getMetadata()
                                                                             .getMeasurementUnit()
                                                                             .toString() )
                                                             .setName( ComponentName.LEFT )
                                                             .build();

        DoubleScoreMetricComponent rightMetric = this.template.toBuilder()
                                                              .setUnits( input.getMetadata()
                                                                              .getMeasurementUnit()
                                                                              .toString() )
                                                              .setName( ComponentName.RIGHT )
                                                              .build();

        DoubleScoreStatisticComponent leftStatistic = DoubleScoreStatisticComponent.newBuilder()
                                                                                   .setMetric( leftMetric )
                                                                                   .setValue( 2.1 )
                                                                                   .build();

        DoubleScoreStatisticComponent rightStatistic = DoubleScoreStatisticComponent.newBuilder()
                                                                                    .setMetric( rightMetric )
                                                                                    .setValue( 2.0 )
                                                                                    .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( this.metricDescription )
                                                            .addStatistics( leftStatistic )
                                                            .addStatistics( rightStatistic )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.minimum.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.LEFT ).getData().getValue(), 0.0 );
        assertEquals( Double.NaN, actual.getComponent( MetricConstants.RIGHT ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MINIMUM.toString(), this.minimum.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.minimum.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.minimum.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.UNIVARIATE_STATISTIC, this.minimum.getScoreOutputGroup() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.minimum.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.minimum.getName() + "'.", actual.getMessage() );
    }

}
