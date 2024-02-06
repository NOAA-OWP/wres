package wres.metrics.singlevalued.univariate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link Mean}.
 * 
 * @author James Brown
 */
public final class MeanTest
{

    /**Default instance of a {@link Mean}.*/

    private Mean mean;

    /**Metric description.*/
    private DoubleScoreMetric metricDescription;

    /**Template for the l/r/b components of the score.*/
    private DoubleScoreMetricComponent template;

    @Before
    public void setupBeforeEachTest()
    {
        this.mean = Mean.of();
        this.metricDescription = DoubleScoreMetric.newBuilder()
                                                  .setName( MetricName.MEAN )
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
        DoubleScoreStatisticOuter actual = this.mean.apply( input );

        DoubleScoreMetricComponent leftMetric = this.template.toBuilder()
                                                             .setUnits( input.getMetadata()
                                                                             .getMeasurementUnit()
                                                                             .toString() )
                                                             .setName( MetricName.LEFT )
                                                             .build();

        DoubleScoreMetricComponent rightMetric = this.template.toBuilder()
                                                              .setUnits( input.getMetadata()
                                                                              .getMeasurementUnit()
                                                                              .toString() )
                                                              .setName( MetricName.RIGHT )
                                                              .build();

        DoubleScoreStatisticComponent leftStatistic = DoubleScoreStatisticComponent.newBuilder()
                                                                                   .setMetric( leftMetric )
                                                                                   .setValue( 3531.04 )
                                                                                   .build();

        DoubleScoreStatisticComponent rightStatistic = DoubleScoreStatisticComponent.newBuilder()
                                                                                    .setMetric( rightMetric )
                                                                                    .setValue( 3731.59 )
                                                                                    .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( this.metricDescription )
                                                            .addStatistics( leftStatistic )
                                                            .addStatistics( rightStatistic )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.mean.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.LEFT ).getStatistic().getValue(), 0.0 );
        assertEquals( Double.NaN, actual.getComponent( MetricConstants.RIGHT ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MEAN.toString(), this.mean.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.mean.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.mean.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.UNIVARIATE_STATISTIC, this.mean.getScoreOutputGroup() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.mean.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.mean.getMetricNameString() + "'.", actual.getMessage() );
    }

}
