package wres.engine.statistics.metric.singlevalued.univariate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link Mean}.
 * 
 * @author james.brown@hydrosolved.com
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

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        BasicPool<Pair<Double, Double>> input =
                BasicPool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.mean.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.LEFT ).getData().getValue(), 0.0 );
        assertEquals( Double.NaN, actual.getComponent( MetricConstants.RIGHT ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.mean.getName().equals( MetricConstants.MEAN.toString() ) );
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
        assertTrue( this.mean.getScoreOutputGroup() == MetricGroup.UNIVARIATE_STATISTIC );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.mean.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.mean.getName() + "'.", actual.getMessage() );
    }

}
