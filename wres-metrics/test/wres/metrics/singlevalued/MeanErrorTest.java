package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

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
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MeanError}.
 * 
 * @author James Brown
 */
public final class MeanErrorTest
{
    
    /**
     * Default instance of a {@link MeanError}.
     */

    private MeanError meanError;

    @Before
    public void setupBeforeEachTest()
    {
        this.meanError = MeanError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.meanError.apply( input );

        DoubleScoreMetricComponent metricComponent = MeanError.METRIC_INNER.getComponents( 0 )
                                                                     .toBuilder()
                                                                     .setUnits( input.getMetadata()
                                                                                     .getMeasurementUnit()
                                                                                     .toString() )
                                                                     .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 200.55 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( DoubleScoreMetric.newBuilder()
                                                                                      .setName( MetricName.MEAN_ERROR ) )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.meanError.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MEAN_ERROR.toString(), this.meanError.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.meanError.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.meanError.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.meanError.getScoreOutputGroup() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.meanError.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.meanError.getName() + "'.", actual.getMessage() );
    }

}
