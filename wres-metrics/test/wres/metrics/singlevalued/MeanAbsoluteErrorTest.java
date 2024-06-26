package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

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
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;

/**
 * Tests the {@link MeanAbsoluteError}.
 * 
 * @author James Brown
 */
public final class MeanAbsoluteErrorTest
{

    /**
     * Default instance of a {@link MeanAbsoluteError}.
     */

    private MeanAbsoluteError mae;

    @Before
    public void setupBeforeEachTest()
    {
        this.mae = MeanAbsoluteError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.mae.apply( input );

        DoubleScoreMetricComponent metricComponent = MeanAbsoluteError.METRIC_INNER.getComponents( 0 )
                                                                                   .toBuilder()
                                                                                   .setUnits( input.getMetadata()
                                                                                                   .getMeasurementUnit()
                                                                                                   .toString() )
                                                                                   .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 201.37 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( DoubleScoreMetric.newBuilder()
                                                                                         .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = mae.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MEAN_ABSOLUTE_ERROR.toString(), this.mae.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.mae.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.mae.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.mae.getScoreOutputGroup() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.mae.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.mae.getMetricNameString() + "'.", actual.getMessage() );
    }

}
