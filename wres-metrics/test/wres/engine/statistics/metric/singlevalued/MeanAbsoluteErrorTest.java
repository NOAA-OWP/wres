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
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;

/**
 * Tests the {@link MeanAbsoluteError}.
 * 
 * @author james.brown@hydrosolved.com
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

        DoubleScoreMetricComponent metricComponent = MeanAbsoluteError.METRIC.getComponents( 0 )
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

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        BasicPool<Pair<Double, Double>> input =
                BasicPool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = mae.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( mae.getName().equals( MetricConstants.MEAN_ABSOLUTE_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( mae.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( mae.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( mae.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.mae.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.mae.getName() + "'.", actual.getMessage() );
    }

}
