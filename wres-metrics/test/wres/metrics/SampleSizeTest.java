package wres.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link SampleSize}.
 * 
 * @author James Brown
 */
public final class SampleSizeTest
{

    /**
     * Constructs a {@link SampleSize} and compares the actual result to the expected result. Also, checks the 
     * parameters of the metric.
     */

    @Test
    public void testSampleSize()
    {
        //Obtain the factories

        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Build the metric
        SampleSize<Pool<Pair<Double, Double>>> ss = SampleSize.of();

        //Check the results
        DoubleScoreStatisticOuter actual = ss.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( SampleSize.MAIN )
                                                                               .setValue( input.get().size() )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( SampleSize.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, PoolMetadata.of() );

        assertEquals( expected, actual );

        //Check the parameters
        assertEquals( MetricConstants.SAMPLE_SIZE.toString(), ss.getName() );
        assertFalse( ss.isDecomposable() );
        assertFalse( ss.isSkillScore() );
        assertSame( MetricGroup.NONE, ss.getScoreOutputGroup() );
    }

    /**
     * Constructs a {@link SampleSize} and checks for exceptional cases.
     */

    @Test
    public void testExceptions()
    {
        //Build the metric
        SampleSize<Pool<Pair<Double, Double>>> ss = SampleSize.of();

        PoolException expected = assertThrows( PoolException.class, () -> ss.apply( null ) );

        assertEquals( "Specify non-null input to the 'SAMPLE SIZE'.", expected.getMessage() );
    }


}
