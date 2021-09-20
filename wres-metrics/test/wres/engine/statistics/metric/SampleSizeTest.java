package wres.engine.statistics.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
        assertTrue( "Unexpected name for the Sample Size.",
                    ss.getName().equals( MetricConstants.SAMPLE_SIZE.toString() ) );
        assertTrue( "The Sample Size is not decomposable.", !ss.isDecomposable() );
        assertTrue( "The Sample Size is not a skill score.", !ss.isSkillScore() );
        assertTrue( "The Sample Size cannot be decomposed.", ss.getScoreOutputGroup() == MetricGroup.NONE );
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
