package wres.metrics.ensemble;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.types.Ensemble;
import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;

/**
 * Tests the {@link RankHistogram}.
 *
 * @author James Brown
 */
public final class RankHistogramTest
{

    /**
     * Default instance of a {@link RankHistogram}.
     */

    private RankHistogram rh;

    /**
     * Instance of a random number generator.
     */

    private Random rng;

    @Before
    public void setupBeforeEachTest()
    {
        this.rng = new Random( 12345678 );
        this.rh = RankHistogram.of( this.rng );
    }

    /**
     * Compares the output from {@link RankHistogram#apply(Pool)} against expected output for pairs without
     * ties.
     */

    @Test
    public void testApplyWithoutTies()
    {
        List<Pair<Double, Ensemble>> values = new ArrayList<>();
        for ( int i = 0; i < 10000; i++ )
        {
            double left = rng.nextDouble();
            double[] right = new double[9];
            for ( int j = 0; j < 9; j++ )
            {
                right[j] = rng.nextDouble();
            }
            values.add( Pair.of( left, Ensemble.of( right ) ) );
        }

        Pool<Pair<Double, Ensemble>> input = Pool.of( values, PoolMetadata.of() );

        //Check the results       
        DiagramStatisticOuter actual = this.rh.apply( input );

        List<Double> expectedRanks = List.of( 1.0,
                                              2.0,
                                              3.0,
                                              4.0,
                                              5.0,
                                              6.0,
                                              7.0,
                                              8.0,
                                              9.0,
                                              10.0 );
        List<Double> expectedRFreqs = List.of( 0.0995,
                                               0.1041,
                                               0.0976,
                                               0.1041,
                                               0.0993,
                                               0.1044,
                                               0.1014,
                                               0.0952,
                                               0.0972,
                                               0.0972 );

        DiagramStatisticComponent ro =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.RANK_ORDER )
                                         .addAllValues( expectedRanks )
                                         .build();

        DiagramStatisticComponent obs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.OBSERVED_RELATIVE_FREQUENCY )
                                         .addAllValues( expectedRFreqs )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( ro )
                                                    .addStatistics( obs )
                                                    .setMetric( RankHistogram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithTies()
    {
        //Generate some data using an RNG for a uniform U[0,1] distribution with a fixed seed
        List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 2.0, Ensemble.of( 1, 2, 2, 2, 4, 5, 6, 7, 8 ) ) );
        Pool<Pair<Double, Ensemble>> input = Pool.of( values, PoolMetadata.of() );

        //Check the results       
        DiagramStatisticOuter actual = this.rh.apply( input );


        List<Double> expectedRanks = List.of( 1.0,
                                              2.0,
                                              3.0,
                                              4.0,
                                              5.0,
                                              6.0,
                                              7.0,
                                              8.0,
                                              9.0,
                                              10.0 );
        List<Double> expectedRFreqs = List.of( 0.0,
                                               0.0,
                                               0.0,
                                               1.0,
                                               0.0,
                                               0.0,
                                               0.0,
                                               0.0,
                                               0.0,
                                               0.0 );

        DiagramStatisticComponent ro =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.RANK_ORDER )
                                         .addAllValues( expectedRanks )
                                         .build();

        DiagramStatisticComponent obs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.OBSERVED_RELATIVE_FREQUENCY )
                                         .addAllValues( expectedRFreqs )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( ro )
                                                    .addStatistics( obs )
                                                    .setMetric( RankHistogram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Ensemble>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DiagramStatisticOuter actual = this.rh.apply( input );

        List<Double> source = Collections.emptyList();

        DiagramStatisticComponent ro =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.RANK_ORDER )
                                         .addAllValues( source )
                                         .build();

        DiagramStatisticComponent obs =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( RankHistogram.OBSERVED_RELATIVE_FREQUENCY )
                                         .addAllValues( source )
                                         .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( ro )
                                                    .addStatistics( obs )
                                                    .setMetric( RankHistogram.BASIC_METRIC )
                                                    .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.RANK_HISTOGRAM.toString(), this.rh.getMetricNameString() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.rh.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.rh.getMetricNameString() + "'.", actual.getMessage() );
    }

    @Test
    public void testConstructionWithoutRNG()
    {
        assertNotNull( RankHistogram.of() );
    }

}
