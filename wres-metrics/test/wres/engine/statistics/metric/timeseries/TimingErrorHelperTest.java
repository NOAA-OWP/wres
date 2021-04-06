package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.pools.pairs.PoolOfPairs;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link TimingErrorHelper}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimingErrorHelperTest
{

    @Test
    public void testGetTimeToPeakWithoutTies()
    {

        // Generate some data
        PoolOfPairs<Double,Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        Iterator<TimeSeries<Pair<Double,Double>>> iterator = input.get().iterator();
        TimeSeries<Pair<Double,Double>> first = iterator.next();
        TimeSeries<Pair<Double,Double>> second = iterator.next();

        //Generate some data using an RNG for a uniform U[0,1] distribution with a fixed seed
        Random r = new Random( 12345678 );

        Pair<Instant, Instant> actualPeaksFirst = TimingErrorHelper.getTimeToPeak( first, r );

        Pair<Instant, Instant> expectedPeaksFirst =
                Pair.of( Instant.parse( "1985-01-01T18:00:00Z" ), Instant.parse( "1985-01-01T12:00:00Z" ) );

        assertTrue( actualPeaksFirst.equals( expectedPeaksFirst ) );

        Pair<Instant, Instant> actualPeaksSecond = TimingErrorHelper.getTimeToPeak( second, r );

        Pair<Instant, Instant> expectedPeaksSecond =
                Pair.of( Instant.parse( "1985-01-02T06:00:00Z" ), Instant.parse( "1985-01-02T18:00:00Z" ) );

        assertTrue( actualPeaksSecond.equals( expectedPeaksSecond ) );
    }

    @Test
    public void testGetTimeToPeakWithTies()
    {
        // Generate some data
        PoolOfPairs<Double,Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFive();
        Iterator<TimeSeries<Pair<Double,Double>>> iterator = input.get().iterator();
        TimeSeries<Pair<Double,Double>> first = iterator.next();

        //Generate some data using an RNG for a uniform U[0,1] distribution with a fixed seed
        Random r = new Random( 12345678 );

        Pair<Instant, Instant> actualPeaks = TimingErrorHelper.getTimeToPeak( first, r );

        Pair<Instant, Instant> expectedPeaks =
                Pair.of( Instant.parse( "1985-01-02T06:00:00Z" ), Instant.parse( "1985-01-02T18:00:00Z" ) );

        assertTrue( actualPeaks.equals( expectedPeaks ) );
    }

    @Test
    public void testInstantiation() throws NoSuchMethodException, InstantiationException,
            IllegalAccessException, InvocationTargetException
    {
        final Constructor<TimingErrorHelper> constructor = TimingErrorHelper.class.getDeclaredConstructor();
        constructor.setAccessible( true );
        constructor.newInstance();
    }

}
