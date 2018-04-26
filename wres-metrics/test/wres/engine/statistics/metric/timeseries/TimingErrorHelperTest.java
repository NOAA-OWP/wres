package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link TimingErrorHelper}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimingErrorHelperTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link TimingErrorHelper#getTimeToPeak(wres.datamodel.time.TimeSeries, java.util.Random) with data
     * that does not contain ties.
     */

    @Test
    public void testGetTimeToPeakWithoutTies()
    {

        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        Iterator<TimeSeries<PairOfDoubles>> iterator = input.basisTimeIterator().iterator();
        TimeSeries<PairOfDoubles> first = iterator.next();
        TimeSeries<PairOfDoubles> second = iterator.next();

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

    /**
     * Tests the {@link TimingErrorHelper#getTimeToPeak(wres.datamodel.time.TimeSeries, java.util.Random) with data
     * that contains ties.
     */

    @Test
    public void testGetTimeToPeakWithTies()
    {
        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFive();

        //Generate some data using an RNG for a uniform U[0,1] distribution with a fixed seed
        Random r = new Random( 12345678 );

        Pair<Instant, Instant> actualPeaks = TimingErrorHelper.getTimeToPeak( input, r );

        Pair<Instant, Instant> expectedPeaks =
                Pair.of( Instant.parse( "1985-01-02T06:00:00Z" ), Instant.parse( "1985-01-02T18:00:00Z" ) );

        assertTrue( actualPeaks.equals( expectedPeaks ) );
    }

    /**
     * Tests instantiation by reflection. TODO: remove when Jacoco can cover private constructors.
     * 
     * @throws SecurityException 
     * @throws NoSuchMethodException 
     * @throws InvocationTargetException 
     * @throws IllegalArgumentException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */

    @Test
    public void testInstantiation() throws NoSuchMethodException, SecurityException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
        final Constructor<TimingErrorHelper> constructor = TimingErrorHelper.class.getDeclaredConstructor();
        constructor.setAccessible( true );
        constructor.newInstance();
    }

}
