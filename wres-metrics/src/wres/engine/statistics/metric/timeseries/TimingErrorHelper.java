package wres.engine.statistics.metric.timeseries;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * A helper class for computing timing errors.
 * 
 * @author james.brown@hydrosolved.com
 */
class TimingErrorHelper
{

    /**
     * Returns the time at which the maximum value occurs on each side of the input. When the maximum value occurs at 
     * more than one time, ties are resolved by randomly selecting from the tied times.
     * 
     * @param timeSeries the time-series input
     * @param rng a random number generator to use in resolving ties
     * @return the time at which the peak occurs on the left and right, respectively
     * @throws NullPointerException if the input is null
     */

    static Pair<Instant, Instant> getTimeToPeak( final TimeSeries<PairOfDoubles> timeSeries, final Random rng )
    {
        Objects.requireNonNull( timeSeries, "Specify a non-null time-series whose time-to-peak error is required." );

        Instant peakLeftTime = null;
        Instant peakRightTime = null;
        List<Instant> tiesLeft = new ArrayList<>();
        List<Instant> tiesRight = new ArrayList<>();

        double peakLeftValue = Double.NEGATIVE_INFINITY;
        double peakRightValue = Double.NEGATIVE_INFINITY;

        // Iterate through the pairs to find the peak on each side
        for ( Event<PairOfDoubles> nextPair : timeSeries.timeIterator() )
        {
            // New peak left
            if ( Double.compare( nextPair.getValue().getItemOne(), peakLeftValue ) > 0 )
            {
                peakLeftValue = nextPair.getValue().getItemOne();
                peakLeftTime = nextPair.getTime();

                // Reset left ties
                tiesLeft.clear();
            }
            // New tie left
            else if ( Double.compare( nextPair.getValue().getItemOne(), peakLeftValue ) == 0 )
            {
                tiesLeft.add( nextPair.getTime() );
            }

            // New peak right
            if ( Double.compare( nextPair.getValue().getItemTwo(), peakRightValue ) > 0 )
            {
                peakRightValue = nextPair.getValue().getItemTwo();
                peakRightTime = nextPair.getTime();

                // Reset tight ties
                tiesRight.clear();
            }
            else if ( Double.compare( nextPair.getValue().getItemTwo(), peakRightValue ) == 0 )
            {
                tiesRight.add( nextPair.getTime() );
            }
        }

        // Resolve any ties
        if ( !tiesLeft.isEmpty() )
        {
            // Add the current (first) time at which the tie was recorded 
            tiesLeft.add( peakLeftTime );

            // Resolve
            peakLeftTime = TimingErrorHelper.resolveTiesByRandomSelection( tiesLeft, rng );
        }

        if ( !tiesRight.isEmpty() )
        {
            // Add the current (first) time at which the tie was recorded 
            tiesRight.add( peakRightTime );

            // Resolve
            peakRightTime = TimingErrorHelper.resolveTiesByRandomSelection( tiesRight, rng );
        }

        return Pair.of( peakLeftTime, peakRightTime );
    }

    /**
     * Resolves ties between times by randomly selecting one time from the input.
     * 
     * @param tiedTimes a collection of tied times
     * @param rng a random number generator used to resolve ties
     * @returned a randomly selected time from the input
     * @throws NullPointerException if any input is null or any of the tied times is null
     */

    private static Instant resolveTiesByRandomSelection( List<Instant> tiedTimes, Random rng )
    {
        Objects.requireNonNull( tiedTimes, "Cannot resolve ties for null input." );

        Objects.requireNonNull( rng, "The random number generator used to resolve ties cannot be null." );

        return tiedTimes.get( rng.nextInt( tiedTimes.size() ) );
    }

    /**
     * Do not construct.
     */

    private TimingErrorHelper()
    {
    }

}
