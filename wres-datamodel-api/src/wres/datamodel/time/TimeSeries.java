package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.function.Predicate;

import wres.datamodel.inputs.pairs.Pair;

/**
 * <p>A representation of one or more atomic time-series, each of which contains one or more {@link Pair} of times and 
 * values. Each value has a designated type, which may comprise a tuple or some other type. For example, a 
 * {@link TimeSeries} may store a single time-series of scalar observations or multiple time-series of paired forecasts 
 * and observations.</p> 
 * 
 * <p>In this context, a time is an instant on the UTC timeline, which is represented by an {@link Instant}. 
 * Each atomic time-series in the {@link TimeSeries} container is anchored to a specific basis time, which is also 
 * represented by an {@link Instant}. A {@link TimeSeries} can only contain one atomic time-series that originates at 
 * a particular {@link Instant}.</p>
 * 
 * <p>A {@link TimeSeries} may be regular or irregular. A {@link TimeSeries} is regular iif each time is separated by 
 * exactly the same {@link Duration}, there are no missing times between the earliest and latest times, and the number
 * of times in each atomic time-series is constant; by implication a regular {@link TimeSeries} cannot contain more
 * than one value with the same time and basis time. If the timeline is viewed as a function of unit time, a regular 
 * time-series is a linear function whose first derivative is the regular timestep.</p>
 * 
 * <p>A duration is defined as the {@link Duration} between the basis time and the (valid) time associated with a 
 * value. If {@link #isRegular()} returns <code>true</code>, the {@link #getRegularDuration()} corresponds to the 
 * first derivative of the timeline, i.e. the duration between successive times in the time-series.</p>
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>This class is immutable and thread-safe. For example, implementations of the methods that return {@link Iterable} 
 * views should not allow {@link Iterator#remove()} to remove an element from the underlying time-series.</p>
 * 
 * @param <T> the designated atomic type stored by this container
 * @version 0.1
 * @since 0.3
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeries<T>
{

    /**
     * Returns an {@link Iterator} over all the {@link Pair} of times and values in the {@link TimeSeries}. The times
     * are returned in a guaranteed order from the earliest time to the latest time.
     * 
     * @return iterable pairs of times and values
     */

    Iterable<Pair<Instant, T>> timeIterator();

    /**
     * Returns a basis-time view of the {@link TimeSeries} whereby each atomic time-series originates from one basis 
     * time and each element is associated with a separate {@link Duration}. The time-series are not returned in a 
     * guaranteed order.
     * 
     * @return an iterable atomic time-series by basis time
     */

    Iterable<TimeSeries<T>> basisTimeIterator();

    /**
     * Returns a duration view of the {@link TimeSeries} whereby each atomic time-series contains one {@link Duration}
     * and each element originates from a separate basis time. The atomic time-series are not returned in a 
     * guaranteed order.
     * 
     * @return an iterable atomic time-series by duration
     */

    Iterable<TimeSeries<T>> durationIterator();

    /**
     * Returns a {@link TimeSeries} whose elements are filtered according to duration or null if no such time-series 
     * exists. If the current time-series is regular, the returned time-series may be irregular, and vice versa, 
     * depending on the filter applied. 
     * 
     * @param duration the duration filter
     * @return a list of values with the same duration or null if no such duration exists
     */

    TimeSeries<T> filterByDuration( Predicate<Duration> duration );

    /**
     * Returns a {@link TimeSeries} whose elements are filtered according to basis time or null if no such time-series 
     * exists.
     * 
     * @param basisTime the basis time filter
     * @return a time-series associated with a specific basis time or null
     */

    TimeSeries<T> filterByBasisTime( Predicate<Instant> basisTime );

    /**
     * Returns the basis times associated with all the atomic time-series in the container. The results are ordered 
     * from the earliest basis time to the latest. If {@link #hasMultipleTimeSeries()} returns <code>false</code>, the
     * returned list will contain a single element, otherwise more than one element.
     * 
     * @return the basis times
     */

    SortedSet<Instant> getBasisTimes();

    /**
     * Returns the durations associated with all the atomic time-series in the container. The results are ordered 
     * from the earliest duration to the latest.
     * 
     * @return the basis times
     */

    SortedSet<Duration> getDurations();

    /**
     * Returns <code>true</code> if the {@link TimeSeries} contains multiple atomic time-series, each with a separate 
     * issue/basis time, <code>false</code> otherwise. Use {@link #basisTimeIterator()} to iterate over the atomic 
     * time-series.
     * 
     * @return true if the time-series contains more than one atomic time-series
     */

    boolean hasMultipleTimeSeries();

    /**
     * Returns true if the {@link TimeSeries} has a regular spacing of times with no missing times. In this context,
     * regularity applies to the time-step associated with each atomic time-series. Also see {#getRegularDuration()}.
     * 
     * @return true if the time-series is regular, false otherwise
     */

    boolean isRegular();

    /**
     * Returns the {@link Duration} between elements in a regular {@link TimeSeries} or null if this is an irregular 
     * {@link TimeSeries}. Also see {@link #isRegular()}. When {@link #isRegular()} returns <code>true</code>, all 
     * atomic time-series must have a constant {@link Duration} between times, as revealed by this method. However, 
     * when the container stores forecasts, the basis/issue time may not be regular and the time-step between 
     * basis/issue times may differ from the {@link #getRegularDuration}. For example, the container may store 
     * forecasts that are issued once per day with a regular time-step of 6 hours. If the timeline is viewed as a 
     * function of the unit duration, f(h)=6h, the first derivative of the timeline is 6h, and 
     * {@link #getRegularDuration} will return a {@link Duration} of 6h. 
     *  
     * 
     * The regular duration is the first derivative of the timeline   
     * 
     * @return a duration for a regular time-series or null
     */

    Duration getRegularDuration();

    /**
     * Returns the earliest basis time associated with the {@link TimeSeries}. If {@link #hasMultipleTimeSeries()} 
     * returns <code>true</code>, the issue time associated with each atomic time-series may be returned by obtaining 
     * a {@link #basisTimeIterator()} and requesting the {@link #getEarliestBasisTime()} for each atomic time-series.  
     * 
     * @return the earliest basis time associated with any time-series
     */

    Instant getEarliestBasisTime();

}
