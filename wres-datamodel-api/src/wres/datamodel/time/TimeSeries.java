package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

/**
 * <p>A representation of one or more atomic time-series, each of which contains one or more {@link Event}, which 
 * comprises an {@link Instant} and value. Each value has a designated type, which may comprise a tuple or some other 
 * type. For example, a {@link TimeSeries} may store a single time-series of scalar observations or multiple 
 * time-series of paired forecasts and observations.</p> 
 * 
 * <p>Each atomic time-series in the {@link TimeSeries} container is anchored to a specific basis time, which is 
 * represented by an {@link Instant}. 
 * 
 * <p>A {@link TimeSeries} may be regular or irregular. A {@link TimeSeries} is regular if and only if each value is 
 * separated by exactly the same {@link Duration}, there are no missing times between the earliest and latest times, 
 * and the number of times in each atomic time-series is constant; by implication a regular {@link TimeSeries} cannot 
 * contain more than one value with the same valid time that originates from the same basis time. 
 * 
 * <p>A duration is defined as the {@link Duration} between the basis time and the (valid) time associated with a 
 * value. If {@link #isRegular()} returns <code>true</code>, the {@link #getRegularDuration()} corresponds to the 
 * fixed duration between successive times in the time-series.</p>
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>This class is immutable and thread-safe. For example, implementations of the methods that return {@link Iterable} 
 * views should not allow {@link Iterator#remove()} to remove an element from the underlying time-series.</p>
 * 
 * @param <T> the atomic type of data
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeries<T>
{

    /**
     * Returns an {@link Iterator} over all the {@link Event} in the {@link TimeSeries}. The times are returned in a 
     * guaranteed order from the earliest time to the latest time.
     * 
     * @return iterable pairs of times and values
     */

    Iterable<Event<T>> timeIterator();

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
     * Returns the basis times associated with all the atomic time-series in the container. If 
     * {@link #hasMultipleTimeSeries()} returns <code>false</code>, the returned list will contain a single element, 
     * otherwise more than one element.
     * 
     * @return the basis times
     */

    List<Instant> getBasisTimes();

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
     * forecasts that are issued once per day with a regular time-step of 6 hours.
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

    /**
     * Returns the latest basis time associated with the {@link TimeSeries}.
     * 
     * @return the latest basis time associated with any time-series
     */

    Instant getLatestBasisTime();    
    
}
