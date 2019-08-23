package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

/**
 * <p>A collection of one or more atomic time-series, each of which contains one or more {@link Event}, which 
 * comprises an {@link Instant} and value. Each value has a designated type, which may comprise a tuple or some other 
 * type. For example, a {@link TimeSeries} may store a single time-series of scalar observations or multiple 
 * time-series of paired forecasts and observations.
 * 
 * <p>Each atomic time-series in the {@link TimeSeries} container is anchored to a specific reference time, which is 
 * represented by an {@link Instant}. 
 * 
 * <p>A {@link TimeSeries} may be regular or irregular. A {@link TimeSeries} is regular if and only if each value is 
 * separated by exactly the same {@link Duration}, there are no missing times between the earliest and latest times, 
 * and the number of times in each atomic time-series is constant; by implication a regular {@link TimeSeries} cannot 
 * contain more than one value with the same valid time that originates from the same reference time. 
 * 
 * <p>A duration is defined as the {@link Duration} between the basis time and the (valid) time associated with a 
 * value.
 * 
 * <p><b>Implementation Requirements:</b>
 * 
 * <p>This class is immutable and thread-safe. For example, implementations of the methods that return {@link Iterable} 
 * views should not allow {@link Iterator#remove()} to remove an element from the underlying time-series.
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

    Iterable<Event<T>> eventIterator();

    /**
     * Returns a basis-time view of the {@link TimeSeries} whereby each atomic time-series originates from one basis 
     * time and each element is associated with a separate {@link Duration}. The time-series are not returned in a 
     * guaranteed order.
     * 
     * @return an iterable atomic time-series by basis time
     */

    Iterable<TimeSeries<T>> referenceTimeIterator();

    /**
     * Returns a duration view of the {@link TimeSeries} whereby each {@link List} contains all of the {@link Event} 
     * associated with one {@link Duration} and each {@link Event} originates from a separate reference time. The 
     * lists are are not returned in a guaranteed order.
     * 
     * @return an iteration over lists of events, each list containing the events for one duration
     */

    Iterable<List<Event<T>>> durationIterator();

    /**
     * Returns the reference times associated with all the atomic time-series in the container. The times are ordered 
     * from the earliest {@link Instant} to the latest.
     * 
     * @return the reference times
     */

    SortedSet<Instant> getReferenceTimes();

    /**
     * Returns the durations associated with all the atomic time-series in the container. The results are ordered 
     * from the earliest duration to the latest.
     * 
     * @return the durations
     */

    SortedSet<Duration> getDurations();
    
}
