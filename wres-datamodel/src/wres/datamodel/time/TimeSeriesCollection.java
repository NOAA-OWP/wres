package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.SortedSet;

/**
 * <p>A collection of one or more atomic time-series.
 * 
 * <p><b>Implementation Requirements:</b>
 * 
 * <p>This class is immutable and thread-safe. For example, implementations of the methods that return {@link Iterable} 
 * views should not allow {@link Iterator#remove()} to remove an element from the underlying time-series.
 * 
 * @param <T> the atomic type of data
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeriesCollection<T>
{

    /**
     * Returns an {@link Iterator} over all the {@link Event} in the {@link TimeSeriesCollection}. The times are returned in a 
     * guaranteed order from the earliest time to the latest time.
     * 
     * @return iterable pairs of times and values
     */

    Iterable<Event<T>> eventIterator();

    /**
     * Returns a basis-time view of the {@link TimeSeriesCollection} whereby each atomic time-series originates from one basis 
     * time and each element is associated with a separate {@link Duration}. The time-series are not returned in a 
     * guaranteed order.
     * 
     * @return an iterable atomic time-series by basis time
     */

    Iterable<TimeSeriesA<T>> referenceTimeIterator();

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
