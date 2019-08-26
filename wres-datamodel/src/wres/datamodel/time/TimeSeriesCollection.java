package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
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
     * Returns the {@link TimeSeries} in the collection.
     * 
     * @return an iterable atomic time-series by basis time
     */

    List<TimeSeries<T>> getTimeSeries();

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
