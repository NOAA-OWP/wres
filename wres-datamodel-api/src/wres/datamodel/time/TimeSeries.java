package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

import wres.datamodel.inputs.pairs.Pair;

/**
 * <p>A representation of one or more time-series, each of which contains one or more <@link Pair> of times and values. 
 * In this context, a time is an instant on the UTC timeline, which is represented by an {@link Instant}. Additionally,
 * each time-series has a basis time, which is also represented by an {@link Instant}. If a time-series is not 
 * associated with a basis time, the earliest time is assumed to be the basis time.</p>
 * 
 * <p>A time-series may be regular or irregular. A time-series is regular iif each time is separated by exactly the 
 * same {@link Duration} and there are no missing times between the earliest and latest times.</p>
 * 
 * <p>In this context, the need to represent one or more values for each time is significant, since it implies that 
 * a concrete implementation cannot assume a unique key for each value unless {@link #isRegular()} returns 
 * <code>true</code></p>
 * 
 * @version 0.1
 * @since 0.3
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeries<T>
{

    /**
     * Returns an {@link Iterator} over the pairs in the time-series.
     * 
     * @return an iterator over the pairs
     */
    
    Iterator<Pair<Instant,T>> iterator();
    
    /**
     * Returns a nested {@link Iterator} where the nested iterator traverses the pairs of times and values associated
     * with a time-series that has a common basis/reference time. For time-series with a single basis time (e.g. observed
     * time-series), this will return a single nested iterator. For forecast time-series, it will return as many nested 
     * iterators as discrete forecasts in the time-series. The nested iterators are returned in time order from the 
     * earliest basis time to the latest basis time on the UTC timeline.
     * 
     * @return a nested iterator over the time-series
     */
    
    Iterator<Iterator<Pair<Instant,T>>> iterateBasisTimes();
    
    /**
     * Returns true if the time-series has a regular spacing of values with a single value for each time.
     * 
     * @return true if the time-series is regular, false otherwise
     */
    
    boolean isRegular();
    
    /**
     * Returns the {@link Duration} between elements in a regular time-series or null if this is an irregular 
     * time-series. Also see {@link #isRegular()}.
     * 
     * @return a duration for a regular time-series or null
     */
    
    Duration getRegularDuration();
    
}
