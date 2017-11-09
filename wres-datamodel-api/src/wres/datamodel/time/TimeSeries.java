package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

import wres.datamodel.inputs.pairs.Pair;

/**
 * <p>A representation of one or more {@link TimeSeries}, each of which contains one or more {@link Pair} of times and 
 * values. Each value has a designated type, which may comprise a tuple or some other type. For example, a 
 * {@link TimeSeries} may store a single time-series of scalar observations or multiple time-series of paired forecasts 
 * and observations.</p> 
 * 
 * <p>In this context, a time is an instant on the UTC timeline, which is represented by an {@link Instant}. 
 * Additionally, each atomic time-series in the {@link TimeSeries} container is anchored to a specific basis time, 
 * which is also represented by an {@link Instant}.</p>
 * 
 * <p>A {@link TimeSeries} may be regular or irregular. A {@link TimeSeries} is regular iif each time is separated by 
 * exactly the same {@link Duration} and there are no missing times between the earliest and latest times; by 
 * implication a regular {@link TimeSeries} cannot contain more than one value at the same time for the same 
 * basis time.</p>
 * 
 * <p>A {@link TimeSeries} may contain only one fundamental type of data, namely non-forecast data (e.g. observations, 
 * model simulations and analysis) or forecast data. It cannot contain both types, and the type is revealed by 
 * {@link #isForecast()}. In other words, the {@link TimeSeries} may store a single atomic time-series or multiple 
 * atomic {@link TimeSeries} that each have a separate basis/issue time.</p>
 * 
 * <p>In this context, the need to represent one or more values for each time is significant, since it implies that 
 * a concrete implementation cannot assume a unique key for each value unless {@link #isRegular()} returns 
 * <code>true</code>.</p>
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>This class is immutable and thread-safe. For example, implementations of the methods that return {@link Iterator} 
 * should not allow {@link Iterator#remove()} to remove an element from the underlying time-series.</p>
 * 
 * <p>If a {@link TimeSeries} is not explicitly associated with a basis time on construction, the earliest time should 
 * be used as the basis time.</p>
 * 
 * @param <T> the designated atomic type stored by this container
 * @version 0.1
 * @since 0.3
 * @author james.brown@hydrosolved.com
 */

public interface TimeSeries<T>
{

    /**
     * Returns an {@link Iterator} over all the times in the {@link TimeSeries}, regardless of basis time.
     * 
     * @return an iterator over the pairs of times and values
     */
    
    Iterator<Pair<Instant,T>> timeIterator();
    
    /**
     * Returns an {@link Iterator} over the atomic {@link TimeSeries} by basis/issue time. For time-series with a 
     * single basis time (e.g. observed time-series), this will return one atomic time-series. For forecast time-series, 
     * it will return as many atomic time-series as discrete forecasts in the time-series container. The time-series are 
     * returned in time order from the earliest basis time to the latest basis time.
     * 
     * @return an iterator over the atomic time-series
     */
    
    Iterator<TimeSeries<T>> basisTimeIterator();
    
    /**
     * Returns an {@link Iterator} over the list of values across all time-series that have a common forecast lead time.
     * For non-forecast time-series, this will return a single list. For forecast time-series, it will return as many 
     * lists as discrete forecast lead times in the time-series container. The values are returned in time order from 
     * the earliest lead time to the latest lead time.
     * 
     * @return an iterator over the atomic time-series
     */
    
    Iterator<List<T>> leadTimeIterator();    
    
    /**
     * Returns true if the {@link TimeSeries} has a regular spacing of times with no missing times. In this context,
     * regularity applies to the time-step associated with each atomic time-series. Also see {#getRegularDuration()}.
     * 
     * @return true if the time-series is regular, false otherwise
     */
    
    boolean isRegular();
    
    /**
     * Returns <code>true</code> if the {@link TimeSeries} contains atomic time-series with more than one basis/issue 
     * time (and hence more than one lead time), <code>false/</code> otherwise (e.g. for observations and non-forecast 
     * model simulations). A {@link TimeSeries} cannot contain more than one type of time-series data.
     * 
     * @return true if the time-series contains forecasts
     */
    
    boolean isForecast();
    
    /**
     * Returns true if the {@link TimeSeries} contains multiple atomic time-series, each with a separate issue/basis
     * time, false otherwise. To obtain the 
     * 
     * @return true if the time-series contains more than one atomic time-series
     */
    
    boolean hasMultipleTimeSeries();

    /**
     * Returns the {@link Duration} between elements in a regular {@link TimeSeries} or null if this is an irregular 
     * {@link TimeSeries}. Also see {@link #isRegular()}. When {@link #isRegular()} returns <code>true</code>, all 
     * atomic time-series must have a constant {@link Duration} between times, as revealed by this method. However, 
     * when the container stores forecasts, the basis/issue time may not be regular and the time-step between 
     * basis/issue times may differ from the {@link #getRegularDuration}. For example, the container may store 
     * forecasts that are issued once per day with a regular time-step of 6 hours. In this context, the 
     * {@link #getRegularDuration} would be 6 hours.  
     * 
     * @return a duration for a regular time-series or null
     */
    
    Duration getRegularDuration();
    
    /**
     * Returns the earliest time associated with the {@link TimeSeries}, which is equivalent to the first issue/basis. 
     * time. If {@link #hasMultipleTimeSeries()} returns <code>true</code>, the issue time associated with each atomic 
     * time-series may be returned by obtaining a {@link #basisTimeIterator()} and requesting the {@link #getAnchor()} 
     * for each atomic time-series.  
     * 
     * @return the earliest time associated with any time-series, which is equivalent to the earliest issue/basis time
     */
    
    Instant getAnchor();
    
}
