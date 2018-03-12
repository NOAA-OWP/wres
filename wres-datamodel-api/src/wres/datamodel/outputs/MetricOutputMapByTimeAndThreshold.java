package wres.datamodel.outputs;

import java.util.Set;

import wres.datamodel.Thresholds;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;

/**
 * A sorted map of {@link MetricOutput} associated with a single metric. The results are stored by {@link TimeWindow}
 * and {@link Thresholds}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapByTimeAndThreshold<T extends MetricOutput<?>>
        extends MetricOutputMapWithBiKey<TimeWindow, Thresholds, T>
{

    /**
     * Filter by time.
     * 
     * @param timeWindow the forecast lead time
     * @return the submap
     * @throws MetricOutputException if the map could not be filtered
     */

    default MetricOutputMapByTimeAndThreshold<T> filterByTime( final TimeWindow timeWindow )
    {
        return (MetricOutputMapByTimeAndThreshold<T>) filterByFirstKey( timeWindow );
    }

    /**
     * Filter by threshold.
     * 
     * @param threshold the threshold
     * @return the submap
     * @throws MetricOutputException if the map could not be filtered
     */

    default MetricOutputMapByTimeAndThreshold<T> filterByThreshold( final Thresholds threshold )
    {
        return (MetricOutputMapByTimeAndThreshold<T>) filterBySecondKey( threshold );
    }

    /**
     * Return the {@link TimeWindow} keys.
     * 
     * @return a view of the time window keys
     */

    default Set<TimeWindow> setOfTimeWindowKey()
    {
        return setOfFirstKey();
    }

    /**
     * Return the {@link Thresholds} keys.
     * 
     * @return a view of the threshold keys
     */

    default Set<Thresholds> setOfThresholdKey()
    {
        return setOfSecondKey();
    }

    /**
     * Returns true if the map contains one or more quantile thresholds, false otherwise.
     * 
     * @return true if the store contains one or more quantile thresholds, false otherwise
     */

    default boolean hasQuantileThresholds()
    {
        return setOfThresholdKey().stream().anyMatch( next -> next.first().isQuantile()
                                                              || ( next.hasTwo() && next.second().isQuantile() ) );
    }

    /**
     * Return only those {@link TimeWindow} keys whose pairs of lead times are unique.
     * 
     * @return a view of the time window keys
     */

    Set<TimeWindow> setOfTimeWindowKeyByLeadTime();

    /**
     * Filters the map by the {@link TimeWindow#getEarliestLeadTime()} and {@link TimeWindow#getLatestLeadTime()} in
     * the input {@link TimeWindow}, returning a new sub-map of elements with matching times.
     * 
     * @param window the time window on which to match lead times
     * @return the submap
     * @throws MetricOutputException if the map could not be filtered
     */

    MetricOutputMapByTimeAndThreshold<T> filterByLeadTime( TimeWindow window );

    /**
     * Returns the {@link MetricOutputMetadata} associated with all {@link MetricOutput} in the store. This may contain
     * more (optional) information than the (required) metadata associated with the individual outputs. However, all
     * required elements must match, in keeping with {@link MetricOutputMetadata#minimumEquals(MetricOutputMetadata)}.
     * 
     * @return the metadata
     */

    MetricOutputMetadata getMetadata();

}
