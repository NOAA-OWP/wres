package wres.datamodel.outputs;

import java.util.Set;

import wres.datamodel.Threshold;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;

/**
 * A sorted map of {@link MetricOutput} associated with a single metric. The results are stored by {@link TimeWindow}
 * and {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapByTimeAndThreshold<T extends MetricOutput<?>>
        extends MetricOutputMapWithBiKey<TimeWindow, Threshold, T>
{

    /**
     * Filter by time.
     * 
     * @param timeWindow the forecast lead time
     * @return the submap
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
     */

    default MetricOutputMapByTimeAndThreshold<T> filterByThreshold( final Threshold threshold )
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
     * Return the {@link Threshold} keys.
     * 
     * @return a view of the threshold keys
     */

    default Set<Threshold> setOfThresholdKey()
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
        return setOfThresholdKey().stream().anyMatch( Threshold::isQuantile );
    }
    
    /**
     * Filters by lead time in hours. Returns all outputs whose {@link TimeWindow#getEarliestLeadTimeInHours()} or
     * {@link TimeWindow#getLatestLeadTimeInHours()} matches the specified lead time in hours.
     * 
     * @param leadHours the lead time in hours
     * @return the submap
     */
    
    MetricOutputMapByTimeAndThreshold<T> filterByLeadTimeInHours( long leadHours );

    /**
     * Returns the unique lead times associated with the {@link TimeWindow} for which the outputs are defined. Checks
     * both the {@link TimeWindow#getEarliestLeadTimeInHours()} and the {@link TimeWindow#getLatestLeadTimeInHours()}.
     * 
     * @return a view of the lead times in hours
     */

    Set<Long> unionOfLeadTimesInHours();
    
    /**
     * Returns the {@link MetricOutputMetadata} associated with all {@link MetricOutput} in the store. This may contain
     * more (optional) information than the (required) metadata associated with the individual outputs. However, all
     * required elements must match, in keeping with {@link MetricOutputMetadata#minimumEquals(MetricOutputMetadata)}.
     * 
     * @return the metadata
     */

    MetricOutputMetadata getMetadata();

}
