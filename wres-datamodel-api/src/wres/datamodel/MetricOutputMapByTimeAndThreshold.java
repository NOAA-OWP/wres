package wres.datamodel;

import java.util.Set;

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
     * Slice by forecast lead time.
     * 
     * @param timeWindow the forecast lead time
     * @return the submap
     */

    default MetricOutputMapByTimeAndThreshold<T> sliceByTime( final TimeWindow timeWindow )
    {
        return (MetricOutputMapByTimeAndThreshold<T>) sliceByFirst( timeWindow );
    }

    /**
     * Slice by threshold.
     * 
     * @param threshold the threshold
     * @return the submap
     */

    default MetricOutputMapByTimeAndThreshold<T> sliceByThreshold( final Threshold threshold )
    {
        return (MetricOutputMapByTimeAndThreshold<T>) sliceBySecond( threshold );
    }

    /**
     * Return the {@link TimeWindow} keys.
     * 
     * @return a view of the time window keys
     */

    default Set<TimeWindow> keySetByTime()
    {
        return keySetByFirstKey();
    }

    /**
     * Return the {@link Threshold} keys.
     * 
     * @return a view of the threshold keys
     */

    default Set<Threshold> keySetByThreshold()
    {
        return keySetBySecondKey();
    }

    /**
     * Returns true if the map contains one or more quantile thresholds, false otherwise.
     * 
     * @return true if the store contains one or more quantile thresholds, false otherwise
     */

    default boolean hasQuantileThresholds()
    {
        return keySetByThreshold().stream().anyMatch( Threshold::isQuantile );
    }

    /**
     * Returns the {@link MetricOutputMetadata} associated with all {@link MetricOutput} in the store. This may contain
     * more (optional) information than the (required) metadata associated with the individual outputs. However, all
     * required elements must match, in keeping with {@link MetricOutputMetadata#minimumEquals(MetricOutputMetadata)}.
     * 
     * @return the metadata
     */

    MetricOutputMetadata getMetadata();

}
