package wres.datamodel.metric;

import java.util.Set;

/**
 * A sorted map of {@link MetricOutput} associated with a single metric. The results are stored by forecast lead time
 * and threshold.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapByLeadThreshold<T extends MetricOutput<?>>
extends MetricOutputMapWithBiKey<Integer, Threshold, T>
{

    /**
     * Slice by forecast lead time.
     * 
     * @param first the forecast lead time
     * @return the submap
     */

    default MetricOutputMapByLeadThreshold<T> sliceByLead(final Integer first)
    {
        return (MetricOutputMapByLeadThreshold<T>)sliceByFirst(first);
    }

    /**
     * Slice by threshold.
     * 
     * @param second the threshold
     * @return the submap
     */

    default MetricOutputMapByLeadThreshold<T> sliceByThreshold(final Threshold second)
    {
        return (MetricOutputMapByLeadThreshold<T>)sliceBySecond(second);
    }

    /**
     * Return the lead time keys.
     * 
     * @return a view of the lead time keys
     */

    default Set<Integer> keySetByLead()
    {
        return keySetByFirstKey();
    }

    /**
     * Return the threshold keys.
     * 
     * @return a view of the threshold keys
     */

    default Set<Threshold> keySetByThreshold()
    {
        return keySetBySecondKey();
    }

    /**
     * Returns the {@link MetricOutputMetadata} associated with all {@link MetricOutput} in the store. This may
     * contain more (optional) information than the (required) metadata associated with the individual outputs. 
     * However, all required elements must match.
     * 
     * @return the metadata
     */
    
    MetricOutputMetadata getMetadata();

}
