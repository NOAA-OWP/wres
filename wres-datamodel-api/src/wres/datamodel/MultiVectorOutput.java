package wres.datamodel;

import java.util.Map;

/**
 * One or more vectors that are explicitly mapped to elements in {@link MetricConstants}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MultiVectorOutput extends MetricOutput<Map<MetricConstants, VectorOfDoubles>>
{

    /**
     * Returns a prescribed vector from the map or null if no mapping exists.
     * 
     * @param identifier the identifier
     * @return a vector or null
     */

    VectorOfDoubles get(MetricConstants identifier);

    /**
     * Returns true if the store contains a mapping for the prescribed identifier, false otherwise.
     * 
     * @param identifier the identifier
     * @return true if the mapping exists, false otherwise
     */

    boolean containsKey(MetricConstants identifier);
}
