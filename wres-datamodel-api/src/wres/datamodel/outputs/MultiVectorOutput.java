package wres.datamodel.outputs;

import java.util.Map;

import wres.datamodel.MetricConstants;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;

/**
 * One or more vectors that are explicitly mapped to elements in {@link MetricDimension}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MultiVectorOutput extends MetricOutput<Map<MetricDimension, VectorOfDoubles>>
{

    /**
     * Returns a prescribed vector from the map or null if no mapping exists.
     * 
     * @param identifier the identifier
     * @return a vector or null
     */

    VectorOfDoubles get(MetricDimension identifier);

    /**
     * Returns true if the store contains a mapping for the prescribed identifier, false otherwise.
     * 
     * @param identifier the identifier
     * @return true if the mapping exists, false otherwise
     */

    boolean containsKey(MetricDimension identifier);
}
