package wres.datamodel;

import wres.datamodel.MetricConstants.ScoreOutputGroup;

/**
 * A vector of outputs associated with a score that comprises one or more components, such as a decomposable score. The 
 * number of outputs, as well as the individual outputs and the order in which they are stored, is prescribed by the 
 * {@link ScoreOutputGroup} associated with the output.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface VectorOutput extends MetricOutput<VectorOfDoubles>
{
    
    /**
     * Returns the {@link ScoreOutputGroup}, which defines the number and order in which outputs are stored. 
     * This is accessible from {@link ScoreOutputGroup#getMetricComponents()}.
     * 
     * @return the {@link ScoreOutputGroup} associated with the metric output
     */
    
    ScoreOutputGroup getOutputTemplate();
    
    /**
     * Returns the value associated with a prescribed {@link MetricConstants} in the input. The input 
     * {@link MetricConstants} must belong to the {@link ScoreOutputGroup} associated with this output.
     * 
     * @param component the component required
     * @return the scalar value associated with the component
     * @throws ArrayIndexOutOfBoundsException if the result is not available
     */
    
    double getValue(MetricConstants component);
}
