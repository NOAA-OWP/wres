package wres.datamodel.metric;

import java.util.Objects;

/**
 * A high-level container of {@link MetricOutput} associated with a verification project. The outputs are stored by 
 * threshold in a {@link MetricOutputMultiMapByThreshold}. Retrieve the outputs using the instance methods. If no 
 * outputs exist, the instance methods return null.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputForProjectByThreshold
{

    /**
     * Returns true if {@link #getScalarOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScalarOutput()} returns non-null, false otherwise
     */
    
    default boolean hasScalarOutput() {
        return Objects.nonNull(getScalarOutput());
    }
    
    /**
     * Returns true if {@link #getVectorOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getVectorOutput()} returns non-null, false otherwise
     */
    
    default boolean hasVectorOutput() {
        return Objects.nonNull(getVectorOutput());
    }    

    /**
     * Returns true if {@link #getMultiVectorOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getMultiVectorOutput()} returns non-null, false otherwise
     */
    
    default boolean hasMultiVectorOutput() {
        return Objects.nonNull(getMultiVectorOutput());
    }  
    
    /**
     * Returns true if {@link #getMatrixOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getMatrixOutput()} returns non-null, false otherwise
     */
    
    default boolean hasMatrixOutput() {
        return Objects.nonNull(getMatrixOutput());
    }        

    /**
     * Returns a {@link MetricOutputMultiMapByThreshold} of {@link ScalarOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     */
    
    MetricOutputMultiMapByThreshold<ScalarOutput> getScalarOutput();
    
    /**
     * Returns a {@link MetricOutputMultiMapByThreshold} of {@link VectorOutput} or null if no output exists.
     * 
     * @return the vector output or null
     */
    
    MetricOutputMultiMapByThreshold<VectorOutput> getVectorOutput();    
    
    /**
     * Returns a {@link MetricOutputMultiMapByThreshold} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     */
    
    MetricOutputMultiMapByThreshold<MultiVectorOutput> getMultiVectorOutput();       

    /**
     * Returns a {@link MetricOutputMultiMapByThreshold} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     */
    
    MetricOutputMultiMapByThreshold<MatrixOutput> getMatrixOutput();        
    
    /**
     * Builder.
     */

    interface Builder
    {                                                                                              

        /**
         * Adds a new scalar result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addScalarOutput(Threshold threshold, MetricOutputMapByMetric<ScalarOutput> result);

        /**
         * Adds a new vector result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addVectorOutput(Threshold threshold, MetricOutputMapByMetric<VectorOutput> result);
        
        /**
         * Adds a new multi-vector result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addMultiVectorOutput(Threshold threshold, MetricOutputMapByMetric<MultiVectorOutput> result);

        /**
         * Adds a new matrix result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addMatrixOutput(Threshold threshold, MetricOutputMapByMetric<MatrixOutput> result);

        /**
         * Returns a {@link MetricOutputForProjectByThreshold}.
         * 
         * @return a {@link MetricOutputForProjectByThreshold}
         */

        MetricOutputForProjectByThreshold build();

    }    
    
    
}
