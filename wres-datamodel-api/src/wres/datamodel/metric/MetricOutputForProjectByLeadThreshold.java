package wres.datamodel.metric;

import java.util.Objects;

import wres.datamodel.metric.MetricConstants.MetricOutputGroup;

/**
 * A high-level container of {@link MetricOutput} associated with a verification project. The outputs are stored by lead
 * time and threshold in a {@link MetricOutputMultiMapByLeadThreshold}. Retrieve the outputs using the instance methods.
 * If no outputs exist, the instance methods return null.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputForProjectByLeadThreshold
{

    /**
     * Returns true if {@link #getOutput(MetricOutputGroup...)} returns non-null for the input type, false otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup} to test
     * @return true if {@link #getOutput(MetricOutputGroup...)} returns non-null for the input, false otherwise
     */

    default boolean hasOutput(MetricOutputGroup outGroup)
    {
        return Objects.nonNull(getOutput(outGroup));
    }

    /**
     * Returns true if {@link #getScalarOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getScalarOutput()} returns non-null, false otherwise
     */

    default boolean hasScalarOutput()
    {
        return Objects.nonNull(getScalarOutput());
    }

    /**
     * Returns true if {@link #getVectorOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getVectorOutput()} returns non-null, false otherwise
     */

    default boolean hasVectorOutput()
    {
        return Objects.nonNull(getVectorOutput());
    }

    /**
     * Returns true if {@link #getMultiVectorOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getMultiVectorOutput()} returns non-null, false otherwise
     */

    default boolean hasMultiVectorOutput()
    {
        return Objects.nonNull(getMultiVectorOutput());
    }

    /**
     * Returns true if {@link #getMatrixOutput()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getMatrixOutput()} returns non-null, false otherwise
     */

    default boolean hasMatrixOutput()
    {
        return Objects.nonNull(getMatrixOutput());
    }

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} for a prescribed array of {@link MetricOutputGroup} or null if
     * no output exists. To return all available outputs, use {@link #getOutputTypes()} as input to this method.
     * 
     * @param outGroup the array of {@link MetricOutputGroup}
     * @return the metric output or null
     */

    MetricOutputMultiMapByLeadThreshold<MetricOutput<?>> getOutput(MetricOutputGroup... outGroup);

    /**
     * Returns all {@link MetricOutputGroup} for which outputs are available.
     * 
     * @return all {@link MetricOutputGroup} for which outputs are available
     */

    MetricOutputGroup[] getOutputTypes();

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link ScalarOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     */

    MetricOutputMultiMapByLeadThreshold<ScalarOutput> getScalarOutput();

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link VectorOutput} or null if no output exists.
     * 
     * @return the vector output or null
     */

    MetricOutputMultiMapByLeadThreshold<VectorOutput> getVectorOutput();

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     */

    MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> getMultiVectorOutput();

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     */

    MetricOutputMultiMapByLeadThreshold<MatrixOutput> getMatrixOutput();

    /**
     * Builder.
     */

    interface Builder
    {

        /**
         * Adds a new scalar result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addScalarOutput(Integer leadTime, Threshold threshold, MetricOutputMapByMetric<ScalarOutput> result);

        /**
         * Adds a new vector result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addVectorOutput(Integer leadTime, Threshold threshold, MetricOutputMapByMetric<VectorOutput> result);

        /**
         * Adds a new multi-vector result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addMultiVectorOutput(Integer leadTime, Threshold threshold, MetricOutputMapByMetric<MultiVectorOutput> result);

        /**
         * Adds a new matrix result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addMatrixOutput(Integer leadTime, Threshold threshold, MetricOutputMapByMetric<MatrixOutput> result);

        /**
         * Returns a {@link MetricOutputForProjectByLeadThreshold}.
         * 
         * @return a {@link MetricOutputForProjectByLeadThreshold}
         */

        MetricOutputForProjectByLeadThreshold build();

    }

}
