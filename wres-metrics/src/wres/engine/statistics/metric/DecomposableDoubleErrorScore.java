package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;

/**
 * A generic implementation of an error score that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

abstract class DecomposableDoubleErrorScore<S extends SingleValuedPairs> extends Metric<S, VectorOutput>
implements Score
{

    /**
     * The decomposition identifier. See {@link MetricConstants#getDecompositionID()}.
     */

    private final MetricDecompositionGroup decompositionID;

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public MetricDecompositionGroup getDecompositionID()
    {
        return decompositionID;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static abstract class DecomposableDoubleErrorScoreBuilder<S extends SingleValuedPairs>
    extends
        MetricBuilder<S, VectorOutput>
    {
        /**
         * The type of metric decomposition. See {@link MetricConstants#getDecompositionID()}.
         */

        private MetricDecompositionGroup decompositionID = MetricDecompositionGroup.NONE;

        /**
         * Sets the decomposition identifier.
         * 
         * @param decompositionID the decomposition identifier
         * @return the builder
         */

        protected DecomposableDoubleErrorScoreBuilder<S> setDecompositionID(final MetricDecompositionGroup decompositionID)
        {
            this.decompositionID = decompositionID;
            return this;
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    protected DecomposableDoubleErrorScore(final DecomposableDoubleErrorScoreBuilder<S> builder)
    {
        super(builder);
        Objects.requireNonNull(builder.decompositionID, "Specify a non-null decomposition identifier.");
        this.decompositionID = builder.decompositionID;
    }

}
