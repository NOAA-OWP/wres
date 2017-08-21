package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.MetricInput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.VectorOutput;

/**
 * A generic implementation of an error score for {@link SingleValuedPairs} that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

abstract class DecomposableDoubleErrorScore<S extends MetricInput<?>> extends Metric<S, VectorOutput>
implements Score
{

    /**
     * The decomposition identifier. See {@link MetricDecompositionGroup}.
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

    static abstract class DecomposableDoubleErrorScoreBuilder<S extends MetricInput<?>>
    extends
        MetricBuilder<S, VectorOutput>
    {
        /**
         * The type of metric decomposition. See {@link MetricDecompositionGroup}.
         */

        MetricDecompositionGroup decompositionID = MetricDecompositionGroup.NONE;

        /**
         * Sets the decomposition identifier.
         * 
         * @param decompositionID the decomposition identifier
         * @return the builder
         */

        DecomposableDoubleErrorScoreBuilder<S> setDecompositionID(final MetricDecompositionGroup decompositionID)
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

    DecomposableDoubleErrorScore(final DecomposableDoubleErrorScoreBuilder<S> builder)
    {
        super(builder);
        Objects.requireNonNull(builder.decompositionID, "Specify a non-null decomposition identifier.");
        this.decompositionID = builder.decompositionID;
    }

}
