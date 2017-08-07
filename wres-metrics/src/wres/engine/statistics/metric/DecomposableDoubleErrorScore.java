package wres.engine.statistics.metric;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;

/**
 * A generic implementation of an error score that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class DecomposableDoubleErrorScore<S extends SingleValuedPairs> extends Metric<S, VectorOutput>
implements Score
{

    /**
     * The decomposition identifier. See {@link MetricConstants#getDecompositionID()}.
     */

    private final MetricConstants decompositionID;

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public MetricConstants getDecompositionID()
    {
        return decompositionID;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static abstract class DecomposableDoubleErrorScoreBuilder<S extends SingleValuedPairs>
    extends
        MetricBuilder<S, VectorOutput>
    {
        /**
         * The type of metric decomposition. See {@link MetricConstants#getDecompositionID()}.
         */

        private MetricConstants decompositionID = MetricConstants.NONE;

        /**
         * Sets the decomposition identifier.
         * 
         * @param decompositionID the decomposition identifier
         * @return the builder
         */

        protected DecomposableDoubleErrorScoreBuilder<S> setDecompositionID(final MetricConstants decompositionID)
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
        if(!Score.isSupportedDecompositionID(builder.decompositionID))
        {
            throw new IllegalStateException("Unsupported decomposition identifier: " + builder.decompositionID);
        }
        this.decompositionID = builder.decompositionID;
    }

}
