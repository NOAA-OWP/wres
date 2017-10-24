package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants.ScoreOutputGroup;
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
     * The decomposition identifier. See {@link ScoreOutputGroup}.
     */

    private final ScoreOutputGroup decompositionID;

    @Override
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public ScoreOutputGroup getScoreOutputGroup()
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
         * The type of metric decomposition. See {@link ScoreOutputGroup}.
         */

        ScoreOutputGroup decompositionID = ScoreOutputGroup.NONE;

        /**
         * Sets the decomposition identifier.
         * 
         * @param decompositionID the decomposition identifier
         * @return the builder
         */

        DecomposableDoubleErrorScoreBuilder<S> setDecompositionID( final ScoreOutputGroup decompositionID )
        {
            this.decompositionID = decompositionID;
            return this;
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    DecomposableDoubleErrorScore( final DecomposableDoubleErrorScoreBuilder<S> builder ) throws MetricParameterException
    {
        super( builder );
        this.decompositionID = builder.decompositionID;
        if ( Objects.isNull( this.decompositionID ) )
        {
            throw new MetricParameterException( "Specify a non-null decomposition identifier." );
        }
    }

}
