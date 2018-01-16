package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MultiValuedScoreOutput;

/**
 * A generic implementation of an error score for that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class DecomposableDoubleErrorScore<S extends MetricInput<?>> extends Metric<S, MultiValuedScoreOutput>
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

    public static abstract class DecomposableDoubleErrorScoreBuilder<S extends MetricInput<?>>
            extends
            MetricBuilder<S, MultiValuedScoreOutput>
    {
        /**
         * The type of metric decomposition. See {@link ScoreOutputGroup}.
         */

        private ScoreOutputGroup decompositionID = ScoreOutputGroup.NONE;

        /**
         * Sets the decomposition identifier.
         * 
         * @param decompositionID the decomposition identifier
         * @return the builder
         */

        public DecomposableDoubleErrorScoreBuilder<S> setDecompositionID( final ScoreOutputGroup decompositionID )
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

    protected DecomposableDoubleErrorScore( final DecomposableDoubleErrorScoreBuilder<S> builder ) throws MetricParameterException
    {
        super( builder );
        this.decompositionID = builder.decompositionID;
        if ( Objects.isNull( this.decompositionID ) )
        {
            throw new MetricParameterException( "Specify a non-null decomposition identifier." );
        }
    }

}
