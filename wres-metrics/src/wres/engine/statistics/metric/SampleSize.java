package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.outputs.ScalarOutput;

/**
 * Constructs a {@link Metric} that returns the {@link MetricInput#size()}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SampleSize<S extends PairedInput<?>> extends Metric<S, ScalarOutput> implements Score
{

    @Override
    public ScalarOutput apply( S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        return getDataFactory().ofScalarOutput( s.getData().size(),
                                                getMetadata( s, s.getData().size(), MetricConstants.MAIN, null ) );
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.SAMPLE_SIZE;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public ScoreOutputGroup getScoreOutputGroup()
    {
        return ScoreOutputGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class SampleSizeBuilder<S extends PairedInput<?>> extends MetricBuilder<S, ScalarOutput>
    {
        @Override
        protected SampleSize<S> build() throws MetricParameterException
        {
            return new SampleSize<>( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private SampleSize( final SampleSizeBuilder<S> builder ) throws MetricParameterException
    {
        super( builder );
    }

}
