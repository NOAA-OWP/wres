package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.outputs.DoubleScoreOutput;

/**
 * Constructs a {@link Metric} that returns the sample size.
 * 
 * @author james.brown@hydrosolved.com
 */
class SampleSize<S extends MetricInput<?>> extends OrdinaryScore<S, DoubleScoreOutput>
{

    @Override
    public DoubleScoreOutput apply( S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        return getDataFactory().ofDoubleScoreOutput( s.getRawData().size(),
                                               getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null ) );
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

    static class SampleSizeBuilder<S extends MetricInput<?>> extends OrdinaryScoreBuilder<S, DoubleScoreOutput>
    {
        @Override
        public SampleSize<S> build() throws MetricParameterException
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
