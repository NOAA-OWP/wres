package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.DataFactory;
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

    /**
     * Returns an instance.
     * 
     * @param <S> the input type
     * @return an instance
     */
    
    public static <S extends MetricInput<?>> SampleSize<S> of()
    {
        return new SampleSize<>();
    }
    
    @Override
    public DoubleScoreOutput apply( S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        return DataFactory.ofDoubleScoreOutput( s.getRawData().size(),
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
     * Hidden constructor.
     */

    private SampleSize()
    {
        super();
    }

}
