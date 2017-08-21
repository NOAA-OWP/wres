package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.MetricInput;
import wres.datamodel.ScalarOutput;

/**
 * Constructs a {@link Metric} that returns the {@link MetricInput#size()}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SampleSize<S extends MetricInput<?>> extends Metric<S, ScalarOutput> implements Score
{

    @Override
    public ScalarOutput apply( S s )
    {
        return getDataFactory().ofScalarOutput( s.size(), getMetadata( s, s.size(), MetricConstants.MAIN, null ) );
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
    public MetricDecompositionGroup getDecompositionID()
    {
        return MetricDecompositionGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class SampleSizeBuilder<S extends MetricInput<?>> extends MetricBuilder<S, ScalarOutput>
    {
        @Override
        protected SampleSize<S> build()
        {
            return new SampleSize<>( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SampleSize( final SampleSizeBuilder<S> builder )
    {
        super( builder );
    }

}
