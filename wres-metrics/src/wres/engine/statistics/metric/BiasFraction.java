package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.concurrent.atomic.DoubleAdder;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;

/**
 * Computes the mean error of a single-valued prediction as a fraction of the mean observed value.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class BiasFraction extends Metric<SingleValuedPairs, ScalarOutput> implements Score
{

    @Override
    public ScalarOutput apply(SingleValuedPairs s)
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
        DoubleAdder left = new DoubleAdder();
        DoubleAdder right = new DoubleAdder();
        s.getData().forEach(pair -> {
            left.add(pair.getItemOne()-pair.getItemTwo());
            right.add(pair.getItemOne());
        });
        return getDataFactory().ofScalarOutput(left.sum()/right.sum(), metOut);        
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BIAS_FRACTION;
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

    static class BiasFractionBuilder extends MetricBuilder<SingleValuedPairs, ScalarOutput>
    {

        @Override
        protected BiasFraction build()
        {
            return new BiasFraction(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private BiasFraction(final BiasFractionBuilder builder)
    {
        super(builder);
    }

}
