package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;
import java.util.concurrent.atomic.DoubleAdder;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.DoubleErrorFunction;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Computes the mean error of a single-valued prediction as a fraction of the mean observed value.
 * 
 * @author james.brown@hydrosolved.com
 */
public class BiasFraction extends DoubleErrorScore<SingleValuedPairs>
{

    @Override
    public DoubleScoreOutput apply(SingleValuedPairs s)
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        final MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );
        DoubleAdder left = new DoubleAdder();
        DoubleAdder right = new DoubleAdder();
        DoubleErrorFunction error = FunctionFactory.error();
        s.getRawData().forEach( pair -> {
            left.add( error.applyAsDouble( pair ) );
            right.add( pair.getItemOne() );
        } );
        double result = left.sum() / right.sum();
        //Set NaN if not finite
        if ( !Double.isFinite( result ) )
        {
            result = Double.NaN;
        }
        return getDataFactory().ofDoubleScoreOutput( result, metOut );
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

    public static class BiasFractionBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        public BiasFraction build() throws MetricParameterException
        {
            return new BiasFraction(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private BiasFraction(final BiasFractionBuilder builder) throws MetricParameterException
    {
        super(builder);
    }

}
