package wres.engine.statistics.metric.categorical;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Measures the predicted fraction of occurrences against the observed fraction of occurrences. A ratio of 1.0 
 * indicates an absence of any bias in the predicted and observed frequencies with which an event occurs.
 * 
 * @author james.brown@hydrosolved.com
 */
public class FrequencyBias extends ContingencyTableScore<DichotomousPairs>
{

    @Override
    public DoubleScoreOutput apply( final DichotomousPairs s )
    {
        return aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreOutput aggregate( final MatrixOutput output )
    {
        this.is2x2ContingencyTable( output, this );
        final MatrixOutput v = output;
        final double[][] cm = v.getData().getDoubles();
        final double score =
                FunctionFactory.finiteOrMissing().applyAsDouble( ( cm[0][0] + cm[0][1] ) / ( cm[0][0] + cm[1][0] ) );
        return getDataFactory().ofDoubleScoreOutput( score, getMetadata( output ) );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.FREQUENCY_BIAS;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class FrequencyBiasBuilder extends OrdinaryScoreBuilder<DichotomousPairs, DoubleScoreOutput>
    {
        @Override
        public FrequencyBias build() throws MetricParameterException
        {
            return new FrequencyBias( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder.
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private FrequencyBias( final FrequencyBiasBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
